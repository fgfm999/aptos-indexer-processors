// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fmt::Debug};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use aptos_protos::transaction::v1::transaction::TxnData;
use async_trait::async_trait;
use diesel::{ExpressionMethods, pg::upsert::excluded};
use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use field_count::FieldCount;
use tracing::error;

use crate::{
    db::common::models::{
        default_models::{
            move_tables::CurrentTableItem,
            transactions::TransactionModel,
            write_set_changes::WriteSetChangeDetail,
        },
        events_models::events::EventModel,
        user_transactions_models::user_transactions::UserTransactionModel,
    },
    schema,
    utils::database::ArcDbPool,
};
use crate::utils::database::{execute_in_chunks, get_config_table_chunk_size};
use crate::worker::TableFlags;
use super::{DefaultProcessingResult, ProcessingResult, ProcessorName, ProcessorTrait};

pub struct CustomProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    deprecated_tables: TableFlags,
}

impl CustomProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        per_table_chunk_sizes: AHashMap<String, usize>,
        deprecated_tables: TableFlags,
    ) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
            deprecated_tables,
        }
    }
}

impl Debug for CustomProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "CustomTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}


fn insert_user_transactions_query(
    items_to_insert: Vec<UserTransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::user_transactions::dsl::*;
    (
        diesel::insert_into(schema::user_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                expiration_timestamp_secs.eq(excluded(expiration_timestamp_secs)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        None,
    )
}

fn insert_events_query(
    items_to_insert: Vec<EventModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::events::dsl::*;
    (
        diesel::insert_into(schema::events::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                indexed_type.eq(excluded(indexed_type)),
            )),
        None,
    )
}

fn insert_current_table_items_query(
    items_to_insert: Vec<CurrentTableItem>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_table_items::dsl::*;

    (
        diesel::insert_into(schema::current_table_items::table)
            .values(items_to_insert)
            .on_conflict((table_handle, key_hash))
            .do_update()
            .set((
                key.eq(excluded(key)),
                decoded_key.eq(excluded(decoded_key)),
                decoded_value.eq(excluded(decoded_value)),
                is_deleted.eq(excluded(is_deleted)),
                last_transaction_version.eq(excluded(last_transaction_version)),
                inserted_at.eq(excluded(inserted_at)),
            )),
        Some(" WHERE current_table_items.last_transaction_version <= excluded.last_transaction_version "),
    )
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    user_transactions: &[UserTransactionModel],
    events: &[EventModel],
    current_table_items: &[CurrentTableItem],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    execute_in_chunks(
        conn.clone(),
        insert_user_transactions_query,
        user_transactions,
        get_config_table_chunk_size::<UserTransactionModel>(
            "user_transactions",
            per_table_chunk_sizes,
        ),
    ).await?;
    execute_in_chunks(conn.clone(), insert_events_query, events, EventModel::field_count()).await?;
    execute_in_chunks(
        conn.clone(),
        insert_current_table_items_query,
        current_table_items,
        CurrentTableItem::field_count(),
    ).await?;
    Ok(())
}

#[async_trait]
impl ProcessorTrait for CustomProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::CustomProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        let processing_start = std::time::Instant::now();

        let mut user_transactions = vec![];
        let mut events = vec![];
        let mut current_table_items = HashMap::new();

        // current table item
        let (_block_metadata_txns, wsc_details) =
            TransactionModel::from_transactions(&transactions);
        for detail in wsc_details {
            if let WriteSetChangeDetail::Table(_item, current_item, _metadata) = detail {
                current_table_items.insert(
                    (
                        current_item.table_handle.clone(),
                        current_item.key_hash.clone(),
                    ),
                    current_item.clone(),
                );
            }
        }

        let skip_txes = vec![
            "0x7de3fea83cd5ca0e1def27c3f3803af619882db51f34abf30dd04ad12ee6af31::tapos::play",
            "0x7de3fea83cd5ca0e1def27c3f3803af619882db51f34abf30dd04ad12ee6af31::tapos_game_2::play"
        ];
        // user tx, events, current object,
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let txn_data = txn.txn_data.as_ref().expect("Txn Data doesn't exit!");

            // user transaction
            if let TxnData::User(inner) = txn_data {
                let (user_transaction, _sigs) = UserTransactionModel::from_transaction(
                    inner,
                    &txn.timestamp.as_ref().unwrap(),
                    block_height,
                    txn.epoch as i64,
                    txn_version,
                );
                if !skip_txes.contains(&&*user_transaction.entry_function_id_str) {
                    user_transactions.push(user_transaction);
                }
            }

            // user event
            if let TxnData::User(tx_inner) = txn_data {
                let txn_events = EventModel::from_events(&tx_inner.events, txn_version, block_height).into_iter()
                    .filter(|e| e.type_ != "0x1::transaction_fee::FeeStatement").collect::<Vec<_>>();
                events.extend(txn_events);
            }
        }
        // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
        let mut current_table_items = current_table_items
            .into_values()
            .collect::<Vec<CurrentTableItem>>();
        // Sort by PK
        current_table_items
            .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &user_transactions,
            &events,
            &current_table_items,
            &self.per_table_chunk_sizes,
        ).await;
        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        match tx_result {
            Ok(_) => Ok(ProcessingResult::DefaultProcessingResult(
                DefaultProcessingResult {
                    start_version,
                    end_version,
                    processing_duration_in_secs,
                    db_insertion_duration_in_secs,
                    last_transaction_timestamp: transactions.last().unwrap().timestamp.clone(),
                }
            )),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            }
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
