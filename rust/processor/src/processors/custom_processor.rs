// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{DefaultProcessingResult, ProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::{
        common::models::default_models::{
            raw_current_table_items::{CurrentTableItemConvertible, RawCurrentTableItem},
            raw_table_items::RawTableItem,
        },
        postgres::models::{
            default_models::move_tables::CurrentTableItem, events_models::events::EventModel,
        },
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::{
    transaction::TxnData, write_set_change::Change as WriteSetChangeEnum, Transaction,
};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::fmt::Debug;
use tracing::error;

pub struct CustomProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

const SKIP_EVENTS: &'static [&str] = &[
    "0x1::block::NewBlockEvent",
    "0x1::fungible_asset::Deposit",
    "0x1::fungible_asset::Withdraw",
    "0x1::coin::DepositEvent",
    "0x1::coin::WithdrawEvent",
    "0x1::coin::Deposit",
    "0x1::coin::Withdraw",
    "0x1::transaction_fee::FeeStatement",
];

impl CustomProcessor {
    pub fn new(connection_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>) -> Self {
        Self {
            connection_pool,
            per_table_chunk_sizes,
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
        insert_events_query,
        events,
        get_config_table_chunk_size::<EventModel>("events", per_table_chunk_sizes),
    )
    .await?;
    execute_in_chunks(
        conn.clone(),
        insert_current_table_items_query,
        current_table_items,
        get_config_table_chunk_size::<CurrentTableItem>(
            "current_table_items",
            per_table_chunk_sizes,
        ),
    )
    .await?;
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
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let (raw_current_table_items, events) =
            tokio::task::spawn_blocking(move || process_transactions(transactions))
                .await
                .expect("Failed to spawn_blocking for TransactionModel::from_transactions");
        let postgres_current_table_items: Vec<CurrentTableItem> = raw_current_table_items
            .iter()
            .map(CurrentTableItem::from_raw)
            .collect();

        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &events,
            &postgres_current_table_items,
            &self.per_table_chunk_sizes,
        )
        .await;
        // These vectors could be super large and take a lot of time to drop, move to background to
        // make it faster.
        tokio::task::spawn(async move {
            drop(postgres_current_table_items);
        });
        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();

        match tx_result {
            Ok(_) => Ok(ProcessingResult::DefaultProcessingResult(
                DefaultProcessingResult {
                    start_version,
                    end_version,
                    processing_duration_in_secs,
                    db_insertion_duration_in_secs,
                    last_transaction_timestamp,
                },
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
            },
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}

pub fn process_transactions(
    transactions: Vec<Transaction>,
) -> (Vec<RawCurrentTableItem>, Vec<EventModel>) {
    let mut current_table_items = AHashMap::new();
    let mut events = vec![];

    for transaction in transactions {
        let version = transaction.version as i64;
        let block_height = transaction.block_height as i64;
        let timestamp = transaction
            .timestamp
            .as_ref()
            .expect("Transaction timestamp doesn't exist!");
        let transaction_info = transaction
            .info
            .as_ref()
            .expect("Transaction info doesn't exist!");
        #[allow(deprecated)]
        let block_timestamp =
            chrono::NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
                .expect("Txn Timestamp is invalid!");

        let txn_data = match transaction.txn_data.as_ref() {
            Some(txn_data) => txn_data,
            None => {
                tracing::warn!(
                    transaction_version = transaction.version,
                    "Transaction data doesn't exist",
                );
                continue;
            },
        };

        // current_table
        for (index, wsc) in transaction_info.changes.iter().enumerate() {
            match wsc
                .change
                .as_ref()
                .expect("WriteSetChange must have a change")
            {
                WriteSetChangeEnum::WriteTableItem(inner) => {
                    let (_ti, cti) = RawTableItem::from_write_table_item(
                        inner,
                        index as i64,
                        version,
                        block_height,
                        block_timestamp,
                    );
                    current_table_items.insert(
                        (cti.table_handle.clone(), cti.key_hash.clone()),
                        cti.clone(),
                    );
                },
                WriteSetChangeEnum::DeleteTableItem(inner) => {
                    let (_ti, cti) = RawTableItem::from_delete_table_item(
                        inner,
                        index as i64,
                        version,
                        block_height,
                        block_timestamp,
                    );
                    current_table_items
                        .insert((cti.table_handle.clone(), cti.key_hash.clone()), cti);
                },
                _ => {},
            };
        }

        // user event
        if let TxnData::User(tx_inner) = txn_data {
            let txn_events = EventModel::from_events(&tx_inner.events, version, block_height)
                .into_iter()
                .filter(|e| !SKIP_EVENTS.contains(&e.indexed_type.as_str()))
                .collect::<Vec<_>>();
            events.extend(txn_events);
        }
    }
    // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
    let mut current_table_items = current_table_items
        .into_values()
        .collect::<Vec<RawCurrentTableItem>>();
    // Sort by PK
    current_table_items
        .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));
    (current_table_items, events)
}
