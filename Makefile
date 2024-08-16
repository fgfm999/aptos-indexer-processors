force_pull:
	git fetch --all && git reset --hard origin/custom-20240706

build_processor:
	cd rust && cargo build -p processor --release

install_processor:
	cp rust/target/release/processor /data1/mainnet/bin/
