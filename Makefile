BUNDLE_DIR ?= "tmp/bundle"
BUNDLE_ARCHIVE ?= "tmp/bundle.tar.gz"
RELEASE ?= "false"
BINARY_PATH ?= "./target/debug/ch-iceberg"

ifeq ($(RELEASE), true)
	CARGO_BUILD_OPTIONS += "--release"
	BINARY_PATH = "./target/release/ch-iceberg"
endif

build: 
	cargo clippy
	cargo build ${CARGO_BUILD_OPTIONS}

bundle: 
	mkdir -p $(BUNDLE_DIR)
	mkdir -p $(BUNDLE_DIR)/etc/clickhouse-server
	mkdir -p $(BUNDLE_DIR)/var/lib/clickhouse/user_defined
	mkdir -p $(BUNDLE_DIR)/var/lib/clickhouse/user_scripts
	mkdir -p $(BUNDLE_DIR)/var/lib/clickhouse/metadata
	cp $(BINARY_PATH) $(BUNDLE_DIR)/var/lib/clickhouse/user_scripts/
	cp config/*_function.*ml $(BUNDLE_DIR)/etc/clickhouse-server/
	COPYFILE_DISABLE=1 tar --no-xattr -cvzf $(BUNDLE_ARCHIVE) -C $(BUNDLE_DIR) .

clean:
	rm -rf bin
	rm -rf $(BUNDLE_DIR)
	rm -rf $(BUNDLE_ARCHIVE)
