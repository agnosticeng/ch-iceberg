# ch-iceberg

A set of [ClickHouse](https://clickhouse.com/) [user-defined functions](https://clickhouse.com/docs/sql-reference/functions/udf) to work with [Apache Iceberg](https://iceberg.apache.org/) tables.

## 📦 Artifact: The Bundle

The output of the build process is distributed as a **compressed archive** called a **bundle**. This bundle includes everything needed to deploy and use the UDFs in ClickHouse.

### 📁 Bundle Contents

Each bundle contains:

- 🧩 **Standalone binary** implementing the native UDFs (compiled with ClickHouse compatibility)
- ⚙️ **ClickHouse configuration files** (`.xml`) to register each native UDF
- 📝 **SQL files** for SQL-based UDFs (used for lightweight functions where SQL outperforms compiled code)

### 📦 Bundle Usage

#### 🛠️ Build the Bundle

```sh
make bundle              # Build for native execution
```

This will:

- Generate the bundle directory at `tmp/bundle/`
- Create a compressed archive at `tmp/bundle.tar.gz`

The internal file structure of the bundle reflects the default layout of a basic ClickHouse installation.  
As a result, **decompressing the archive at the root of a ClickHouse server filesystem should "just work"** with no additional path configuration.

---

#### ▶️ Run with `clickhouse-local`

```sh
clickhouse local \
    --log-level=debug \
    --path tmp/clickhouse \
    -- \
    --user_scripts_path="./tmp/bundle/var/lib/clickhouse/user_scripts" \
    --user_defined_executable_functions_config="./tmp/bundle/etc/clickhouse-server/*_function.*ml" \
    --user_defined_path="./tmp/bundle/var/lib/clickhouse/user_defined" \
    --send_logs_level=trace \
    --output_format_arrow_string_as_string=0
```

This runs ClickHouse in local mode using the provided config and a temporary storage path.
