use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::json_result::JSONResult;
use clap::Args;
use iceberg_extra::catalog::load_catalog;
use iceberg_extra::catalog::parse_identifier;
use iceberg_extra::object_store::opts_from_env;
use iceberg_extra::object_store::opts_from_query_string;
use iceberg_rust::catalog::create::CreateTable;
use itertools::izip;
use std::io::{stdin, stdout};
use std::str;
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct IcebergCreateTableCommand {}

impl IcebergCreateTableCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Binary,
            false,
        )]));

        loop {
            let reader = StreamReader::try_new_buffered(stdin(), None)?;
            let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

            for input_batch in reader {
                let input_batch = input_batch.context("failed to read input batch")?;

                let mut result_col_builder = GenericByteBuilder::<BinaryType>::with_capacity(
                    input_batch.num_rows(),
                    input_batch.num_rows() * 1024,
                );

                let catalog_col: &BinaryArray = input_batch.get_column("catalog")?;
                let table_col: &BinaryArray = input_batch.get_column("table")?;
                let payload_col: &BinaryArray = input_batch.get_column("payload")?;

                for (catalog, table, payload) in izip!(catalog_col, table_col, payload_col) {
                    let opts = itertools::concat([
                        opts_from_env(),
                        opts_from_query_string(str::from_utf8(catalog.unwrap())?),
                    ]);

                    let cat = load_catalog(opts).await?;
                    let mut create_table: CreateTable = serde_json::from_slice(payload.unwrap())?;
                    let id = parse_identifier(str::from_utf8(table.unwrap())?)?;
                    create_table.name = id.name().to_owned();

                    let res = cat
                        .clone()
                        .create_table(id, create_table)
                        .await
                        .map(|_| serde_json::Value::Object(serde_json::Map::new()))
                        .context("failed to create table");

                    result_col_builder
                        .append_value(serde_json::to_string(&JSONResult::from(res))?.as_bytes());
                }

                let result_col = result_col_builder.finish();
                let output_batch =
                    RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;
                writer
                    .write(&output_batch)
                    .context("failed to write output batch")?;
                writer.flush().context("failed to flush output stream")?;
            }
        }
    }
}
