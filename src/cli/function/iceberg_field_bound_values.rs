use crate::iceberg::field_bound_values::field_bound_values;
use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::json_result::JSONResult;
use clap::Args;
use iceberg_extra::catalog::load_catalog_and_table;
use iceberg_extra::object_store::opts_from_env;
use iceberg_extra::object_store::opts_from_query_string;
use itertools::izip;
use std::io::{stdin, stdout};
use std::str;
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct IcebergFieldBoundValuesCommand {}

impl IcebergFieldBoundValuesCommand {
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
                let field_name_col: &BinaryArray = input_batch.get_column("field_name")?;

                for (catalog, table, field_name) in izip!(catalog_col, table_col, field_name_col) {
                    let opts = itertools::concat([
                        opts_from_env(),
                        opts_from_query_string(str::from_utf8(catalog.unwrap())?),
                    ]);

                    let (_, table) =
                        load_catalog_and_table(opts, str::from_utf8(table.unwrap())?).await?;

                    let bound_values =
                        field_bound_values(&table, str::from_utf8(field_name.unwrap())?).await;

                    result_col_builder.append_value(
                        serde_json::to_string(&JSONResult::from(bound_values))?.as_bytes(),
                    );
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
