use crate::iceberg::utils::create_static_table;
use anyhow::{Context, Result};
use arrow::array::{BinaryArray, GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::RecordBatchExt;
use ch_udf_common::json_result::JSONResult;
use clap::Args;
use itertools::izip;
use std::io::{stdin, stdout};
use std::str;
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct IcebergCreateStaticTableCommand {}

impl IcebergCreateStaticTableCommand {
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

                let table_location_col: &BinaryArray = input_batch.get_column("table_location")?;
                let payload_col: &BinaryArray = input_batch.get_column("payload")?;

                for (table_location, payload) in izip!(table_location_col, payload_col) {
                    let res = create_static_table(
                        str::from_utf8(table_location.unwrap())?,
                        payload.unwrap(),
                    )
                    .await;

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
