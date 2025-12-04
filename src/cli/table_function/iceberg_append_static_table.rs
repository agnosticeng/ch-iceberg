use crate::iceberg::utils::load_static_table;
use anyhow::{Context, Result};
use arrow::array::{GenericByteBuilder, RecordBatch};
use arrow::datatypes::{BinaryType, DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use clap::Args;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use std::io::{stdin, stdout};
use std::sync::Arc;

#[derive(Debug, Clone, Args)]
pub struct IcebergAppendStaticTableCommand {
    #[arg(short, long)]
    table_location: String,
}

impl IcebergAppendStaticTableCommand {
    pub async fn run(&self) -> Result<()> {
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Binary,
            false,
        )]));

        let reader = StreamReader::try_new_buffered(stdin(), None)?;
        let mut writer = StreamWriter::try_new_buffered(stdout(), &output_schema)?;

        let mut table = load_static_table(&self.table_location).await?;

        let metadata_files = write_parquet_partitioned(&table, stream::iter(reader), None).await?;

        table
            .new_transaction(None)
            .append_data(metadata_files.clone())
            .commit()
            .await?;

        let mut result_col_builder = GenericByteBuilder::<BinaryType>::new();

        for metadata_file in metadata_files {
            result_col_builder.append_value(metadata_file.file_path());
        }

        let result_col = result_col_builder.finish();
        let output_batch = RecordBatch::try_new(output_schema.clone(), vec![Arc::new(result_col)])?;
        writer
            .write(&output_batch)
            .context("failed to write output batch")?;
        writer.flush().context("failed to flush output stream")?;

        Ok(())
    }
}
