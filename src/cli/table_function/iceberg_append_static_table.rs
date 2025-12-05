use crate::iceberg::utils::load_static_table;
use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, GenericByteBuilder, RecordBatch, TimestampMicrosecondArray,
};
use arrow::datatypes::{
    BinaryType, DataType, Field, Schema, SchemaBuilder, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType,
};
use arrow::error::ArrowError;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use ch_udf_common::arrow::ArrayRefExt;
use clap::Args;
use futures::{StreamExt, TryStreamExt, stream};
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

        let s = stream::iter(reader).map(|b| b.and_then(|b| transform(&b)));

        let metadata_files = write_parquet_partitioned(&table, s, None).await?;

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

fn transform(b: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let schema = b.schema();
    let columns = b.columns();
    let mut new_schema_builder = SchemaBuilder::with_capacity(schema.fields.len());
    let mut new_columns = Vec::with_capacity(columns.len());

    for i in 0..schema.fields.len() {
        let field = schema.field(i);
        let column = &columns[i];

        match schema.field(i).data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                if *tz != "UTC".into() {
                    new_schema_builder.push(field.to_owned())
                } else {
                    new_schema_builder.push(Field::new(
                        field.name(),
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        field.is_nullable(),
                    ));
                    new_columns.push(Arc::new(
                        column
                            .as_primitive::<TimestampMicrosecondType>()
                            .clone()
                            .with_timezone_opt::<String>(None),
                    ) as ArrayRef);
                }
            }
            _ => {
                new_schema_builder.push(field.to_owned());
                new_columns.push(column.clone());
            }
        }
    }

    RecordBatch::try_new(Arc::new(new_schema_builder.finish()), new_columns)
}
