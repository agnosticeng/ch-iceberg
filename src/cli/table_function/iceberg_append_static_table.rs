use crate::iceberg::utils::load_static_table;
use anyhow::{Context, Result};
use arrow::array::{Array, GenericByteBuilder, RecordBatch};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{BinaryType, DataType, Field, Schema, SchemaBuilder};
use arrow::error::ArrowError;
use arrow::util::display::FormatOptions;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use clap::Args;
use futures::{StreamExt, stream};
use iceberg_rust::arrow::write::write_parquet_partitioned;
use itertools::izip;
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
        let table_schema = table.current_schema(None)?;
        let table_arrow_schema: Arc<ArrowSchema> = Arc::new((table_schema.fields()).try_into()?);

        let s = stream::iter(reader)
            .map(move |b| b.and_then(|b| cast_record_batch(table_arrow_schema.clone(), &b)));

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

fn cast_record_batch(
    table_schema: Arc<ArrowSchema>,
    b: &RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    let schema = b.schema();
    let columns = b.columns();
    let mut new_schema_builder = SchemaBuilder::with_capacity(schema.fields.len());
    let mut new_columns = Vec::with_capacity(columns.len());

    for (table_field, field, column) in izip!(table_schema.fields(), schema.fields(), columns) {
        let (new_field, new_column) =
            cast_column(table_field.clone(), field.clone(), column.clone())?;
        new_schema_builder.push(new_field);
        new_columns.push(new_column);
    }

    RecordBatch::try_new(Arc::new(new_schema_builder.finish()), new_columns)
}

fn cast_column(
    table_field: Arc<Field>,
    field: Arc<Field>,
    column: Arc<dyn Array>,
) -> Result<(Arc<Field>, Arc<dyn Array>), ArrowError> {
    if table_field.data_type() == field.data_type() {
        return Ok((field, column));
    }

    let new_column = cast_with_options(
        &column,
        table_field.data_type(),
        &CastOptions {
            safe: true,
            format_options: FormatOptions::new(),
        },
    )
    .map_err(|e| {
        if let ArrowError::CastError(s) = e {
            ArrowError::CastError(format!(
                "Field {} / {}: {}",
                table_field.name(),
                field.name(),
                s
            ))
        } else {
            e
        }
    })?;

    Ok((table_field, new_column))
}
