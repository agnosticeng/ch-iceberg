use super::error::Error;
use iceberg_rust::table::Table;
use iceberg_rust_spec::spec::{
    manifest::{Content, ManifestEntry, Status},
    values::Value,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str;

#[derive(Serialize, Deserialize)]
pub struct FieldBoundValuesItem {
    field_id: i64,
    field_name: String,
    file_path: String,
    file_rows: i64,
    lower: Value,
    upper: Value,
}

impl FieldBoundValuesItem {
    pub fn new(field_id: i32, file_path: &str, manifest: &ManifestEntry) -> Result<Self, Error> {
        if *manifest.status() == Status::Deleted {
            return Err(Error::FileIsDeleted);
        }

        if *manifest.data_file().content() != Content::Data {
            return Err(Error::FileIsNotDataFile);
        }

        Ok(FieldBoundValuesItem {
            field_id: 1,
            field_name: "test".to_owned(),
            file_path: file_path.to_owned(),
            file_rows: *manifest.data_file().record_count(),
            lower: field_bound_value(manifest.data_file().lower_bounds(), field_id)?,
            upper: field_bound_value(manifest.data_file().upper_bounds(), field_id)?,
        })
    }
}

fn field_bound_value(m: &Option<HashMap<i32, Value>>, field_id: i32) -> Result<Value, Error> {
    let Some(m) = m else {
        return Err(Error::NoBoundValues);
    };

    let Some(value) = m.get(&field_id) else {
        return Err(Error::NoBoundValuesForField(field_id));
    };

    Ok(value.to_owned())
}

pub async fn field_bound_values(
    table: &Table,
    field_name: &str,
) -> Result<Vec<FieldBoundValuesItem>, Error> {
    let schema = table.current_schema(None)?;
    let Some(field_id) = schema.get_name(field_name).map(|f| f.id) else {
        return Err(Error::FieldDoesNotExists(field_name.to_owned()));
    };

    let manifests = table.manifests(None, None).await?;
    let datafiles = table.datafiles(&manifests, None, (None, None)).await?;

    datafiles
        .map_ok(|(file_path, manifest)| FieldBoundValuesItem::new(field_id, &file_path, &manifest))
        .flatten()
        .collect::<Result<Vec<FieldBoundValuesItem>, Error>>()
}
