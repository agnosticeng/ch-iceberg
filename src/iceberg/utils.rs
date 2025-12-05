use crate::iceberg::{object_store::parse_url_opts, static_table_catalog::StaticTableCatalog};
use ch_udf_common::object_store::{opts_from_env, opts_from_url};
use iceberg_rust::catalog::Catalog;
use iceberg_rust::catalog::create::CreateTable;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_rust::table::Table;
use std::str;
use std::sync::Arc;
use url::Url;

pub async fn load_static_table(table_location: &str) -> Result<Table, iceberg_rust::error::Error> {
    let mut u = Url::parse(table_location)?;
    let (object_store, _) =
        parse_url_opts(&u, itertools::concat([opts_from_env(), opts_from_url(&u)]))?;
    u.set_fragment(None);
    let catalog = Arc::new(StaticTableCatalog::new(u.to_string().as_str(), object_store).await?);
    let tabular = catalog
        .clone()
        .load_tabular(&Identifier::new(&["public".to_owned()], catalog.name()))
        .await?;

    match tabular {
        Tabular::Table(table) => Ok(table),
        _ => Err(iceberg_rust::error::Error::NotSupported(
            "Tabular must be a table".to_owned(),
        )),
    }
}

pub async fn create_static_table(
    table_location: &str,
    payload: &[u8],
) -> Result<(), iceberg_rust::error::Error> {
    let mut u = Url::parse(table_location)?;
    let (object_store, _) =
        parse_url_opts(&u, itertools::concat([opts_from_env(), opts_from_url(&u)]))?;
    u.set_fragment(None);
    let catalog = Arc::new(StaticTableCatalog::new(u.to_string().as_str(), object_store).await?);
    let create_table: CreateTable = serde_json::from_slice(payload)?;
    catalog
        .clone()
        .create_table(
            Identifier::new(&["public".to_owned()], catalog.name()),
            create_table,
        )
        .await?;
    Ok(())
}
