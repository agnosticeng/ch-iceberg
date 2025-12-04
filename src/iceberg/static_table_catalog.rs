use async_trait::async_trait;
use core::str;
use iceberg_rust::{
    catalog::{
        Catalog,
        commit::{
            CommitTable, CommitView, TableRequirement, apply_table_updates,
            check_table_requirements,
        },
        create::{CreateMaterializedView, CreateTable, CreateView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
    },
    error::Error as IcebergError,
    materialized_view::MaterializedView,
    object_store::{Bucket, ObjectStoreBuilder, store::IcebergStore},
    spec::{
        identifier::FullIdentifier,
        table_metadata::{TableMetadata, new_metadata_location},
    },
    table::Table,
    view::View,
};
use object_store::ObjectStore;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub struct StaticTableCatalog {
    path: String,
    object_store: ObjectStoreBuilder,
    metadata: Arc<RwLock<Option<(String, TableMetadata)>>>,
}

impl StaticTableCatalog {
    pub async fn new(
        path: &str,
        object_store: ObjectStoreBuilder,
    ) -> Result<Self, iceberg_rust::error::Error> {
        Ok(StaticTableCatalog {
            path: path.to_owned(),
            object_store,
            metadata: Arc::new(RwLock::new(None)),
        })
    }

    fn is_public_namespace(&self, namespace: &Namespace) -> bool {
        namespace.len() == 1 && namespace[0] == "public"
    }

    fn identifier(&self) -> Identifier {
        Identifier::new(&["public".to_owned()], self.name())
    }

    fn get_object_store_and_path(
        &self,
    ) -> Result<(Arc<dyn ObjectStore>, String), iceberg_rust::error::Error> {
        let bucket = Bucket::from_path(&self.path)?;
        let (_, path) = self
            .path
            .split_once(&bucket.to_string())
            .unwrap_or(("", ""));
        let object_store = self.object_store.build(bucket)?;

        Ok((object_store, path.to_owned()))
    }

    fn replace_metadata(&self, path: &str, md: TableMetadata) {
        let mut w = self.metadata.write().unwrap();
        *w = Some((path.to_owned(), md));
    }
}

#[async_trait]
impl Catalog for StaticTableCatalog {
    fn name(&self) -> &str {
        self.path.trim_end_matches('/').split("/").last().unwrap()
    }

    async fn create_namespace(
        &self,
        _namespace: &Namespace,
        _properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        unimplemented!()
    }

    async fn drop_namespace(&self, _namespace: &Namespace) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn load_namespace(
        &self,
        _namespace: &Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        unimplemented!()
    }

    async fn update_namespace(
        &self,
        _namespace: &Namespace,
        _updates: Option<HashMap<String, String>>,
        _removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, IcebergError> {
        if self.is_public_namespace(namespace) {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, IcebergError> {
        if self.is_public_namespace(namespace) {
            Ok(vec![Identifier::new(&["public".to_owned()], self.name())])
        } else {
            Ok(vec![])
        }
    }

    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        if parent.is_none() {
            Ok(vec![Namespace::try_new(&["public".to_owned()])?])
        } else {
            Ok(vec![])
        }
    }

    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        Ok(self.is_public_namespace(identifier.namespace()) && identifier.name() == self.name())
    }

    async fn drop_table(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn drop_view(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn drop_materialized_view(&self, _identifier: &Identifier) -> Result<(), IcebergError> {
        unimplemented!()
    }

    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        if !self.tabular_exists(identifier).await? {
            return Err(IcebergError::CatalogNotFound);
        }

        let (object_store, path) = self.get_object_store_and_path()?;
        let version_hint_location = path.to_owned() + "/metadata/version-hint.text";

        let version_hint_content = object_store
            .get(&version_hint_location.into())
            .await?
            .bytes()
            .await?;

        let version = str::from_utf8(&version_hint_content)?;

        let metadata_location = path.to_owned() + "/metadata/" + version + ".metadata.json";

        let metadata_content = object_store
            .get(&metadata_location.clone().into())
            .await?
            .bytes()
            .await?;

        let metadata: TableMetadata = serde_json::from_slice(&metadata_content)?;
        self.replace_metadata(&metadata_location, metadata.clone());

        Ok(Tabular::Table(
            Table::new(
                identifier.clone(),
                self.clone(),
                object_store.clone(),
                metadata,
            )
            .await?,
        ))
    }

    async fn create_table(
        self: Arc<Self>,
        _identifier: Identifier,
        mut create_table: CreateTable,
    ) -> Result<Table, IcebergError> {
        create_table.location = Some(self.path.clone());
        let (object_store, path) = self.get_object_store_and_path()?;
        let version_hint_location = path.to_owned() + "/metadata/version-hint.txt";

        let e = object_store.head(&version_hint_location.into()).await;

        match e {
            Ok(_) => {
                return Err(IcebergError::InvalidFormat(
                    "Table already exists. Path".to_owned(),
                ));
            }
            Err(object_store::Error::NotFound { .. }) => (),
            Err(e) => return Err(IcebergError::ObjectStore(e)),
        };

        let metadata: TableMetadata = create_table.try_into()?;
        let metadata_location = new_metadata_location(&metadata);

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();
        self.replace_metadata(&metadata_location, metadata.clone());

        Ok(Table::new(
            self.identifier(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }

    async fn create_view(
        self: Arc<Self>,
        _identifier: Identifier,
        mut _create_view: CreateView<Option<()>>,
    ) -> Result<View, IcebergError> {
        unimplemented!()
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        _identifier: Identifier,
        _create_view: CreateMaterializedView,
    ) -> Result<MaterializedView, IcebergError> {
        unimplemented!()
    }

    async fn update_table(self: Arc<Self>, commit: CommitTable) -> Result<Table, IcebergError> {
        let (object_store, _path) = self.get_object_store_and_path()?;

        let Some(entry) = self.metadata.read().unwrap().clone() else {
            #[allow(clippy::if_same_then_else)]
            if !matches!(commit.requirements[0], TableRequirement::AssertCreate) {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ));
            } else {
                return Err(IcebergError::InvalidFormat(
                    "Create table assertion".to_owned(),
                ));
            }
        };

        let (_, mut metadata) = entry;

        if !check_table_requirements(&commit.requirements, &metadata) {
            return Err(IcebergError::InvalidFormat(
                "Table requirements not valid".to_owned(),
            ));
        }

        apply_table_updates(&mut metadata, commit.updates)?;

        let metadata_location = new_metadata_location(&metadata);

        object_store
            .put_metadata(&metadata_location, metadata.as_ref())
            .await?;

        object_store.put_version_hint(&metadata_location).await.ok();
        self.replace_metadata(&metadata_location, metadata.clone());

        Ok(Table::new(
            commit.identifier.clone(),
            self.clone(),
            object_store.clone(),
            metadata,
        )
        .await?)
    }

    async fn update_view(
        self: Arc<Self>,
        _commit: CommitView<Option<()>>,
    ) -> Result<View, IcebergError> {
        unimplemented!()
    }
    async fn update_materialized_view(
        self: Arc<Self>,
        _commit: CommitView<FullIdentifier>,
    ) -> Result<MaterializedView, IcebergError> {
        unimplemented!()
    }

    async fn register_table(
        self: Arc<Self>,
        _identifier: Identifier,
        _metadata_location: &str,
    ) -> Result<Table, IcebergError> {
        unimplemented!()
    }
}
