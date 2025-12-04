use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ParseError(#[from] url::ParseError),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("file is deleted")]
    FileIsDeleted,
    #[error("file is not a datafile")]
    FileIsNotDataFile,
    #[error("no bound values")]
    NoBoundValues,
    #[error("no bound values for field `{0}`")]
    NoBoundValuesForField(i32),
    #[error(transparent)]
    IcebergError(#[from] iceberg_rust::error::Error),
    #[error("no field named `{0}`")]
    FieldDoesNotExists(String),
}

impl From<Error> for iceberg_rust::error::Error {
    fn from(value: Error) -> Self {
        iceberg_rust::error::Error::External(Box::new(value))
    }
}
