use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct AppError(Box<ErrorKind>);

#[derive(Error, Debug)]
#[error(transparent)]
pub enum ErrorKind {
    #[error("SerdeJson: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Io: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

impl<E> From<E> for AppError
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        AppError(Box::new(ErrorKind::from(err)))
    }
}
pub type Result<T> = std::result::Result<T, AppError>;
