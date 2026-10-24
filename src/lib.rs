//! Durable local incident notifications.
pub mod checks;
pub mod config;
pub mod delivery;
pub mod model;
pub mod server;
pub mod store;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("notification storage unavailable")]
    Storage(#[source] anyhow::Error),
    #[error("attosend daemon unavailable")]
    Unavailable,
}

pub type Result<T> = std::result::Result<T, Error>;
