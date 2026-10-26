#[cfg(feature = "async")]
pub mod r#async;
pub mod lazy;

#[cfg(feature = "async")]
pub use r#async::AsyncSourceBuffer;
pub use lazy::SourceBuffer;
