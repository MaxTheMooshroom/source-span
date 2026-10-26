#[cfg(feature = "async")]
mod r#async;
mod lazy;

#[cfg(feature = "async")]
pub use r#async::AsyncSourceBuffer;
pub use lazy::SourceBuffer;
