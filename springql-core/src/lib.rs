//! libSpringQL implementation.

#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub(crate) mod pipeline;
pub(crate) mod sql_processor;
pub(crate) mod stream_engine;

mod api;

pub use api::*;