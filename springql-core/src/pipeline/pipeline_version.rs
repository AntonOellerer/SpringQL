// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub(crate) struct PipelineVersion(u64);

impl PipelineVersion {
    pub(crate) fn new() -> Self {
        Self(1)
    }

    pub(crate) fn up(&mut self) {
        self.0 += 1;
    }
}
