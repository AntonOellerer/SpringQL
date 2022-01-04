// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod sink_writer_type;

use serde::{Deserialize, Serialize};

use self::sink_writer_type::SinkWriterType;

use super::{
    name::{SinkWriterName, StreamName},
    option::Options,
};

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, new)]
pub(crate) struct SinkWriterModel {
    name: SinkWriterName,
    sink_writer_type: SinkWriterType,
    from_sink_stream: StreamName,
    options: Options,
}

impl SinkWriterModel {
    pub(crate) fn name(&self) -> &SinkWriterName {
        &self.name
    }

    pub(crate) fn sink_writer_type(&self) -> &SinkWriterType {
        &self.sink_writer_type
    }

    pub(crate) fn from_sink_stream(&self) -> &StreamName {
        &self.from_sink_stream
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }
}
