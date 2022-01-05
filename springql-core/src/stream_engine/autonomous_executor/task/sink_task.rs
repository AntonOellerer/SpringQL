// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod sink_writer;

use super::task_context::TaskContext;
use crate::error::Result;
use crate::pipeline::name::SinkWriterName;
use crate::pipeline::sink_writer_model::SinkWriterModel;
use crate::stream_engine::autonomous_executor::row::foreign_row::sink_row::SinkRow;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task_graph::task_id::TaskId;

#[derive(Debug)]
pub(crate) struct SinkTask {
    id: TaskId,
    sink_writer_name: SinkWriterName,
}

impl SinkTask {
    pub(in crate::stream_engine) fn new(sink_writer: &SinkWriterModel) -> Self {
        let id = TaskId::from_sink(sink_writer);
        Self {
            id,
            sink_writer_name: sink_writer.name().clone(),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<()> {
        let row_repo = context.row_repository();

        let row = row_repo.collect_next(&context.task())?;
        let row = row.fixme_clone(); // Ahhhhhhhhhhhhhh

        self.emit(row, context)
    }

    fn emit(&self, row: Row, context: &TaskContext) -> Result<()> {
        let f_row = SinkRow::from(row);

        let sink_writer = context
            .sink_writer_repository()
            .get_sink_writer(&self.sink_writer_name);

        sink_writer
            .lock()
            .expect("other worker threads sharing the same sink subtask must not get panic")
            .send_row(f_row)?;

        Ok(())
    }
}
