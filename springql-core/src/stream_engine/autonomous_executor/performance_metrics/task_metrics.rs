use serde::{Deserialize, Serialize};

use crate::stream_engine::autonomous_executor::performance_metrics::calculation::next_avg;

use super::metrics_update_command::metrics_update_command_by_task_execution::MetricsUpdateCommandByTaskExecution;

/// Flow monitor of a task (including in-memory queue sink) execution.
#[derive(Clone, PartialEq, Debug, Default, Serialize, Deserialize)]
pub(in crate::stream_engine::autonomous_executor) struct TaskMetrics {
    avg_gain_bytes_per_sec: f32,
    n_executions: u64,
}

impl TaskMetrics {
    pub(in crate::stream_engine::autonomous_executor) fn update_by_task_execution(
        &mut self,
        command: &MetricsUpdateCommandByTaskExecution,
    ) {
        let n = self.n_executions;

        self.n_executions += 1;
        self.avg_gain_bytes_per_sec = next_avg(
            self.avg_gain_bytes_per_sec,
            n,
            command.task_gain_bytes_per_sec(),
        );
    }

    pub(in crate::stream_engine::autonomous_executor) fn avg_gain_bytes_per_sec(&self) -> f32 {
        self.avg_gain_bytes_per_sec
    }
}