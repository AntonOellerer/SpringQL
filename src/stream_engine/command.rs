use self::alter_pipeline_command::AlterPipelineCommand;

pub(crate) mod alter_pipeline_command;
pub(crate) mod query_plan;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Command {
    AlterPipeline(AlterPipelineCommand),
}
