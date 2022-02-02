// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::expression::function_call::FunctionCall;
use crate::expression::Expression;
use crate::pipeline::field::field_name::ColumnReference;
use crate::pipeline::field::Field;
use crate::pipeline::name::{ColumnName, StreamName};
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct EvalValueExprSubtask {
    expressions: Vec<Expression>, // TODO include both ValueExpr and AggrExpr (enum?)
}

impl EvalValueExprSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(&self, tuple: Tuple) -> Result<Tuple> {
        let rowtime = *tuple.rowtime();

        let new_fields = self
            .expressions
            .iter()
            .map(|expr| {
                let colref = match expr {
                    Expression::FieldPointer(ptr) => ColumnReference::new(
                        StreamName::new("_".to_string()), // super ugly...
                        ColumnName::new(ptr.attr().to_string()),
                    ),
                    Expression::FunctionCall(fun_call) => match fun_call {
                        FunctionCall::FloorTime { target, .. } => {
                            // TODO will use label for projection
                            match target.as_ref() {
                                Expression::FieldPointer(ptr) => ColumnReference::new(
                                    StreamName::new("_".to_string()), // super ugly...
                                    ColumnName::new(ptr.attr().to_string()),
                                ),
                                _ => unimplemented!(),
                            }
                        }
                        FunctionCall::DurationSecs { .. } => {
                            unreachable!("DURATION_SECS() cannot appear in field list")
                        }
                    },
                    _ => ColumnReference::new(
                        StreamName::new("_".to_string()),
                        ColumnName::new("_".to_string()),
                    ),
                };
                let value = tuple.eval_expression(expr.clone())?;
                Ok(Field::new(colref, value))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Tuple::new(rowtime, new_fields))
    }
}