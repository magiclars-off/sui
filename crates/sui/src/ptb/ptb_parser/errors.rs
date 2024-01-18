// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

use super::context::FileScope;

pub type PTBResult<T> = Result<T, PTBError>;

#[derive(Debug, Clone, Error)]
pub enum PTBError {
    #[error("{message} at command {} in file '{}'", file_scope.file_command_index, file_scope.name)]
    WithSource {
        file_scope: FileScope,
        message: String,
    },
}

#[macro_export]
macro_rules! error {
    ($x:expr, $($arg:tt)*) => {
        return Err($crate::ptb::ptb_parser::errors::PTBError::WithSource {
            file_scope: $x.context.current_file_scope().clone(),
            message: format!($($arg)*),
        })
    };
}

#[macro_export]
macro_rules! err {
    ($x:expr, $($arg:tt)*) => {
        $crate::ptb::ptb_parser::errors::PTBError::WithSource {
            file_scope: $x.context.current_file_scope().clone(),
            message: format!($($arg)*),
        }
    };
}
