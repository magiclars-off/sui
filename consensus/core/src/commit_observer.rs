// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;

use crate::commit::Commit;

#[derive(Default)]
pub struct CommitObserverRecoveredState {
    /// All previously committed subdags.
    pub sub_dags: Vec<Commit>,
    /// Last observed state of the commit observer returned by CommitObserver::aggregator_state
    pub state: Option<Bytes>,
}
