// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use crate::{
    block::{BlockAPI, BlockRef, VerifiedBlock},
    commit::{Commit, CommitIndex},
    commit_observer::CommitObserverRecoveredState,
    storage::Store,
};

/// The output of consensus is an ordered list of [`CommittedSubDag`]. The application
/// can arbitrarily sort the blocks within each sub-dag (but using a deterministic algorithm).
#[derive(Clone)]
pub struct CommittedSubDag {
    /// A reference to the anchor of the sub-dag
    pub anchor: BlockRef,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<VerifiedBlock>,
    /// The timestamp of the commit, obtained from the timestamp of the anchor block.
    pub timestamp_ms: u64,
    /// Index of the commit.
    /// First commit after genesis has a index of 1, then every next commit has a
    /// index incremented by 1.
    pub index: CommitIndex,
}

#[allow(unused)]
impl CommittedSubDag {
    /// Create new (empty) sub-dag.
    pub fn new(
        anchor: BlockRef,
        blocks: Vec<VerifiedBlock>,
        timestamp_ms: u64,
        index: CommitIndex,
    ) -> Self {
        Self {
            anchor,
            blocks,
            timestamp_ms,
            index,
        }
    }

    pub fn new_from_commit_data(commit_data: Commit, block_store: Arc<dyn Store>) -> Self {
        let mut leader_block_idx = None;
        let blocks = commit_data
            .blocks
            .into_iter()
            .enumerate()
            .map(|(idx, block_ref)| {
                // todo: add read_block?
                let block: VerifiedBlock = block_store
                    .read_blocks(&[block_ref])
                    .expect("We should have the block referenced in the commit data")
                    .first()
                    .expect("We should have the block referenced in the commit data")
                    .clone()
                    .expect("We should have the block referenced in the commit data");
                if block_ref == commit_data.leader {
                    leader_block_idx = Some(idx);
                }
                block
            })
            .collect::<Vec<_>>();
        let leader_block_idx = leader_block_idx.expect("Leader block must be in the sub-dag");
        let leader_block_ref = blocks[leader_block_idx].reference();
        let timestamp_ms = blocks[leader_block_idx].timestamp_ms();
        CommittedSubDag::new(leader_block_ref, blocks, timestamp_ms, commit_data.index)
    }

    /// Sort the blocks of the sub-dag by round number. Any deterministic algorithm works.
    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }
}

impl Display for CommittedSubDag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CommittedSubDag(anchor={}, index={}, blocks=[",
            self.anchor.digest, self.index
        )?;
        for (idx, block) in self.blocks.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", block.digest())?;
        }
        write!(f, "])")
    }
}

/// Expand a committed sequence of leader into a sequence of sub-dags.
#[allow(unused)]
pub struct Linearizer {
    // Persistent storage for blocks, commits and other consensus data.
    block_store: Arc<dyn Store>,
    /// Keep track of all committed blocks to avoid committing the same block twice.
    committed: HashSet<BlockRef>,
    /// Keep track of the index of last linearized commit
    last_index: u64,
}

#[allow(unused)]
impl Linearizer {
    pub fn new(block_store: Arc<dyn Store>) -> Self {
        Self {
            block_store,
            committed: Default::default(),
            last_index: Default::default(),
        }
    }

    pub fn recover_state(&mut self, recovered_state: &CommitObserverRecoveredState) {
        assert!(self.committed.is_empty());
        assert_eq!(self.last_index, 0);
        for commit in recovered_state.sub_dags.iter() {
            assert!(commit.index > self.last_index);
            self.last_index = commit.index;

            for block in commit.blocks.iter() {
                self.committed.insert(*block);
            }
            // Leader must be part of the subdag and hence should have been inserted in the loop above.
            assert!(self.committed.contains(&commit.leader));
        }
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn collect_sub_dag(&mut self, leader_block: VerifiedBlock) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        let timestamp_ms = leader_block.timestamp_ms();
        let leader_block_ref = leader_block.reference();
        let mut buffer = vec![leader_block];
        assert!(self.committed.insert(leader_block_ref));
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.ancestors() {
                // The block manager may have cleaned up blocks passed the latest committed rounds.
                // todo: add read_block?
                let block: VerifiedBlock = self
                    .block_store
                    .read_blocks(&[*reference])
                    .expect("We should have the whole sub-dag by now")
                    .first()
                    .expect("We should have the whole sub-dag by now")
                    .clone()
                    .expect("We should have the whole sub-dag by now");

                // Skip the block if we already committed it (either as part of this sub-dag or
                // a previous one).
                if self.committed.insert(*reference) {
                    buffer.push(block);
                }
            }
        }
        self.last_index += 1;
        CommittedSubDag::new(leader_block_ref, to_commit, timestamp_ms, self.last_index)
    }

    pub fn handle_commit(&mut self, committed_leaders: Vec<VerifiedBlock>) -> Vec<CommittedSubDag> {
        let mut committed = vec![];
        for leader_block in committed_leaders {
            // Collect the sub-dag generated using each of these leaders as anchor.
            let mut sub_dag = self.collect_sub_dag(leader_block);

            // [Optional] sort the sub-dag using a deterministic algorithm.
            sub_dag.sort();
            committed.push(sub_dag);
        }
        committed
    }
}

impl fmt::Debug for CommittedSubDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}(", self.anchor, self.index)?;
        for block in &self.blocks {
            write!(f, "{}, ", block.reference())?;
        }
        write!(f, ")")
    }
}
