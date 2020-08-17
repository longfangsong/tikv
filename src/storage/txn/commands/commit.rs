// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::metrics::MVCC_CONFLICT_COUNTER;
use crate::storage::mvcc::metrics::MVCC_DUPLICATE_CMD_COUNTER_VEC;
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::{
    Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, Result as MvccResult,
};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{Error, ErrorInner, Result};
use crate::storage::{ProcessResult, Snapshot, TxnStatus};
use txn_types::{Key, LockType, Write, WriteType};

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {} {} -> {} | {:?}", (keys.len, lock_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: txn_types::TimeStamp,
            /// The commit timestamp.
            commit_ts: txn_types::TimeStamp,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Commit {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        if self.commit_ts <= self.lock_ts {
            return Err(Error::from(ErrorInner::InvalidTxnTso {
                start_ts: self.lock_ts,
                commit_ts: self.commit_ts,
            }));
        }
        let mut txn = MvccTxn::new(
            snapshot,
            self.lock_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let rows = self.keys.len();
        // Pessimistic txn needs key_hashes to wake up waiters
        let mut released_locks = ReleasedLocks::new(self.lock_ts, self.commit_ts);
        for key in &self.keys {
            fail_point!("commit", |err| Err(MvccError::from(make_txn_error(
                err,
                key,
                self.lock_ts
            ))
            .into()));
            let result: MvccResult<_> = match txn.reader.load_lock(&key)? {
                Some(mut lock) if lock.ts == self.lock_ts => {
                    // A lock with larger min_commit_ts than current commit_ts can't be committed
                    if self.commit_ts < lock.min_commit_ts {
                        info!(
                            "trying to commit with smaller commit_ts than min_commit_ts";
                            "key" => %key,
                            "start_ts" => self.lock_ts,
                            "commit_ts" => self.commit_ts,
                            "min_commit_ts" => lock.min_commit_ts,
                        );
                        return Err(MvccError::from(MvccErrorInner::CommitTsExpired {
                            start_ts: self.lock_ts,
                            commit_ts: self.commit_ts,
                            key: key.to_raw()?,
                            min_commit_ts: lock.min_commit_ts,
                        })
                        .into());
                    }
                    // It's an abnormal routine since pessimistic locks shouldn't be committed in our
                    // transaction model. But a pessimistic lock will be left if the pessimistic
                    // rollback request fails to send and the transaction need not to acquire
                    // this lock again(due to WriteConflict). If the transaction is committed, we
                    // should commit this pessimistic lock too.
                    if lock.lock_type == LockType::Pessimistic {
                        warn!(
                            "commit a pessimistic lock with Lock type";
                            "key" => %key,
                            "start_ts" => self.lock_ts,
                            "commit_ts" => self.commit_ts,
                        );
                        // Commit with WriteType::Lock.
                        lock.lock_type = LockType::Lock;
                    }
                    let mut write = Write::new(
                        WriteType::from_lock_type(lock.lock_type).unwrap(),
                        self.lock_ts,
                        lock.short_value.take(),
                    );

                    for ts in &lock.rollback_ts {
                        if *ts == self.commit_ts {
                            write = write.set_overlapped_rollback(true);
                            break;
                        }
                    }

                    txn.put_write(key.clone(), self.commit_ts, write.as_ref().to_bytes());
                    Ok(txn.unlock_key(key.clone(), lock.is_pessimistic_txn()))
                }
                _ => match txn.reader.get_txn_commit_record(&key, self.lock_ts)?.info() {
                    Some((_, WriteType::Rollback)) | None => {
                        MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                        // None: related Rollback has been collapsed.
                        // Rollback: rollback by concurrent transaction.
                        info!(
                            "txn conflict (lock not found)";
                            "key" => %key,
                            "start_ts" => self.lock_ts,
                            "commit_ts" => self.commit_ts,
                        );
                        Err(MvccError::from(MvccErrorInner::TxnLockNotFound {
                            start_ts: self.lock_ts,
                            commit_ts: self.commit_ts,
                            key: key.to_raw()?,
                        })
                        .into())
                    }
                    // Committed by concurrent transaction.
                    Some((_, WriteType::Put))
                    | Some((_, WriteType::Delete))
                    | Some((_, WriteType::Lock)) => {
                        MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                        Ok(None)
                    }
                },
            };
            released_locks.push(result?);
        }
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::committed(self.commit_ts),
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::concurrency_manager::ConcurrencyManager;
    use crate::storage::kv::Engine;
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::mvcc::tests::*;
    use crate::storage::txn::commands::{WriteCommand, WriteContext};
    use crate::storage::TimeStamp;
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    pub fn must_success<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let mut ctx = Context::default();
        ctx.set_not_fill_cache(false);
        let snapshot = engine.snapshot(&ctx).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let key = Key::from_raw(key);
        let command = Commit {
            ctx: ctx.clone(),
            keys: vec![key],
            lock_ts: start_ts.into(),
            commit_ts: commit_ts.into(),
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &DummyLockManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    pipelined_pessimistic_lock: false,
                },
            )
            .unwrap();
        write(engine, &ctx, result.to_be_write.modifies);
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let mut ctx = Context::default();
        ctx.set_not_fill_cache(false);
        let snapshot = engine.snapshot(&ctx).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let key = Key::from_raw(key);
        let command = Commit {
            ctx: ctx.clone(),
            keys: vec![key],
            lock_ts: start_ts.into(),
            commit_ts: commit_ts.into(),
        };
        let result = command.process_write(
            snapshot,
            WriteContext {
                lock_mgr: &DummyLockManager,
                concurrency_manager: cm,
                extra_op: Default::default(),
                statistics: &mut Default::default(),
                pipelined_pessimistic_lock: false,
            },
        );
        assert!(result.is_err());
    }
}
