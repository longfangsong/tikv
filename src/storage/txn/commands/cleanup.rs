// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC};
use crate::storage::mvcc::txn::make_txn_error;
use crate::storage::mvcc::txn::MissingLockAction;
use crate::storage::mvcc::{
    Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, Result as MvccResult,
};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot, TxnStatus};

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Cleanup:
        cmd_ty => (),
        display => "kv::command::cleanup {} @ {} | {:?}", (key, start_ts, ctx),
        content => {
            key: Key,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The approximate current ts when cleanup request is invoked, which is used to check the
            /// lock's TTL. 0 means do not check TTL.
            current_ts: TimeStamp,
        }
}

impl CommandExt for Cleanup {
    ctx!();
    tag!(cleanup);
    ts!(start_ts);
    write_bytes!(key);
    gen_lock!(key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Cleanup {
    // Cleanup the lock if it's TTL has expired, comparing with `current_ts`. If `current_ts` is 0, cleanup the lock without checking TTL.
    // If the lock is the primary lock of a pessimistic transaction, the rollback record is protected from being collapsed.
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );

        let mut released_locks = ReleasedLocks::new(self.start_ts, TimeStamp::zero());

        fail_point!("cleanup", |err| Err(MvccError::from(make_txn_error(
            err,
            &self.key,
            self.start_ts,
        ))
        .into()));

        let released_lock: MvccResult<_> = match txn.reader.load_lock(&self.key)? {
            Some(ref lock) if lock.ts == self.start_ts => {
                // If current_ts is not 0, check the Lock's TTL.
                // If the lock is not expired, do not rollback it but report key is locked.
                if !self.current_ts.is_zero()
                    && lock.ts.physical() + lock.ttl >= self.current_ts.physical()
                {
                    Err(MvccErrorInner::KeyIsLocked(
                        lock.clone().into_lock_info(self.key.into_raw()?),
                    )
                    .into())
                } else {
                    let is_pessimistic_txn = !lock.for_update_ts.is_zero();
                    txn.check_write_and_rollback_lock(self.key, lock, is_pessimistic_txn)
                }
            }
            l => match txn.check_txn_status_missing_lock(
                self.key,
                l,
                MissingLockAction::rollback_protect(true),
            )? {
                TxnStatus::Committed { commit_ts } => {
                    MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                    Err(MvccErrorInner::Committed { commit_ts }.into())
                }
                TxnStatus::RolledBack => {
                    // Return Ok on Rollback already exist.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                    Ok(None)
                }
                TxnStatus::LockNotExist => Ok(None),
                _ => unreachable!(),
            },
        };

        released_locks.push(released_lock?);
        released_locks.wake_up(context.lock_mgr);

        context.statistics.add(&txn.take_statistics());
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr: ProcessResult::Res,
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
    use crate::storage::txn::commands::{txn_heart_beat, WriteCommand, WriteContext};
    use crate::storage::TestEngineBuilder;
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    pub fn must_success<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let command = Cleanup {
            ctx: ctx.clone(),
            key: Key::from_raw(key),
            start_ts: start_ts.into(),
            current_ts,
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
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let command = Cleanup {
            ctx: ctx.clone(),
            key: Key::from_raw(key),
            start_ts: start_ts.into(),
            current_ts,
        };
        assert!(command
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
            .is_err());
    }

    #[test]
    fn test_cleanup() {
        // Cleanup's logic is mostly similar to rollback, except the TTL check. Tests that not
        // related to TTL check should be covered by other test cases.
        let engine = TestEngineBuilder::new().build().unwrap();

        // Shorthand for composing ts.
        let ts = TimeStamp::compose;

        let (k, v) = (b"k", b"v");

        must_prewrite_put(&engine, k, v, k, ts(10, 0));
        must_locked(&engine, k, ts(10, 0));
        txn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 100, 100);
        // Check the last txn_heart_beat has set the lock's TTL to 100.
        txn_heart_beat::tests::must_success(&engine, k, ts(10, 0), 90, 100);

        // TTL not expired. Do nothing but returns an error.
        must_err(&engine, k, ts(10, 0), ts(20, 0));
        must_locked(&engine, k, ts(10, 0));

        // Try to cleanup another transaction's lock. Does nothing.
        must_success(&engine, k, ts(10, 1), ts(120, 0));
        // If there is no exisiting lock when cleanup, it may be a pessimistic transaction,
        // so the rollback should be protected.
        must_get_rollback_protected(&engine, k, ts(10, 1), true);
        must_locked(&engine, k, ts(10, 0));

        // TTL expired. The lock should be removed.
        must_success(&engine, k, ts(10, 0), ts(120, 0));
        must_unlocked(&engine, k);
        // Rollbacks of optimistic transactions needn't be protected
        must_get_rollback_protected(&engine, k, ts(10, 0), false);
        must_get_rollback_ts(&engine, k, ts(10, 0));

        // Rollbacks of primary keys in pessimistic transactions should be protected
        must_acquire_pessimistic_lock(&engine, k, k, ts(11, 1), ts(12, 1));
        must_success(&engine, k, ts(11, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(11, 1), true);

        must_acquire_pessimistic_lock(&engine, k, k, ts(13, 1), ts(14, 1));
        must_pessimistic_prewrite_put(&engine, k, v, k, ts(13, 1), ts(14, 1), true);
        must_success(&engine, k, ts(13, 1), ts(120, 0));
        must_get_rollback_protected(&engine, k, ts(13, 1), true);
    }
}
