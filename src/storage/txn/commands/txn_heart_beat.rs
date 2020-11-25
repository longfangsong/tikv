// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn};
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot, TxnStatus};
use txn_types::{Key, TimeStamp};

command! {
    /// Heart beat of a transaction. It enlarges the primary lock's TTL.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) or
    /// [`Prewrite`](Command::Prewrite).
    TxnHeartBeat:
        cmd_ty => TxnStatus,
        display => "kv::command::txn_heart_beat {} @ {} ttl {} | {:?}", (primary_key, start_ts, advise_ttl, ctx),
        content => {
            /// The primary key of the transaction.
            primary_key: Key,
            /// The transaction's start_ts.
            start_ts: TimeStamp,
            /// The new TTL that will be used to update the lock's TTL. If the lock's TTL is already
            /// greater than `advise_ttl`, nothing will happen.
            advise_ttl: u64,
        }
}

impl CommandExt for TxnHeartBeat {
    ctx!();
    tag!(txn_heart_beat);
    ts!(start_ts);
    write_bytes!(primary_key);
    gen_lock!(primary_key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for TxnHeartBeat {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // TxnHeartBeat never remove locks. No need to wake up waiters.
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        fail_point!("txn_heart_beat", |err| Err(
            crate::storage::mvcc::Error::from(crate::storage::mvcc::txn::make_txn_error(
                err,
                &self.primary_key,
                self.start_ts,
            ))
            .into()
        ));

        let lock = match txn.reader.load_lock(&self.primary_key)? {
            Some(mut lock) if lock.ts == self.start_ts => {
                if lock.ttl < self.advise_ttl {
                    lock.ttl = self.advise_ttl;
                    txn.put_lock(self.primary_key.clone(), &lock);
                }
                lock
            }
            _ => {
                context.statistics.add(&txn.take_statistics());
                return Err(MvccError::from(MvccErrorInner::TxnNotFound {
                    start_ts: self.start_ts,
                    key: self.primary_key.into_raw()?,
                })
                .into());
            }
        };

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::uncommitted(lock, false),
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::kv::TestEngineBuilder;
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::mvcc::tests::*;
    use crate::storage::txn::commands::WriteCommand;
    use crate::storage::txn::tests::*;
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;

    pub fn must_success<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        start_ts: impl Into<TimeStamp>,
        advise_ttl: u64,
        expect_ttl: u64,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let command = crate::storage::txn::commands::TxnHeartBeat {
            ctx: Context::default(),
            primary_key: Key::from_raw(primary_key),
            start_ts,
            advise_ttl,
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &DummyLockManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                },
            )
            .unwrap();
        if let ProcessResult::TxnStatus {
            txn_status: TxnStatus::Uncommitted { lock, .. },
        } = result.pr
        {
            write(engine, &ctx, result.to_be_write.modifies);
            assert_eq!(lock.ttl, expect_ttl);
        } else {
            unreachable!();
        }
    }

    pub fn must_err<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        start_ts: impl Into<TimeStamp>,
        advise_ttl: u64,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let command = crate::storage::txn::commands::TxnHeartBeat {
            ctx,
            primary_key: Key::from_raw(primary_key),
            start_ts,
            advise_ttl,
        };
        assert!(command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &DummyLockManager,
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                },
            )
            .is_err());
    }

    #[test]
    fn test_txn_heart_beat() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        let test = |ts| {
            // Do nothing if advise_ttl is less smaller than current TTL.
            must_success(&engine, k, ts, 90, 100);
            // Return the new TTL if the TTL when the TTL is updated.
            must_success(&engine, k, ts, 110, 110);
            // The lock's TTL is updated and persisted into the db.
            must_success(&engine, k, ts, 90, 110);
            // Heart beat another transaction's lock will lead to an error.
            must_err(&engine, k, ts - 1, 150);
            must_err(&engine, k, ts + 1, 150);
            // The existing lock is not changed.
            must_success(&engine, k, ts, 90, 110);
        };

        // No lock.
        must_err(&engine, k, 5, 100);

        // Create a lock with TTL=100.
        // The initial TTL will be set to 0 after calling must_prewrite_put. Update it first.
        must_prewrite_put(&engine, k, v, k, 5);
        must_locked(&engine, k, 5);
        must_success(&engine, k, 5, 100, 100);

        test(5);

        must_locked(&engine, k, 5);
        must_commit(&engine, k, 5, 10);
        must_unlocked(&engine, k);

        // No lock.
        must_err(&engine, k, 5, 100);
        must_err(&engine, k, 10, 100);

        must_acquire_pessimistic_lock(&engine, k, k, 8, 15);
        must_pessimistic_locked(&engine, k, 8, 15);
        must_success(&engine, k, 8, 100, 100);

        test(8);

        must_pessimistic_locked(&engine, k, 8, 15);
    }
}
