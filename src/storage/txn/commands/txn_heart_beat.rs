// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{ErrorInner as MvccErrorInner, MvccTxn, Result as MvccResult};
use crate::storage::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
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

impl TxnHeartBeat {
    /// Update a primary key's TTL if `advise_ttl > lock.ttl`.
    ///
    /// Returns the new TTL.
    pub fn txn_heart_beat<S: Snapshot>(&mut self, txn: &mut MvccTxn<S>) -> MvccResult<u64> {
        fail_point!("txn_heart_beat", |err| Err(crate::storage::mvcc::txn::make_txn_error(
            err,
            &self.primary_key,
            self.start_ts,
        )
        .into()));

        if let Some(mut lock) = txn.reader.load_lock(&self.primary_key)? {
            if lock.ts == self.start_ts {
                if lock.ttl < self.advise_ttl {
                    lock.ttl = self.advise_ttl;
                    txn.put_lock(self.primary_key.clone(), &lock);
                } else {
                    debug!(
                        "txn_heart_beat with advise_ttl not large than current ttl";
                        "primary_key" => %self.primary_key,
                        "start_ts" => self.start_ts,
                        "advise_ttl" => self.advise_ttl,
                        "current_ttl" => lock.ttl,
                    );
                }
                return Ok(lock.ttl);
            }
        }

        debug!(
            "txn_heart_beat invoked but lock is absent";
            "primary_key" => %self.primary_key,
            "start_ts" => self.start_ts,
            "advise_ttl" => self.advise_ttl,
        );
        Err(MvccErrorInner::TxnLockNotFound {
            start_ts: self.start_ts,
            commit_ts: TimeStamp::zero(),
            key: self.primary_key.clone().into_raw()?,
        }
        .into())
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for TxnHeartBeat {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // TxnHeartBeat never remove locks. No need to wake up waiters.
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        let lock_ttl = self.txn_heart_beat(&mut txn)?;

        context.statistics.add(&txn.take_statistics());
        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::uncommitted(lock_ttl, TimeStamp::zero(), false, vec![]),
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
