// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use crate::storage::kv::{Callback, Modify, SnapContext, WriteData};
use crate::storage::{Engine, RocksEngine};
use kvproto::kvrpcpb::Context;
use std::collections::LinkedList;
use std::sync::{Arc, Mutex};

/// A mock engine is a simple wrapper around RocksEngine
/// but with the ability to assert the modifies
#[derive(Clone)]
struct MockEngine {
    base: RocksEngine,
    expected_modifies: Arc<Mutex<LinkedList<Modify>>>,
}

impl Engine for MockEngine {
    type Snap = <RocksEngine as Engine>::Snap;
    type Local = <RocksEngine as Engine>::Local;

    fn kv_engine(&self) -> Self::Local {
        self.base.kv_engine()
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> Result<Self::Snap> {
        self.base.snapshot_on_kv_engine(start_key, end_key)
    }

    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()> {
        self.base.modify_on_kv_engine(modifies)
    }

    fn async_snapshot(&self, ctx: SnapContext<'_>, cb: Callback<Self::Snap>) -> Result<()> {
        self.base.async_snapshot(ctx, cb)
    }

    fn async_write(&self, ctx: &Context, batch: WriteData, write_cb: Callback<()>) -> Result<()> {
        let mut expected_modifies = self.expected_modifies.lock().unwrap();
        for modify in batch.modifies.iter() {
            if let Some(m) = expected_modifies.pop_front() {
                assert_eq!(
                    &m, modify,
                    "modify writing to Engine not match with expected"
                )
            } else {
                panic!("unexpected modify {:?} wrote to the Engine", modify)
            }
        }
        self.base.async_write(ctx, batch, write_cb)
    }
}

impl Drop for MockEngine {
    fn drop(&mut self) {
        let expected_modifies = &mut *self.expected_modifies.lock().unwrap();
        assert_eq!(
            expected_modifies.len(),
            0,
            "not all expected modifies have been written to the engine, {} rest",
            expected_modifies.len()
        )
    }
}
