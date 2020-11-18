use crate::storage::mvcc::{seek_for_valid_write, MvccTxn, Result as MvccResult};
use crate::storage::Snapshot;
use txn_types::{Key, OldValue, Write, WriteType};

pub fn get_old_value<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    prev_write: Option<Write>,
) -> MvccResult<Option<OldValue>> {
    Ok(if let Some(w) = prev_write {
        // If write is Rollback or Lock, seek for valid write record.
        if w.write_type == WriteType::Rollback || w.write_type == WriteType::Lock {
            let write_cursor = txn.reader.write_cursor.as_mut().unwrap();
            // Skip the current write record.
            write_cursor.next(&mut txn.reader.statistics.write);
            let write =
                seek_for_valid_write(write_cursor, key, txn.start_ts, &mut txn.reader.statistics)?;
            write.map(|w| OldValue {
                short_value: w.short_value,
                start_ts: w.start_ts,
            })
        } else {
            Some(OldValue {
                short_value: w.short_value,
                start_ts: w.start_ts,
            })
        }
    } else {
        None
    })
}
