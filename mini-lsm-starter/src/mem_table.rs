// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::block::{BLOCK_DATA_ENTRY_OFFSET_SIZE, KEY_LEN_SIZE, VALUE_LEN_SIZE};
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
    // is_freezed: true if just been flushed, set to false when newly created.
    // memtable is invalid and regarded as empty when is_freezed is true even it has not been deleted.
    is_freezed: Arc<AtomicBool>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        Self {
            id: _id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
            is_freezed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id: _id,
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(_path.as_ref())?),
            approximate_size: Arc::new(AtomicUsize::new(0)),
            is_freezed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        Ok(Self {
            id: _id,
            wal: Some(Wal::recover(_path.as_ref(), &map)?),
            map,
            approximate_size: Arc::new(AtomicUsize::new(0)),
            is_freezed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        self.map.get(_key).map(|v| v.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let estimated_size = _key.len()
            + _value.len()
            + KEY_LEN_SIZE
            + VALUE_LEN_SIZE
            + BLOCK_DATA_ENTRY_OFFSET_SIZE; // Q: can we estimate the flushed sst table size more precisely?
        self.map
            .insert(Bytes::copy_from_slice(_key), Bytes::copy_from_slice(_value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);

        if let Some(ref wal) = self.wal {
            wal.put(_key, _value)?;
        }

        // self.sync_wal()?; // sync wal after each put to guarantee strong recovery consistency, but very time-consuming for put

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let lower = map_bound(_lower);
        let upper = map_bound(_upper);

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }
        .build();

        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6. (time consuming on write path)
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            let key = KeySlice::from_slice(entry.key());
            _builder.add(key, entry.value());
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_freezed(&self, freezed: bool) {
        self.is_freezed.store(freezed, Ordering::SeqCst);
    }

    pub fn is_freezed(&self) -> bool {
        self.is_freezed.load(Ordering::SeqCst)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty() || self.is_freezed()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.borrow_item().0.as_ref())
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
