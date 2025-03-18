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

use std::collections::{BTreeSet, HashMap};
use std::fs::{read, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
    /// if memtable has just been freeze
    pub memtable_freezed: bool,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)), // no use
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
            memtable_freezed: true,
        }
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of immutable memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
    pub block_cache_size: u64,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
            block_cache_size: 1 << 14, // 16KB
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
            block_cache_size: 1 << 14, // 16KB
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
            block_cache_size: 1 << 14, // 16KB
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;

        // wait for the compaction and flush threads to finish
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        // TODO: return directly without flush if enable wal

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?; // this may need change
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Flush memtable and all imm memtables, only call this in test cases due to race conditions (or user forced flush for test in some cases)
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() && !self.inner.state.read().memtable_freezed
        {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    // return the id of next sst table to push into the flush queue, which is the id of the latest creaeted immutable memtable
    pub(crate) fn next_sst_id(&self, is_new_mem_table: bool) -> usize {
        let next_sst_id = self
            .next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // if is_new_mem_table {
        //     println!("next_sst_id: {} (new_mem_table)", ret); // for debug test
        // } else {
        //     println!("next_sst_id: {} (compcation)", ret); // for debug test
        // }
        return next_sst_id;
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut state = LsmStorageState::create(&options);
        let path = path.as_ref();
        let mut next_sst_id = 1; // sst id starts with 1
        let block_cache = Arc::new(BlockCache::new(options.block_cache_size));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }

        let mut sst_cnt = 0;
        // recover SSTs
        for table_id in state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, files)| files))
        {
            let table_id = *table_id;
            let sst = SsTable::open(
                table_id,
                Some(block_cache.clone()),
                FileObject::open(&Self::path_of_sst_static(path, table_id))
                    .with_context(|| format!("failed to open SST: {}", table_id))?,
            )?;
            state.sstables.insert(table_id, Arc::new(sst));
            sst_cnt += 1;
        }
        println!("{} SSTs opened", sst_cnt);

        // TODO: codes about WAL and compaction

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;
        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // search the memtable first
        if let Some(value) = snapshot.memtable.get(_key) {
            if value.is_empty() {
                // empty string represents the tombstone
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // serach the immutable memtables if not found in the memtable
        for imm_memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = imm_memtable.get(_key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // prune ssts using key range and bloom filter
        let prune_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return false;
                    } else {
                        return true; // pruned by bloom filter
                    }
                } else {
                    return false;
                }
            }
            true // pruned by key range
        };

        let mut read_table_cnt = 0;

        // search the l0 sstables
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if prune_table(_key, &table) {
                continue;
            }
            let table_iter =
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(_key))?;
            read_table_cnt += 1;
            if table_iter.is_valid() && table_iter.key().raw_ref() == _key {
                // println!("get: {} SSTs read, get at L0", read_table_cnt); // for debug test
                if table_iter.value().is_empty() {
                    return Ok(None); // it's different from ' b"" '
                }
                return Ok(Some(Bytes::copy_from_slice(table_iter.value())));
            }
        }

        // search all sstables in level order, no need to search more sstables if found
        let mut level_index = 0;
        for (_, level_sst_ids) in &snapshot.levels {
            level_index += 1;
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if !prune_table(_key, &table) {
                    level_ssts.push(table);
                }
            }
            if level_ssts.is_empty() {
                continue;
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(_key))?;
            read_table_cnt += 1;
            if level_iter.is_valid() && level_iter.key().raw_ref() == _key {
                // println!(
                //     "get {:?}, value = {:?}: {} SSTs read, get at L{}, sst-{}",
                //     std::str::from_utf8(_key),
                //     std::str::from_utf8(level_iter.value()),
                //     read_table_cnt,
                //     level_index,
                //     level_iter.current_sstable().unwrap().sst_id()
                // ); // for debug test
                if level_iter.value().is_empty() {
                    return Ok(None); // it's different from ' b"" '
                }
                return Ok(Some(Bytes::copy_from_slice(level_iter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        {
            let guard = self.state.read();
            if guard.memtable_freezed {
                if guard.memtable_freezed {
                    drop(guard);
                    self.create_new_memtable();
                }
            }
        }

        {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            guard.memtable.put(_key, _value)?;
            // println!(
            //     "put: key = {:?}, value = {:?}, memtable_ssid = {:?}, memtable_size = {:?}",
            //     std::str::from_utf8(_key),
            //     std::str::from_utf8(_value),
            //     guard.memtable.id(),
            //     guard.memtable.approximate_size()
            // ); // for debug test

            // try freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                // println!("try_freeze: memtable_ssid = {:?}, approximate_size = {:?}", guard.memtable.id(), guard.memtable.approximate_size()); // for debug test
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, b"")
    }

    fn create_new_memtable(&self) {
        let mut guard = self.state.write();
        let mut snapshot: LsmStorageState = guard.as_ref().clone();
        let memtable_id = self.next_sst_id(true);
        snapshot.memtable = Arc::new(MemTable::create(memtable_id));
        snapshot.memtable_freezed = false;
        *guard = Arc::new(snapshot);
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                // println!("try_freeze: memtable_ssid = {:?}, approximate_size = {:?}", guard.memtable.id(), guard.memtable.approximate_size()); // for debug test
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mut guard = self.state.write();
        let mut snapshot: LsmStorageState = guard.as_ref().clone();
        snapshot.imm_memtables.insert(0, snapshot.memtable.clone());
        snapshot.memtable_freezed = true;

        // Update the snapshot.
        *guard = Arc::new(snapshot);
        drop(guard);

        self.sync_dir()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // flush the imm earliest memtable to disk
        let state_lock = self.state_lock.lock();
        let flush_memtable;
        {
            let guard = self.state.read();
            flush_memtable = guard
                .imm_memtables
                .last()
                .expect("no imm memtables!")
                .clone();
        }
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut sst_builder)?;
        let sst_id = flush_memtable.id();
        let sst = Arc::new(sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        // add the sst to the the sstable list
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.l0_sstables.insert(0, sst_id);
            snapshot.sstables.insert(sst_id, sst);
            // Remove the memtable from the immutable memtables.
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            *guard = Arc::new(snapshot);
        }

        // println!("force_flush_next_imm_memtable: sst-{} flushed", sst_id); // for debug test
        self.sync_dir()?;
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // mem table iter vec
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        // push memtable iter
        memtable_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        // push immutable memtable iters
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(_lower, _upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        // L0_SST
        let mut l0_table_iters: Vec<Box<SsTableIterator>> =
            Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table: Arc<SsTable> = snapshot.sstables[table_id].clone();
            if range_overlap(
                // skip SST tables with no range overlap
                _lower,
                _upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match _lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                l0_table_iters.push(Box::new(iter));
            }
        }

        // L1-Lmax SST
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if range_overlap(
                    _lower,
                    _upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key),
                    )?;
                    while iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        let l0_iter = MergeIterator::create(l0_table_iters);
        let memtable_l0_iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        let all_iter =
            TwoMergeIterator::create(memtable_l0_iter, MergeIterator::create(level_iters))?;

        Ok(FusedIterator::new(LsmIterator::new(
            all_iter,
            map_bound(_upper),
        )?))
    }
}
