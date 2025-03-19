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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{
    BlockMeta, FileObject, SsTable, TABLE_BLOOM_OFFSET_SIZE, TABLE_META_ENTRY_OFFSET_SIZE,
    TABLE_META_OFFSET_SIZE,
};
use crate::block::{BlockBuilder, KEY_LEN_SIZE};
use crate::key::{KeySlice, KeyVec};
use crate::lsm_storage::BlockCache;

const BLOOM_EXPECTED_FALSE_POSITIVE_RATE: f64 = 0.01;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_meta_estimated_size: usize,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            block_size,
            block_meta_estimated_size: 0,
            builder: BlockBuilder::new(block_size),
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // create a new block builder and append block data
        self.finish_current_block();

        // add the key-value pair to the next block (assert that it is successful)
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable (not including bloom filter bits).
    pub fn estimated_size(&self) -> usize {
        self.data.len()
            + self.block_meta_estimated_size
            + TABLE_META_OFFSET_SIZE
            + TABLE_BLOOM_OFFSET_SIZE
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_current_block();

        // buf = data + meta(len + entries + checksum) + meta_offset + bloom(bits + k) + bloom_offset

        // bloom meta section
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf, self.block_meta_estimated_size);
        buf.put_u32(meta_offset as u32);

        // bloom filter section
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), BLOOM_EXPECTED_FALSE_POSITIVE_RATE),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0, // will be changed to latest ts in week 2
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    // Finish the current block, append it to the SSTable data, and update the meta information.
    fn finish_current_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();

        self.block_meta_estimated_size += TABLE_META_ENTRY_OFFSET_SIZE
            + KEY_LEN_SIZE
            + self.first_key.len()
            + KEY_LEN_SIZE
            + self.last_key.len();

        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });

        // TODO: faster checksum hash to reduce block write time
        // let checksum: u32 = crc32fast::hash(&encoded_block);
        let checksum: u32 = 0;
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
    }
}
