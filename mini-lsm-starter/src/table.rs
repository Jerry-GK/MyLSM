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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();
pub(crate) const TABLE_BLOCK_META_LEN_SIZE: usize = SIZEOF_U32;
pub(crate) const TABLE_BLOCK_CHECKSUM_SIZE: usize = SIZEOF_U32;
pub(crate) const TABLE_META_CHECKSUM_SIZE: usize = SIZEOF_U32;
pub(crate) const TABLE_META_ENTRY_OFFSET_SIZE: usize = SIZEOF_U32;
pub(crate) const TABLE_META_OFFSET_SIZE: usize = SIZEOF_U32;
pub(crate) const TABLE_BLOOM_OFFSET_SIZE: usize = SIZEOF_U32;

#[derive(Clone, Debug, PartialEq, Eq)]

pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    /// BlockMeta = BlockMetaLen + [BlockMetaEntry] * BlockMetaLen + Checksum
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
        block_meta_estimated_size: usize,
    ) {
        let reserved_size =
            TABLE_BLOCK_META_LEN_SIZE + block_meta_estimated_size + TABLE_META_CHECKSUM_SIZE;
        buf.reserve(reserved_size);
        buf.put_u32(block_meta.len() as u32);
        let checksum_calc_offset = buf.len(); // checksum only hash BlockMeta Entries
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        buf.put_u32(crc32fast::hash(&buf[checksum_calc_offset..]));
        assert_eq!(
            reserved_size,
            buf.len() - checksum_calc_offset + TABLE_BLOCK_META_LEN_SIZE
        );
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        let block_meta_len: usize = buf.get_u32() as usize;
        let checksum: u32 = crc32fast::hash(&buf[..buf.remaining() - TABLE_META_CHECKSUM_SIZE]);
        for _ in 0..block_meta_len {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }

        Ok(block_meta)
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks (always cached in memory, about 0.2% of data size).
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file. (Load block meta into memory)
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();

        let raw_bloom_offset = file.read(
            len - TABLE_BLOOM_OFFSET_SIZE as u64,
            TABLE_BLOOM_OFFSET_SIZE as u64,
        )?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(
            bloom_offset,
            len - bloom_offset - TABLE_BLOOM_OFFSET_SIZE as u64,
        )?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;

        let raw_meta_offset = file.read(
            bloom_offset - TABLE_META_OFFSET_SIZE as u64,
            TABLE_META_OFFSET_SIZE as u64,
        )?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(
            block_meta_offset,
            bloom_offset - block_meta_offset - TABLE_META_OFFSET_SIZE as u64,
        )?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;

        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // let mut start = std::time::Instant::now();
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_len = offset_end - offset - TABLE_BLOCK_CHECKSUM_SIZE;
        let block_data_with_checksum: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?; // IO here! (time consuming on read path)
        let block_data: &[u8] = &block_data_with_checksum[..block_len];

        // TODO: faster checksum hash to reduce read time
        // let checksum = (&block_data_with_checksum[block_len..]).get_u32();
        // if checksum != crc32fast::hash(block_data) {
        //     bail!("block checksum mismatched");
        // }

        // let duration_io = start.elapsed().as_micros();

        // start = std::time::Instant::now();
        let block = Arc::new(Block::decode(block_data));
        // let duration_decode = start.elapsed().as_micros();

        // println!(
        //     "(I/O: Read block from disk, sst-{}-block{}, IO cost: {:.4}ms, decode cost: {:.4}ms)",
        //     self.sst_id(),
        //     block_idx,
        //     (duration_io as f64) / 1000.0,
        //     (duration_decode as f64) / 1000.0
        // );
        Ok(block)
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let enable_block_cache = true; // for test
        if !enable_block_cache {
            return self.read_block(block_idx);
        }

        if let Some(ref block_cache) = self.block_cache {
            let blk: Arc<Block> = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    /// Note: If the key is larger than the last key of block[n-1], but smaller than the first key of block[n] (or n > BlockLen), it will return n.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // this method returns the first element that does not satisfy the predicate, using binary search
        self.block_meta
            .partition_point(|meta| meta.last_key.as_key_slice() < key)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
