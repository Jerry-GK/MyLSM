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

use bytes::BufMut;

use crate::{
    block::{KEY_LEN_SIZE, VALUE_LEN_SIZE},
    key::KeySlice,
};

use super::{Block, BLOCK_DATA_ENTRY_OFFSET_SIZE, BLOCK_NUM_ELEMENT_SIZE};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block. [Entry] = [key_len (2B) | key (keylen) | value_len (2B) | value (varlen)]
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }

    fn estimated_size(&self) -> usize {
        BLOCK_NUM_ELEMENT_SIZE /* number of key-value pairs(Extra field) */ +  self.offsets.len() * BLOCK_DATA_ENTRY_OFFSET_SIZE /* offsets */ + self.data.len()
        /* key-value pairs */
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        // can't add if exceeds block size unless it's the first entry
        if self.estimated_size()
            + key.len()
            + value.len()
            + KEY_LEN_SIZE
            + VALUE_LEN_SIZE
            + BLOCK_DATA_ENTRY_OFFSET_SIZE
            > self.block_size
            && !self.is_empty()
        {
            return false;
        }

        // record the offset
        self.offsets.push(self.data.len() as u16);
        // Encode key length.
        self.data.put_u16(key.len() as u16);
        // Encode key content.
        self.data.put(key.raw_ref());
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
