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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
// def KEY_LEN_SIZE, OFFSET_SIZE, VALUE_LEN_SIZE, EXTRA_FIELD_SIZE
pub(crate) const KEY_LEN_SIZE: usize = SIZEOF_U16;
pub(crate) const VALUE_LEN_SIZE: usize = SIZEOF_U16;
pub(crate) const OFFSET_SIZE: usize = SIZEOF_U16;
pub(crate) const EXTRA_FIELD_SIZE: usize = SIZEOF_U16;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    /// All serialized key-value pairs in the block. [Entry] = [key_len (2B) | key (keylen) | value_len (2B) | value (varlen)]
    pub(crate) data: Vec<u8>,
    /// Offsets of each key-value entries.
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf: Vec<u8> = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // Adds number of elements at the end of the block
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    /// Decode from the data layout into Block struct
    pub fn decode(data: &[u8]) -> Self {
        // get number of elements in the block
        let offsets_len = (&data[data.len() - EXTRA_FIELD_SIZE..]).get_u16() as usize;
        let data_end: usize = data.len() - EXTRA_FIELD_SIZE - offsets_len * OFFSET_SIZE;
        let offsets_raw = &data[data_end..data.len() - EXTRA_FIELD_SIZE];
        // decode offset array
        let offsets = offsets_raw
            .chunks(OFFSET_SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        // get data
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
