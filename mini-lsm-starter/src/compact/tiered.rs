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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize, // the minimum number of tiers to trigger compaction
    pub max_size_amplification_percent: usize, // the maximum size amplification percent to trigger full compaction
    pub size_ratio: usize, // the min size ratio to trigger compaction between upper tiers and current tier
    pub min_merge_width: usize, // the minimum number of tiers to compact for size_ratio triggered compaction
    pub max_merge_width: Option<usize>, // the maximum number of tiers to compact for tier-reduction triggered compaction
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );

        // no compaction if levels are less than num_tiers
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // compaction triggered by space amplification ratio (reduce write amplification, compact to the bottom level)
        let mut non_bottom_level_total_size = 0;
        for id in 0..(_snapshot.levels.len() - 1) {
            non_bottom_level_total_size += _snapshot.levels[id].1.len();
        }
        let space_amp_ratio = (non_bottom_level_total_size as f64)
            / (_snapshot.levels.last().unwrap().1.len() as f64)
            * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            let bottom_tier_included = true;
            println!(
                "compaction triggered by space amplification ratio: {}, compact L1~{}, bottom_tier_included = {}",
                space_amp_ratio,
                _snapshot.levels.len(),
                bottom_tier_included,
            );
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included,
            });
        }

        // compaction triggered by size ratio (reduce read amplification)
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;
        for level_idx in 0..(_snapshot.levels.len() - 1) {
            size += _snapshot.levels[level_idx].1.len();
            let next_level_size = _snapshot.levels[level_idx + 1].1.len();
            let cur_size_ratio = next_level_size as f64 / size as f64;
            if cur_size_ratio >= size_ratio_trigger && level_idx + 1 >= self.options.min_merge_width
            {
                let bottom_tier_included = level_idx + 1 >= _snapshot.levels.len();
                println!(
                    "compaction triggered by size ratio: {} > {}, compact L1~{}, bottom_tier_included = {}",
                    cur_size_ratio * 100.0,
                    size_ratio_trigger * 100.0,
                    level_idx + 1,
                    bottom_tier_included,
                );
                return Some(TieredCompactionTask {
                    tiers: _snapshot
                        .levels
                        .iter()
                        .take(level_idx + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included,
                });
            }
        }

        // compaction triggered by reducing sorted runs (reduce sorted runs)
        let num_tiers_to_take = _snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        let bottom_tier_included = num_tiers_to_take + 1 >= _snapshot.levels.len();
        println!(
            "compaction triggered by reducing sorted runs, compact L1~{}, bottom_tier_included = {}", 
            num_tiers_to_take,
            bottom_tier_included,
        );
        Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );

        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut tier_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;

        // remove tiers involved in compaction and insert the new compacted tier (one SST)
        for (tier_id, files) in &snapshot.levels {
            if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                // removed tier in in a compaction task
                assert_eq!(ffiles, files, "file changed after issuing compaction task");
                files_to_remove.extend(ffiles.iter().copied());
            } else {
                // retain the tier not involved in compaction
                levels.push((*tier_id, files.clone()));
            }

            if tier_to_remove.is_empty() && !new_tier_added {
                // add the new compacted tier to the LSM tree
                // tiered compaction only produces one SST in output
                new_tier_added = true;
                levels.push((_output[0], _output.to_vec()));
                // dont break here, tiers below unaffected by compaction should be kept
            }
        }
        if !tier_to_remove.is_empty() && new_tier_added {
            unreachable!("some tiers not found in compaction task?");
        }

        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
