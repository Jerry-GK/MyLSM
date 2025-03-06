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

mod wrapper;

use rustyline::DefaultEditor;
use wrapper::mini_lsm_wrapper;

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use mini_lsm_wrapper::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
    TieredCompactionOptions,
};
use mini_lsm_wrapper::iterators::StorageIterator;
use mini_lsm_wrapper::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::os::macos::raw::stat;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    Leveled,
    Tiered,
    None,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "lsm.db")]
    path: PathBuf,
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,
    #[arg(long)]
    enable_wal: bool,
    #[arg(long)]
    serializable: bool,
}

struct ReplHandler {
    epoch: u64,
    lsm: Arc<MiniLsm>,
}

impl ReplHandler {
    fn handle(&mut self, command: &Command) -> Result<u128> {
        let start = std::time::Instant::now();
        let mut duration = 0; // microsecond

        match command {
            Command::Fill { begin, end } => {
                if *begin > *end {
                    println!("invalid range in `Fill` command!");
                    return Ok(0);
                }
                for i in *begin..=*end {
                    self.lsm.put(
                        format!("{}", i).as_bytes(),
                        format!("value{}@{}", i, self.epoch).as_bytes(),
                    )?;
                }
                duration = start.elapsed().as_nanos();

                println!(
                    "{} values filled with epoch {}",
                    end - begin + 1,
                    self.epoch
                );
            }
            Command::FillRandom { begin, end } => {
                if *begin > *end {
                    println!("invalid range in `FillRandom` command!");
                    return Ok(0);
                }
            
                let mut keys: Vec<u64> = (*begin..=*end).collect();
                use rand::seq::SliceRandom;
                use rand::thread_rng;
                
                let mut rng = thread_rng();
                keys.shuffle(&mut rng);
            
                for key in keys.iter() {
                    self.lsm.put(
                        format!("{}", key).as_bytes(),
                        format!("value{}@{}", key, self.epoch).as_bytes(),
                    )?;
                }
                
                duration = start.elapsed().as_nanos();
            
                println!(
                    "{} values filled in random order within range [{}, {}] with epoch {}",
                    end - begin + 1,
                    begin,
                    end,
                    self.epoch
                );
            }
            Command::Put { key, value } => {
                self.lsm.put(
                    format!("{}", key).as_bytes(),
                    format!("value{}@{}", value, self.epoch).as_bytes(),
                )?;
                duration = start.elapsed().as_nanos();
                println!("put success with epoch {}", self.epoch);
            }
            Command::Del { key } => {
                self.lsm.delete(key.as_bytes())?;
                duration = start.elapsed().as_nanos();
                println!("{} deleted", key);
            }
            Command::Get { key } => {
                if let Some(value) = self.lsm.get(key.as_bytes())? {
                    duration = start.elapsed().as_nanos();
                    println!("{}={:?}", key, value);
                } else {
                    duration = start.elapsed().as_nanos();
                    println!("{} not exist", key);
                }
            }
            Command::GetRange { begin, end } => {
                // use multiple get to implement scan, for test
                if *begin > *end {
                    println!("invalid range in `GetRange` command!");
                    return Ok(0);
                }

                let mut cnt = 0;
                let mut entries = Vec::new();
                for i in *begin..=*end {
                    let key_str = format!("{}", i);
                    let key_bytes = key_str.as_bytes();
                    if let Some(value) = self.lsm.get(key_bytes)? {
                        entries.push((key_bytes.to_vec(), value));
                        cnt += 1;
                    }
                }
                duration = start.elapsed().as_nanos();

                // // print is time consuming
                // for (key, value) in entries {
                //     println!(
                //         "{:?}={:?}",
                //         Bytes::copy_from_slice(&key),
                //         Bytes::copy_from_slice(&value)
                //     );
                // }
                // println!();
                println!("get {} keys in range", cnt);
            }
            Command::Scan { begin, end } => match (begin, end) {
                (None, None) => {
                    let mut iter = self
                        .lsm
                        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
                    let mut cnt = 0;
                    let mut entries = Vec::new();
                    while iter.is_valid() {
                        entries.push((iter.key().to_vec(), iter.value().to_vec()));
                        iter.next()?;
                        cnt += 1;
                    }
                    duration = start.elapsed().as_nanos();

                    // // print is time consuming
                    // for (key, value) in entries {
                    //     println!(
                    //         "{:?}={:?}",
                    //         Bytes::copy_from_slice(&key),
                    //         Bytes::copy_from_slice(&value)
                    //     );
                    // }
                    // println!();
                    println!("{} keys scanned", cnt);
                }
                (Some(begin), Some(end)) => {
                    let mut iter = self.lsm.scan(
                        std::ops::Bound::Included(begin.as_bytes()),
                        std::ops::Bound::Included(end.as_bytes()),
                    )?;
                    let mut cnt = 0;
                    let mut entries = Vec::new();
                    while iter.is_valid() {
                        entries.push((iter.key().to_vec(), iter.value().to_vec()));
                        iter.next()?;
                        cnt += 1;
                    }
                    duration = start.elapsed().as_nanos();

                    // // print is time consuming
                    // for (key, value) in entries {
                    //     println!(
                    //         "{:?}={:?}",
                    //         Bytes::copy_from_slice(&key),
                    //         Bytes::copy_from_slice(&value)
                    //     );
                    // }
                    // println!();
                    println!("{} keys scanned", cnt);
                }
                _ => {
                    println!("invalid command");
                }
            },
            Command::Execute { file_name } => {
                match std::fs::read_to_string(file_name) {
                    Ok(content) => {
                        let mut time_info = Vec::new();
                        let mut quit = false;
                        for line in content.lines() {
                            match Command::parse(line) {
                                Ok(command) => {
                                    // break the loop if command is close
                                    if let Command::Quit = command {
                                        quit = true;
                                        break;
                                    }
                                    let command_duration = self.handle(&command)?;
                                    duration += command_duration;
                                    time_info.push(format!("(command<{}> - execution time: {:.4}ms)", line, (command_duration as f64) / 1000000.0))
                                }
                                Err(e) => {
                                    println!("Invalid command: {}", e);
                                    continue;
                                }
                            };
                        }

                        println!("\nexecute script `{}` success", file_name);
                        for info in time_info {
                            println!("{}", info);
                        }
                        if quit {
                            self.lsm.close()?;
                            std::process::exit(0);
                        }
                    },
                    Err(e) => {
                        println!("Failed to read script `{}`: {}", file_name, e);
                    }
                }
            }
            Command::Dump => {
                self.lsm.dump_structure();
                duration = start.elapsed().as_nanos();
                println!("dump success");
            }
            Command::Flush => {
                self.lsm.force_flush()?;
                duration = start.elapsed().as_nanos();
                println!("flush success");
            }
            Command::FullCompaction => {
                self.lsm.force_full_compaction()?;
                duration = start.elapsed().as_nanos();
                println!("full compaction success");
            }
            Command::Quit => {
                self.lsm.close()?;
                std::process::exit(0);
            }
        };

        self.epoch += 1;

        Ok(duration)
    }
}

#[derive(Debug)]
enum Command {
    Fill {
        begin: u64,
        end: u64,
    },
    FillRandom {
        begin: u64,
        end: u64,
    },
    Put {
        key: u64,
        value: u64,
    },
    Del {
        key: String,
    },
    Get {
        key: String,
    },
    GetRange {
        begin: u64,
        end: u64,
    },
    Scan {
        begin: Option<String>,
        end: Option<String>,
    },
    Execute {
        file_name: String,
    },

    Dump,
    Flush,
    FullCompaction,
    Quit,
}

impl Command {
    pub fn parse(input: &str) -> Result<Self> {
        use nom::bytes::complete::*;
        use nom::character::complete::*;

        use nom::branch::*;
        use nom::combinator::*;
        use nom::sequence::*;

        let uint = |i| {
            map_res(digit1::<&str, nom::error::Error<_>>, |s: &str| {
                s.parse()
                    .map_err(|_| nom::error::Error::new(s, nom::error::ErrorKind::Digit))
            })(i)
        };

        let string = |i| {
            map(take_till1(|c: char| c.is_whitespace()), |s: &str| {
                s.to_string()
            })(i)
        };

        let fill = |i| {
            map(
                tuple((tag_no_case("fill"), space1, uint, space1, uint)),
                |(_, _, key, _, value)| Command::Fill {
                    begin: key,
                    end: value,
                },
            )(i)
        };

        let fill_random = |i| {
            map(
                tuple((tag_no_case("fillrandom"), space1, uint, space1, uint)),
                |(_, _, key, _, value)| Command::FillRandom {
                    begin: key,
                    end: value,
                },
            )(i)
        };

        let del = |i| {
            map(
                tuple((tag_no_case("del"), space1, string)),
                |(_, _, key)| Command::Del { key },
            )(i)
        };

        let put = |i| {
            map(
                tuple((tag_no_case("put"), space1, uint, space1, uint)),
                |(_, _, key, _, value)| Command::Put { key, value },
            )(i)
        };

        let get = |i| {
            map(
                tuple((tag_no_case("get"), space1, string)),
                |(_, _, key)| Command::Get { key },
            )(i)
        };

        let get_range = |i| {
            map(
                tuple((tag_no_case("getrange"), space1, uint, space1, uint)),
                |(_, _, begin, _, end)| Command::GetRange { begin, end },
            )(i)
        };

        let scan = |i| {
            map(
                tuple((
                    tag_no_case("scan"),
                    opt(tuple((space1, string, space1, string))),
                )),
                |(_, opt_args)| {
                    let (begin, end) = opt_args
                        .map_or((None, None), |(_, begin, _, end)| (Some(begin), Some(end)));
                    Command::Scan { begin, end }
                },
            )(i)
        };

        let execute = |i| {
            map(
                tuple((tag_no_case("execute"), space1, string)),
                |(_, _, file_name)| Command::Execute { file_name },
            )(i)
        };

        let command = |i| {
            alt((
                fill,
                fill_random,
                put,
                del,
                get,
                get_range,
                scan,
                execute,
                map(tag_no_case("dump"), |_| Command::Dump),
                map(tag_no_case("flush"), |_| Command::Flush),
                map(tag_no_case("full_compaction"), |_| Command::FullCompaction),
                map(tag_no_case("quit"), |_| Command::Quit),
                map(tag_no_case("close"), |_| Command::Quit),
                map(tag_no_case("exit"), |_| Command::Quit),
            ))(i)
        };

        command(input)
            .map(|(_, c)| c)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

struct Repl {
    app_name: String,
    description: String,
    prompt: String,

    handler: ReplHandler,

    editor: DefaultEditor,
}

impl Repl {
    pub fn run(mut self) -> Result<()> {
        self.bootstrap()?;

        loop {
            let readline = self.editor.readline(&self.prompt)?;
            if readline.trim().is_empty() {
                // Skip noop
                continue;
            }

            match Command::parse(&readline) {
                Ok(command) => {
                    let duration = self.handler.handle(&command)?;
                    println!("(execution time: {:.4}ms)", (duration as f64) / 1000000.0);
                    self.editor.add_history_entry(readline)?;
                }
                Err(e) => {
                    println!("Invalid command(enter 'close/exit' to exit): {}", e);
                    continue;
                }
            };
        }
    }

    fn bootstrap(&mut self) -> Result<()> {
        println!("Welcome to {}!", self.app_name);
        println!("{}", self.description);
        println!();
        Ok(())
    }
}

struct ReplBuilder {
    app_name: String,
    description: String,
    prompt: String,
}

impl ReplBuilder {
    pub fn new() -> Self {
        Self {
            app_name: "mini-lsm-cli".to_string(),
            description: "A CLI for mini-lsm".to_string(),
            prompt: "mini-lsm-cli> ".to_string(),
        }
    }

    pub fn app_name(mut self, app_name: &str) -> Self {
        self.app_name = app_name.to_string();
        self
    }

    pub fn description(mut self, description: &str) -> Self {
        self.description = description.to_string();
        self
    }

    pub fn prompt(mut self, prompt: &str) -> Self {
        self.prompt = prompt.to_string();
        self
    }

    pub fn build(self, handler: ReplHandler) -> Result<Repl> {
        Ok(Repl {
            app_name: self.app_name,
            description: self.description,
            prompt: self.prompt,
            editor: DefaultEditor::new()?,
            handler,
        })
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let lsm = MiniLsm::open(
        args.path,
        LsmStorageOptions {
            block_size: 4096,         // 4KB
            target_sst_size: 2 << 20, // 2MB
            num_memtable_limit: 3,
            compaction_options: match args.compaction {
                CompactionStrategy::None => CompactionOptions::NoCompaction,
                CompactionStrategy::Simple => {
                    CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                        size_ratio_percent: 200,
                        level0_file_num_compaction_trigger: 2,
                        max_levels: 4,
                    })
                }
                CompactionStrategy::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
                    num_tiers: 3,
                    max_size_amplification_percent: 200,
                    size_ratio: 1,
                    min_merge_width: 2,
                    max_merge_width: None,
                }),
                CompactionStrategy::Leveled => {
                    CompactionOptions::Leveled(LeveledCompactionOptions {
                        level0_file_num_compaction_trigger: 2,
                        max_levels: 4,
                        base_level_size_mb: 128,
                        level_size_multiplier: 2,
                    })
                }
            },
            enable_wal: args.enable_wal,
            serializable: args.serializable,
            block_cache_size: 2 << 25, // 32MB
            // block_cache_size: 0, // 0MB
        },
    )?;

    let repl = ReplBuilder::new()
        .app_name("mini-lsm-cli")
        .description("A CLI for mini-lsm")
        .prompt("mini-lsm-cli> ")
        .build(ReplHandler { epoch: 0, lsm })?;

    repl.run()?;
    Ok(())
}
