## Mini-LSM Q&A 

(Some answers are generated by deepseek)

### Own:

- 本实现与标准实现主要有哪些区别？
    - 本实现不会创建一个空的、可能不会写入的memtable，而是在flush后，下次put时，如果发现没有可用memtable（上一个memtable已被freezed/flushed），那么创建个新的memtable再插入。这样更合理、代码也更明确（创建memtable只在一处），并且不会出现sst的id跳跃的情况（因为项目中不会flush空的memtable成sst文件，标准实现存在这个问题）。
    - 较大改进了mini-lsm-cli，增加了FillRandom、GetRange，并支持了运行脚本，方便测试。

### Week1

#### W1D1: Memtable

- Why doesn't the memtable provide a `delete` API?

    删除操作通过插入墓碑标记（tombstone）实现，而非物理删除，确保后续合并时能正确处理。

    LSM树的墓碑标记可以在不同层起到版本控制的作用，并不只是为了方便。

- Does it make sense for the memtable to store all write operations instead of only the latest version of a key? For example, the user puts a->1, a->2, and a->3 into the same memtable.

    不需要，LSM通过合并过程处理历史版本，memtable只需保留最新值以减少内存占用。

- Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist?

    可以（如B树、哈希表）。跳表支持有序遍历且并发友好，实现简单，但内存局部性较差。

- Why do we need a combination of `state` and `state_lock`? Can we only use `state.read()` and `state.write()`?

    读写锁（`state_lock`）确保并发安全，`state`存储实际数据，单独使用无法保证原子性。

- Why does the order to store and to probe the memtables matter? If a key appears in multiple memtables, which version should you return to the user?

    查询时从新到旧遍历memtable，返回最新版本（覆盖旧值），其中id越大、越晚生成的memtable中的数据越新。

- Is the memory layout of the memtable efficient / does it have good data locality? (Think of how `Byte` is implemented and stored in the skiplist...) What are the possible optimizations to make the memtable more efficient?

    跳表的节点分散存储（`Byte`为堆分配），数据局部性差。优化：预分配连续内存块。

- So we are using `parking_lot` locks in this course. Is its read-write lock a fair lock? What might happen to the readers trying to acquire the lock if there is one writer waiting for existing readers to stop?

    非公平锁，可能导致写者饥饿（新读者可能优先于等待的写者获取锁）。

- After freezing the memtable, is it possible that some threads still hold the old LSM state and wrote into these immutable memtables? How does your solution prevent it from happening?

    不会，冻结后切换为新memtable，旧memtable变为只读，通过状态锁保证一致性。

- There are several places that you might first acquire a read lock on state, then drop it and acquire a write lock (these two operations might be in different functions but they happened sequentially due to one function calls the other). How does it differ from directly upgrading the read lock to a write lock? Is it necessary to upgrade instead of acquiring and dropping and what is the cost of doing the upgrade?

    直接升级可能死锁，分开操作更安全但可能引入竞态，需权衡性能与正确性。

#### W1D2: Merge iterator

- What is the time/space complexity of using your merge iterator?

    时间O(n log k)（k为迭代器数），空间O(k)（堆大小）。

- Why do we need a self-referential structure for memtable iterator?

    Rust生命周期限制，避免迭代器引用memtable时所有权问题。

- If a key is removed (there is a delete tombstone), do you need to return it to the user? Where did you handle this logic?

    不返回，合并迭代器过滤墓碑标记（逻辑在读取时处理）。

- If a key has multiple versions, will the user see all of them? Where did you handle this logic?

    否，合并迭代器返回最新版本（按遍历顺序覆盖旧值），通过以堆或TwoMerge为基础的迭代器实现。

- If we want to get rid of self-referential structure and have a lifetime on the memtable iterator (i.e., `MemtableIterator<'a>`, where `'a` = memtable or `LsmStorageInner` lifetime), is it still possible to implement the `scan` functionality?

    可以，但需明确生命周期绑定（如`'a`关联存储引擎或memtable）。

- What happens if (1) we create an iterator on the skiplist memtable (2) someone inserts new keys into the memtable (3) will the iterator see the new key?

    不会，迭代器基于冻结的memtable快照，新写入不影响已创建的迭代器。

- What happens if your key comparator cannot give the binary heap implementation a stable order?

    导致堆顺序错误，可能返回错误的最新版本。

- Why do we need to ensure the merge iterator returns data in the iterator construction order?

    确保数据优先级（如memtable顺序），正确覆盖旧值，返回最新版本。

- Is it possible to implement a Rust-style iterator (i.e., `next(&self) -> (Key, Value)`) for LSM iterators? What are the pros/cons?

    不可行，需维护可变状态（如游标），`&mut self`更合适。 （Not sure）

- The scan interface is like `fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>)`. How to make this API compatible with Rust-style range (i.e., `key_a..key_b`)? If you implement this, try to pass a full range `..` to the interface and see what will happen.

    转换为无界`Bound`，需处理空范围或全范围查询。

- The starter code provides the merge iterator interface to store `Box<I>` instead of `I`. What might be the reason behind that?

    统一不同迭代器类型，动态分发，简化生命周期管理。

#### W1D3: Block

- What is the time complexity of seeking a key in the block?

    O(log n)（二分查找）。一个SST内的block也是按顺序的，也是按此复杂度根据meta定位block。

- Where does the cursor stop when you seek a non-existent key in your implementation?

    停在第一个大于等于该键的位置（符合迭代语义）。后续处理逻辑中会通过比较决定是否返回。

- So `Block` is simply a vector of raw data and a vector of offsets. Can we change them to `Byte` and `Arc<[u16]>`, and change all the iterator interfaces to return `Byte` instead of `&[u8]`? (Assume that we use `Byte::slice` to return a slice of the block without copying.) What are the pros/cons?

    可行，减少拷贝（`Byte`切片零拷贝），但增加内存碎片风险。

- What is the endian of the numbers written into the blocks in your implementation?

    小端。

- Is your implementation prune to a maliciously-built block? Will there be invalid memory access, or OOMs, if a user deliberately construct an invalid block?

    是，需校验offset范围防止越界/OOM（如偏移超出数据长度）。

- Can a block contain duplicated keys?

    不应存在，SST构建时保证键有序且唯一，这与memtable更新时不保存历史记录对应。

- What happens if the user adds a key larger than the target block size?

    强制分割到新block，确保单个block不超过大小限制。如果这条记录本身就超过了target block size，那么允许这一个block超出大小、单独存储这条记录。

- Consider the case that the LSM engine is built on object store services (S3). How would you optimize/change the block format and parameters to make it suitable for such services?

    增大block大小（如1MB），减少请求数；预取相邻块。

- Do you love bubble tea? Why or why not?

    Yes

#### W1D4: SST

- What is the time complexity of seeking a key in the SST?

    O(log n)（二分查找索引，再块内二分）。

- Where does the cursor stop when you seek a non-existent key in your implementation?

    同block逻辑，停在首个大于等于键的位置。

- Is it possible (or necessary) to do in-place updates of SST files?

    不必要，SST不可变，通过合并生成新文件。这是LSM树的核心思想与目的之一。

- An SST is usually large (i.e., 256MB). In this case, the cost of copying/expanding the `Vec` would be significant. Does your implementation allocate enough space for your SST builder in advance? How did you implement it?

    是，预先分配`Vec`容量避免频繁扩容（如按block大小预计算）。

- Looking at the `moka` block cache, why does it return `Arc<Error>` instead of the original `Error`?

    允许多线程共享错误，避免克隆`Error`类型。

- Does the usage of a block cache guarantee that there will be at most a fixed number of blocks in memory? For example, if you have a `moka` block cache of 4GB and block size of 4KB, will there be more than 4GB/4KB number of blocks in memory at the same time?

    不保证，缓存可能超过容量（如热点数据重复加载）。（Not sure）

- Is it possible to store columnar data (i.e., a table of 100 integer columns) in an LSM engine? Is the current SST format still a good choice?

    可能，但当前行式存储不适合列扫描，需调整格式（如列分块）。

- Consider the case that the LSM engine is built on object store services (i.e., S3). How would you optimize/change the SST format/parameters and the block cache to make it suitable for such services?

    分层存储元数据，延迟加载索引，缓存热点块。增大block大小（如1MB），减少请求数；预取相邻块。

- For now, we load the index of all SSTs into the memory. Assume you have a 16GB memory reserved for the indexes, can you estimate the maximum size of the database your LSM system can support? (That's why you need an index cache!)

    假设每个SST索引块大小4KB，支持约400万SST，总数据量约PB级（假设256MB/SST）。

    

#### W1D5: Read path

- Consider the case that a user has an iterator that iterates the whole storage engine, and the storage engine is 1TB large, so that it takes ~1 hour to scan all the data. What would be the problems if the user does so? (This is a good question and we will ask it several times at different points of the course...)

    长时迭代占用资源（内存/文件句柄），可能阻塞合并或写入。

- Another popular interface provided by some LSM-tree storage engines is multi-get (or vectored get). The user can pass a list of keys that they want to retrieve. The interface returns the value of each of the key. For example, `multi_get(vec!["a", "b", "c", "d"]) -> a=1,b=2,c=3,d=4`. Obviously, an easy implementation is to simply doing a single get for each of the key. How will you implement the multi-get interface, and what optimizations you can do to make it more efficient? (Hint: some operations during the get process will only need to be done once for all keys, and besides that, you can think of an improved disk I/O interface to better support this multi-get interface).

    批量加载重叠SST块，预取布隆过滤器，并行查找不同层级。也就是将定位SST的过程合并，而不是单独独立执行get。这种场景与scan还有不同，由于scan的数据局部性，使用迭代器访问后续数据即可。

#### W1D6: Write path

- What happens if a user requests to delete a key twice?

    如果在同一个memtable中，那么跟只是又覆盖写了一次墓碑标记，没什么影响。如果写在了不同memtable，那么可能存在两个SST都含有墓碑标记，最终合并到底层时被删除。

- How much memory (or number of blocks) will be loaded into memory at the same time when the iterator is initialized?

    仅加载SST索引，数据块按需加载（受缓存控制）。

- Some crazy users want to *fork* their LSM tree. They want to start the engine to ingest some data, and then fork it, so that they get two identical dataset and then operate on them separately. An easy but not efficient way to implement is to simply copy all SSTs and the in-memory structures to a new directory and start the engine. However, note that we never modify the on-disk files, and we can actually reuse the SST files from the parent engine. How do you think you can implement this fork functionality efficiently without copying data? (Check out [Neon Branching](https://neon.tech/docs/introduction/branching)).

    硬链接SST文件（只读共享），复制内存状态，独立WAL。

- Imagine you are building a multi-tenant LSM system where you host 10k databases on a single 128GB memory machine. The memtable size limit is set to 256MB. How much memory for memtable do you need for this setup?
  
    - Obviously, you don't have enough memory for all these memtables. Assume each user still has their own memtable, how can you design the memtable flush policy to make it work? Does it make sense to make all these users share the same memtable (i.e., by encoding a tenant ID as the key prefix)?
    
    10k*256MB=2.5TB，远超内存。解决方案：分时刷新或共享memtable（前缀隔离租户）。

#### W1D7: Bloom filter

- How does the bloom filter help with the SST filtering process? What kind of information can it tell you about a key? (may not exist/may exist/must exist/must not exist)

    判断“键可能不存在”，减少不必要的磁盘查找。实际中对SST的过滤性非常好（只要肯牺牲一定空间和哈希时间）

- Consider the case that we need a backward iterator. Does our key compression affect backward iterators?

    是，前缀压缩可能丢失细节，反向遍历需解压完整键。

- Can you use bloom filters on scan?

    基本无效，布隆过滤器仅适用于点查，扫描需遍历所有或一定范围的数据，很容易出现假阳性。

- What might be the pros/cons of doing key-prefix encoding over adjacent keys instead of with the first key in the block?

    减少存储，但可能增加误判（前缀相同但实际键不同）。

    

### Week2

#### W2D1: Compaction implementation

- What are the definitions of read/write/space amplifications? (This is covered in the overview chapter)

    读放大：查询时访问的数据量；写放大：写入数据量与实际写入量的比值；空间放大：磁盘占用与实际数据量的比值。

- What are the ways to accurately compute the read/write/space amplifications, and what are the ways to estimate them?

    精确计算需统计所有操作，估算基于层级大小和合并策略。

- Is it correct that a key will take some storage space even if a user requests to delete it?

    是，直到合并到最底层才会清理墓碑。在中间层时墓碑标记有必要保留，否则如果是最新已被删除而更底层有该key的非空value，那么用户可能读到该旧值。

- Given that compaction takes a lot of write bandwidth and read bandwidth and may interfere with foreground operations, it is a good idea to postpone compaction when there are large write flow. It is even beneficial to stop/pause existing compaction tasks in this situation. What do you think of this idea? (Read the [SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores](https://www.usenix.org/conference/atc19/presentation/balmau) paper!)

    可能增加读放大，但SILK论文提出动态调整以减少延迟峰值。

- Is it a good idea to use/fill the block cache for compactions? Or is it better to fully bypass the block cache when compaction?

    可能会较大影响压缩时的读性能，尤其是读最新数据的性能。可以考虑绕过缓存，避免污染缓存（冷数据）。

- Does it make sense to have a `struct ConcatIterator<I: StorageIterator>` in the system?

    合并相邻SST，减少迭代器数量，提升顺序读效率。

- Some researchers/engineers propose to offload compaction to a remote server or a serverless lambda function. What are the benefits, and what might be the potential challenges and performance impacts of doing remote compaction? (Think of the point when a compaction completes and what happens to the block cache on the next read request...)

    网络延迟，块缓存同步困难，需跨节点数据传输。

#### W2D2: Simple compaction strategy

- What is the estimated write amplification of leveled compaction?

    约10-20倍（与层级乘数相关）。

- What is the estimated read amplification of leveled compaction?

    每层需查一个SST，O(L)复杂度（L为层数）。

- Is it correct that a key will only be purged from the LSM tree if the user requests to delete it and it has been compacted in the bottom-most level?

    需合并到最底层且墓碑未被保留策略清除。

- Is it a good strategy to periodically do a full compaction on the LSM tree? Why or why not?

    不推荐，写放大极高，应增量合并。

- Actively choosing some old files/levels to compact even if they do not violate the level amplifier would be a good choice, is it true? (Look at the [Lethe](https://disc-projects.bu.edu/lethe/) paper!)

    可能减少长尾延迟（如Lethe的惰性删除优化）。

- If the storage device can achieve a sustainable 1GB/s write throughput and the write amplification of the LSM tree is 10x, how much throughput can the user get from the LSM key-value interfaces?

    用户吞吐约100MB/s（1GB/s / 10）。

- Can you merge L1 and L3 directly if there are SST files in L2? Does it still produce correct result?

    不正确，需按层级顺序合并保证数据连续性。否则可能出现版本错乱。

- So far, we have assumed that our SST files use a monotonically increasing id as the file name. Is it okay to use `<level>_<begin_key>_<end_key>.sst` as the SST file name? What might be the potential problems with that? (You can ask yourself the same question in week 3...)

    在tiered compaction中，SST生成后并不一定在固定的层级。其他合并方法或许可以将level作为文件标识。（Not sure）

- What is your favorite boba shop in your city? (If you answered yes in week 1 day 3...)

    ？？？

#### W2D3: Tiered compaction strategy

- What is the estimated write amplification of leveled compaction? (Okay this is hard to estimate... But what if without the last *reduce sorted run* trigger?)

    较低（约4-5x），因合并次数少。

- What is the estimated read amplification of leveled compaction?

    较高（每层需查多个SST）。

- What are the pros/cons of universal compaction compared with simple leveled/tiered compaction?

    写放大低，但读/空间放大高（保留多版本）。

- How much storage space is it required (compared with user data size) to run universal compaction?

    约2x用户数据（需保留多版本SST）。

- Can we merge two tiers that are not adjacent in the LSM state?

    可能破坏有序性，需保证合并后数据连续。

- What happens if compaction speed cannot keep up with the SST flushes for tiered compaction?

    SST堆积，读性能下降，需动态调整合并策略。

- What might needs to be considered if the system schedules multiple compaction tasks in parallel?

    资源竞争（I/O、CPU），需调度优先级和限制并发数。

- SSDs also write its own logs (basically it is a log-structured storage). If the SSD has a write amplification of 2x, what is the end-to-end write amplification of the whole system? Related: [ZNS: Avoiding the Block Interface Tax for Flash-based SSDs](https://www.usenix.org/conference/atc21/presentation/bjorling).

    总写放大=LSM写放大 * SSD写放大（如10x*2x=20x）。

- Consider the case that the user chooses to keep a large number of sorted runs (i.e., 300) for tiered compaction. To make the read path faster, is it a good idea to keep some data structure that helps reduce the time complexity (i.e., to `O(log n)`) of finding SSTs to read in each layer for some key ranges? Note that normally, you will need to do a binary search in each sorted run to find the key ranges that you will need to read. (Check out Neon's [layer map](https://neon.tech/blog/persistent-structures-in-neons-wal-indexing) implementation!)

    使用层级索引（如跳表或B树）加速范围定位。

#### W2D4: Leveled compaction strategy

- What is the estimated write amplification of leveled compaction?

    约10-20倍（与层级数相关）。

- What is the estimated read amplification of leveled compaction?

    每层查一个SST，O(L)复杂度。

- Finding a good key split point for compaction may potentially reduce the write amplification, or it does not matter at all? (Consider that case that the user write keys beginning with some prefixes, `00` and `01`. The number of keys under these two prefixes are different and their write patterns are different. If we can always split `00` and `01` into different SSTs...)

    可能减少重叠，降低合并数据量，但实现复杂。

- Imagine that a user was using tiered (universal) compaction before and wants to migrate to leveled compaction. What might be the challenges of this migration? And how to do the migration?

    数据需重新分层，合并策略切换可能引发大规模写操作。

- And if we do it reversely, what if the user wants to migrate from leveled compaction to tiered compaction?

    需合并小SST为更大tier，可能空间放大临时增加。

- What happens if compaction speed cannot keep up with the SST flushes for leveled compaction?

    L0堆积，读延迟增加，需限速或优先级调度。

    如果超过了限制的imm memtables和L0大小，那么可能导致写堵塞。

- What might needs to be considered if the system schedules multiple compaction tasks in parallel?

    避免重叠键范围合并，协调锁机制。

- What is the peak storage usage for leveled compaction? Compared with universal compaction?

    低于Tiered（更积极合并），但写放大更高。

- Is it true that with a lower `level_size_multiplier`, you can always get a lower write amplification?

    不一定，过小导致频繁合并，可能增加写放大。

- What needs to be done if a user not using compaction at all decides to migrate to leveled compaction?、

    需全量合并现有SST，资源消耗大。

- Some people propose to do intra-L0 compaction (compact L0 tables and still put them in L0) before pushing them to lower layers. What might be the benefits of doing so? (Might be related: [PebblesDB SOSP'17](https://www.cs.utexas.edu/~vijay/papers/sosp17-pebblesdb.pdf))

    减少L0文件数，降低读放大（如PebblesDB的Guards优化）。

- Consider the case that the upper level has two tables of `[100, 200], [201, 300]` and the lower level has `[50, 150], [151, 250], [251, 350]`. In this case, do you still want to compact one file in the upper level at a time? Why?

    需合并多个文件，避免范围碎片，减少读放大。

#### W2D5: Manifest

- When do you need to call `fsync`? Why do you need to fsync the directory?

    状态变更后（如添加SST），同时fsync目录确保元数据持久化。

- What are the places you will need to write to the manifest?

    ManifestRecord有三种类型，Flush，NewMemtable，Compaction，分别对应memtable的flush、新建，和compaction的完成。

- Consider an alternative implementation of an LSM engine that does not use a manifest file. Instead, it records the level/tier information in the header of each file, scans the storage directory every time it restarts, and recover the LSM state solely from the files present in the directory. Is it possible to correctly maintain the LSM state in this implementation and what might be the problems/challenges with that?

    可能，但需扫描所有SST并推断层级，效率低且易出错（如文件丢失）。可能会导致启动慢，好处是如果能实现那么可以降低运行时磁盘写入负载。

- Currently, we create all SST/concat iterators before creating the merge iterator, which means that we have to load the first block of the first SST in all levels into memory before starting the scanning process. We have start/end key in the manifest, and is it possible to leverage this information to delay the loading of the data blocks and make the time to return the first key-value pair faster?

    利用SST元数据延迟加载，快速返回首个键（如按需加载块）。

- Is it possible not to store the tier/level information in the manifest? i.e., we only store the list of SSTs we have in the manifest without the level information, and rebuild the tier/level using the key range and timestamp information (SST metadata).

    必需，否则无法重建LSM状态（如SST属于哪个层级）。（Not sure）

#### W2D6: WAL

- When should you call `fsync` in your engine? What happens if you call `fsync` too often (i.e., on every put key request)?

    每次批量写入后，频繁fsync降低吞吐（SSD约100μs/次）。

- How costly is the `fsync` operation in general on an SSD (solid state drive)?

    较高（约毫秒级），但较HDD快。不能每一次memtable操作都马上调用否则极大影响写入速率。

- When can you tell the user that their modifications (put/delete) have been persisted?

    WAL写入并fsync后，确保崩溃恢复不丢失数据。

- How can you handle corrupted data in WAL?

    校验checksum，跳过损坏条目或从头重建。

- Is it possible to design an LSM engine without WAL (i.e., use L0 as WAL)? What will be the implications of this design?

    不可行，内存数据未持久化前崩溃会丢失（L0未刷盘）。

#### W2D7: Batch write and checksums

- Consider the case that an LSM storage engine only provides `write_batch` as the write interface (instead of single put + delete). Is it possible to implement it as follows: there is a single write thread with an mpsc channel receiver to get the changes, and all threads send write batches to the write thread. The write thread is the single point to write to the database. What are the pros/cons of this implementation? (Congrats if you do so you get BadgerDB!)

    优点：简化并发控制；缺点：吞吐受限（单线程瓶颈）。

- Is it okay to put all block checksums altogether at the end of the SST file instead of store it along with the block? Why?

    不可，需按块校验，因为读取SST时是按块读取的，而非整体读取。







































