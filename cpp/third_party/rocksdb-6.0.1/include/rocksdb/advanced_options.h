// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>

#include "rocksdb/memtablerep.h"
#include "rocksdb/universal_compaction.h"

namespace rocksdb {

class Slice;
class SliceTransform;
enum CompressionType : unsigned char;
class TablePropertiesCollectorFactory;
class TableFactory;
struct Options;

enum CompactionStyle : char {
  // level based compaction style
  kCompactionStyleLevel = 0x0,
  // Universal compaction style
  // Not supported in ROCKSDB_LITE.
  kCompactionStyleUniversal = 0x1,
  // FIFO compaction style
  // Not supported in ROCKSDB_LITE
  kCompactionStyleFIFO = 0x2,
  // Disable background compaction. Compaction jobs are submitted
  // via CompactFiles().
  // Not supported in ROCKSDB_LITE
  kCompactionStyleNone = 0x3,
};

// In Level-based compaction, it Determines which file from a level to be
// picked to merge to the next level. We suggest people try
// kMinOverlappingRatio first when you tune your database.
enum CompactionPri : char {
  // Slightly prioritize larger files by size compensated by #deletes
  kByCompensatedSize = 0x0,
  // First compact files whose data's latest update time is oldest.
  // Try this if you only update some hot keys in small ranges.
  kOldestLargestSeqFirst = 0x1,
  // First compact files whose range hasn't been compacted to the next level
  // for the longest. If your updates are random across the key space,
  // write amplification is slightly better with this option.
  kOldestSmallestSeqFirst = 0x2,
  // First compact files whose ratio between overlapping size in next level
  // and its size is the smallest. It in many cases can optimize write
  // amplification.
  kMinOverlappingRatio = 0x3,
};

struct CompactionOptionsFIFO {
  // once the total sum of table files reaches this, we will delete the oldest
  // table file
  // Default: 1GB
  uint64_t max_table_files_size;

  // If true, try to do compaction to compact smaller files into larger ones.
  // Minimum files to compact follows options.level0_file_num_compaction_trigger
  // and compaction won't trigger if average compact bytes per del file is
  // larger than options.write_buffer_size. This is to protect large files
  // from being compacted again.
  // Default: false;
  bool allow_compaction = false;

  CompactionOptionsFIFO() : max_table_files_size(1 * 1024 * 1024 * 1024) {}
  CompactionOptionsFIFO(uint64_t _max_table_files_size, bool _allow_compaction)
      : max_table_files_size(_max_table_files_size),
        allow_compaction(_allow_compaction) {}
};

// Compression options for different compression algorithms like Zlib
struct CompressionOptions {
  // RocksDB's generic default compression level. Internally it'll be translated
  // to the default compression level specific to the library being used (see
  // comment above `ColumnFamilyOptions::compression`).
  //
  // The default value is the max 16-bit int as it'll be written out in OPTIONS
  // file, which should be portable.
  const static int kDefaultCompressionLevel = 32767;

  int window_bits;
  int level;
  int strategy;

  // Maximum size of dictionaries used to prime the compression library.
  // Enabling dictionary can improve compression ratios when there are
  // repetitions across data blocks.
  //
  // The dictionary is created by sampling the SST file data. If
  // `zstd_max_train_bytes` is nonzero, the samples are passed through zstd's
  // dictionary generator. Otherwise, the random samples are used directly as
  // the dictionary.
  //
  // When compression dictionary is disabled, we compress and write each block
  // before buffering data for the next one. When compression dictionary is
  // enabled, we buffer all SST file data in-memory so we can sample it, as data
  // can only be compressed and written after the dictionary has been finalized.
  // So users of this feature may see increased memory usage.
  //
  // Default: 0.
  uint32_t max_dict_bytes;

  // Maximum size of training data passed to zstd's dictionary trainer. Using
  // zstd's dictionary trainer can achieve even better compression ratio
  // improvements than using `max_dict_bytes` alone.
  //
  // The training data will be used to generate a dictionary of max_dict_bytes.
  //
  // Default: 0.
  uint32_t zstd_max_train_bytes;

  // When the compression options are set by the user, it will be set to "true".
  // For bottommost_compression_opts, to enable it, user must set enabled=true.
  // Otherwise, bottommost compression will use compression_opts as default
  // compression options.
  //
  // For compression_opts, if compression_opts.enabled=false, it is still
  // used as compression options for compression process.
  //
  // Default: false.
  bool enabled;

  CompressionOptions()
      : window_bits(-14),
        level(kDefaultCompressionLevel),
        strategy(0),
        max_dict_bytes(0),
        zstd_max_train_bytes(0),
        enabled(false) {}
  CompressionOptions(int wbits, int _lev, int _strategy, int _max_dict_bytes,
                     int _zstd_max_train_bytes, bool _enabled)
      : window_bits(wbits),
        level(_lev),
        strategy(_strategy),
        max_dict_bytes(_max_dict_bytes),
        zstd_max_train_bytes(_zstd_max_train_bytes),
        enabled(_enabled) {}
};

enum UpdateStatus {    // Return status For inplace update callback
  UPDATE_FAILED   = 0, // Nothing to update
  UPDATED_INPLACE = 1, // Value updated inplace
  UPDATED         = 2, // No inplace update. Merged value set
};


struct AdvancedColumnFamilyOptions {
  // The maximum number of write buffers that are built up in memory.
  // The default and the minimum number is 2, so that when 1 write buffer
  // is being flushed to storage, new writes can continue to the other
  // write buffer.
  // If max_write_buffer_number > 3, writing will be slowed down to
  // options.delayed_write_rate if we are writing to the last write buffer
  // allowed.
  //
  // Default: 2
  //
  // Dynamically changeable through SetOptions() API
  int max_write_buffer_number = 2;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.  If set to 1, then
  // all write buffers are flushed to L0 as individual files and this increases
  // read amplification because a get request has to check in all of these
  // files. Also, an in-memory merge may result in writing lesser
  // data to storage if there are duplicate records in each of these
  // individual write buffers.  Default: 1
  int min_write_buffer_number_to_merge = 1;

  // The total maximum number of write buffers to maintain in memory including
  // copies of buffers that have already been flushed.  Unlike
  // max_write_buffer_number, this parameter does not affect flushing.
  // This controls the minimum amount of write history that will be available
  // in memory for conflict checking when Transactions are used.
  //
  // When using an OptimisticTransactionDB:
  // If this value is too low, some transactions may fail at commit time due
  // to not being able to determine whether there were any write conflicts.
  //
  // When using a TransactionDB:
  // If Transaction::SetSnapshot is used, TransactionDB will read either
  // in-memory write buffers or SST files to do write-conflict checking.
  // Increasing this value can reduce the number of reads to SST files
  // done for conflict detection.
  //
  // Setting this value to 0 will cause write buffers to be freed immediately
  // after they are flushed.
  // If this value is set to -1, 'max_write_buffer_number' will be used.
  //
  // Default:
  // If using a TransactionDB/OptimisticTransactionDB, the default value will
  // be set to the value of 'max_write_buffer_number' if it is not explicitly
  // set by the user.  Otherwise, the default is 0.
  int max_write_buffer_number_to_maintain = 0;

  // Allows thread-safe inplace updates. If this is true, there is no way to
  // achieve point-in-time consistency using snapshot or iterator (assuming
  // concurrent updates). Hence iterator and multi-get will return results
  // which are not consistent as of any point-in-time.
  // If inplace_callback function is not set,
  //   Put(key, new_value) will update inplace the existing_value iff
  //   * key exists in current memtable
  //   * new sizeof(new_value) <= sizeof(existing_value)
  //   * existing_value for that key is a put i.e. kTypeValue
  // If inplace_callback function is set, check doc for inplace_callback.
  // Default: false.
  bool inplace_update_support = false;

  // Number of locks used for inplace update
  // Default: 10000, if inplace_update_support = true, else 0.
  //
  // Dynamically changeable through SetOptions() API
  size_t inplace_update_num_locks = 10000;

  // existing_value - pointer to previous value (from both memtable and sst).
  //                  nullptr if key doesn't exist
  // existing_value_size - pointer to size of existing_value).
  //                       nullptr if key doesn't exist
  // delta_value - Delta value to be merged with the existing_value.
  //               Stored in transaction logs.
  // merged_value - Set when delta is applied on the previous value.

  // Applicable only when inplace_update_support is true,
  // this callback function is called at the time of updating the memtable
  // as part of a Put operation, lets say Put(key, delta_value). It allows the
  // 'delta_value' specified as part of the Put operation to be merged with
  // an 'existing_value' of the key in the database.

  // If the merged value is smaller in size that the 'existing_value',
  // then this function can update the 'existing_value' buffer inplace and
  // the corresponding 'existing_value'_size pointer, if it wishes to.
  // The callback should return UpdateStatus::UPDATED_INPLACE.
  // In this case. (In this case, the snapshot-semantics of the rocksdb
  // Iterator is not atomic anymore).

  // If the merged value is larger in size than the 'existing_value' or the
  // application does not wish to modify the 'existing_value' buffer inplace,
  // then the merged value should be returned via *merge_value. It is set by
  // merging the 'existing_value' and the Put 'delta_value'. The callback should
  // return UpdateStatus::UPDATED in this case. This merged value will be added
  // to the memtable.

  // If merging fails or the application does not wish to take any action,
  // then the callback should return UpdateStatus::UPDATE_FAILED.

  // Please remember that the original call from the application is Put(key,
  // delta_value). So the transaction log (if enabled) will still contain (key,
  // delta_value). The 'merged_value' is not stored in the transaction log.
  // Hence the inplace_callback function should be consistent across db reopens.

  // Default: nullptr
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value) = nullptr;

  // if prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
  // create prefix bloom for memtable with the size of
  // write_buffer_size * memtable_prefix_bloom_size_ratio.
  // If it is larger than 0.25, it is sanitized to 0.25.
  //
  // Default: 0 (disable)
  //
  // Dynamically changeable through SetOptions() API
  double memtable_prefix_bloom_size_ratio = 0.0;

  // Enable whole key bloom filter in memtable. Note this will only take effect
  // if memtable_prefix_bloom_size_ratio is not 0. Enabling whole key filtering
  // can potentially reduce CPU usage for point-look-ups.
  //
  // Default: false (disable)
  //
  // Dynamically changeable through SetOptions() API
  bool memtable_whole_key_filtering = false;

  // Page size for huge page for the arena used by the memtable. If <=0, it
  // won't allocate from huge page but from malloc.
  // Users are responsible to reserve huge pages for it to be allocated. For
  // example:
  //      sysctl -w vm.nr_hugepages=20
  // See linux doc Documentation/vm/hugetlbpage.txt
  // If there isn't enough free huge page available, it will fall back to
  // malloc.
  //
  // Dynamically changeable through SetOptions() API
  size_t memtable_huge_page_size = 0;

  // If non-nullptr, memtable will use the specified function to extract
  // prefixes for keys, and for each prefix maintain a hint of insert location
  // to reduce CPU usage for inserting keys with the prefix. Keys out of
  // domain of the prefix extractor will be insert without using hints.
  //
  // Currently only the default skiplist based memtable implements the feature.
  // All other memtable implementation will ignore the option. It incurs ~250
  // additional bytes of memory overhead to store a hint for each prefix.
  // Also concurrent writes (when allow_concurrent_memtable_write is true) will
  // ignore the option.
  //
  // The option is best suited for workloads where keys will likely to insert
  // to a location close the last inserted key with the same prefix.
  // One example could be inserting keys of the form (prefix + timestamp),
  // and keys of the same prefix always comes in with time order. Another
  // example would be updating the same key over and over again, in which case
  // the prefix can be the key itself.
  //
  // Default: nullptr (disable)
  std::shared_ptr<const SliceTransform>
      memtable_insert_with_hint_prefix_extractor = nullptr;

  // Control locality of bloom filter probes to improve cache miss rate.
  // This option only applies to memtable prefix bloom and plaintable
  // prefix bloom. It essentially limits every bloom checking to one cache line.
  // This optimization is turned off when set to 0, and positive number to turn
  // it on.
  // Default: 0
  uint32_t bloom_locality = 0;

  // size of one block in arena memory allocation.
  // If <= 0, a proper value is automatically calculated (usually 1/8 of
  // writer_buffer_size, rounded up to a multiple of 4KB).
  //
  // There are two additional restriction of the specified size:
  // (1) size should be in the range of [4096, 2 << 30] and
  // (2) be the multiple of the CPU word (which helps with the memory
  // alignment).
  //
  // We'll automatically check and adjust the size number to make sure it
  // conforms to the restrictions.
  //
  // Default: 0
  //
  // Dynamically changeable through SetOptions() API
  size_t arena_block_size = 0;

  // Different levels can have different compression policies. There
  // are cases where most lower levels would like to use quick compression
  // algorithms while the higher levels (which have more data) use
  // compression algorithms that have better compression but could
  // be slower. This array, if non-empty, should have an entry for
  // each level of the database; these override the value specified in
  // the previous field 'compression'.
  //
  // NOTICE if level_compaction_dynamic_level_bytes=true,
  // compression_per_level[0] still determines L0, but other elements
  // of the array are based on base level (the level L0 files are merged
  // to), and may not match the level users see from info log for metadata.
  // If L0 files are merged to level-n, then, for i>0, compression_per_level[i]
  // determines compaction type for level n+i-1.
  // For example, if we have three 5 levels, and we determine to merge L0
  // data to L4 (which means L1..L3 will be empty), then the new files go to
  // L4 uses compression type compression_per_level[1].
  // If now L0 is merged to L2. Data goes to L2 will be compressed
  // according to compression_per_level[1], L3 using compression_per_level[2]
  // and L4 using compression_per_level[3]. Compaction for each level can
  // change when data grows.
  std::vector<CompressionType> compression_per_level;

  // Number of levels for this database
  int num_levels = 7;

  // Soft limit on number of level-0 files. We start slowing down writes at this
  // point. A value <0 means that no writing slow down will be triggered by
  // number of files in level-0.
  //
  // Default: 20
  //
  // Dynamically changeable through SetOptions() API
  int level0_slowdown_writes_trigger = 20;

  // Maximum number of level-0 files.  We stop writes at this point.
  //
  // Default: 36
  //
  // Dynamically changeable through SetOptions() API
  int level0_stop_writes_trigger = 36;

  // Target file size for compaction.
  // target_file_size_base is per-file size for level-1.
  // Target file size for level L can be calculated by
  // target_file_size_base * (target_file_size_multiplier ^ (L-1))
  // For example, if target_file_size_base is 2MB and
  // target_file_size_multiplier is 10, then each file on level-1 will
  // be 2MB, and each file on level 2 will be 20MB,
  // and each file on level-3 will be 200MB.
  //
  // Default: 64MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t target_file_size_base = 64 * 1048576;

  // By default target_file_size_multiplier is 1, which means
  // by default files in different levels will have similar size.
  //
  // Dynamically changeable through SetOptions() API
  int target_file_size_multiplier = 1;

  // If true, RocksDB will pick target size of each level dynamically.
  // We will pick a base level b >= 1. L0 will be directly merged into level b,
  // instead of always into level 1. Level 1 to b-1 need to be empty.
  // We try to pick b and its target size so that
  // 1. target size is in the range of
  //   (max_bytes_for_level_base / max_bytes_for_level_multiplier,
  //    max_bytes_for_level_base]
  // 2. target size of the last level (level num_levels-1) equals to extra size
  //    of the level.
  // At the same time max_bytes_for_level_multiplier and
  // max_bytes_for_level_multiplier_additional are still satisfied.
  // (When L0 is too large, we make some adjustment. See below.)
  //
  // With this option on, from an empty DB, we make last level the base level,
  // which means merging L0 data into the last level, until it exceeds
  // max_bytes_for_level_base. And then we make the second last level to be
  // base level, to start to merge L0 data to second last level, with its
  // target size to be 1/max_bytes_for_level_multiplier of the last level's
  // extra size. After the data accumulates more so that we need to move the
  // base level to the third last one, and so on.
  //
  // For example, assume max_bytes_for_level_multiplier=10, num_levels=6,
  // and max_bytes_for_level_base=10MB.
  // Target sizes of level 1 to 5 starts with:
  // [- - - - 10MB]
  // with base level is level. Target sizes of level 1 to 4 are not applicable
  // because they will not be used.
  // Until the size of Level 5 grows to more than 10MB, say 11MB, we make
  // base target to level 4 and now the targets looks like:
  // [- - - 1.1MB 11MB]
  // While data are accumulated, size targets are tuned based on actual data
  // of level 5. When level 5 has 50MB of data, the target is like:
  // [- - - 5MB 50MB]
  // Until level 5's actual size is more than 100MB, say 101MB. Now if we keep
  // level 4 to be the base level, its target size needs to be 10.1MB, which
  // doesn't satisfy the target size range. So now we make level 3 the target
  // size and the target sizes of the levels look like:
  // [- - 1.01MB 10.1MB 101MB]
  // In the same way, while level 5 further grows, all levels' targets grow,
  // like
  // [- - 5MB 50MB 500MB]
  // Until level 5 exceeds 1000MB and becomes 1001MB, we make level 2 the
  // base level and make levels' target sizes like this:
  // [- 1.001MB 10.01MB 100.1MB 1001MB]
  // and go on...
  //
  // By doing it, we give max_bytes_for_level_multiplier a priority against
  // max_bytes_for_level_base, for a more predictable LSM tree shape. It is
  // useful to limit worse case space amplification.
  //
  //
  // If the compaction from L0 is lagged behind, a special mode will be turned
  // on to prioritize write amplification against max_bytes_for_level_multiplier
  // or max_bytes_for_level_base. The L0 compaction is lagged behind by looking
  // at number of L0 files and total L0 size. If number of L0 files is at least
  // the double of level0_file_num_compaction_trigger, or the total size is
  // at least max_bytes_for_level_base, this mode is on. The target of L1 grows
  // to the actual data size in L0, and then determine the target for each level
  // so that each level will have the same level multiplier.
  //
  // For example, when L0 size is 100MB, the size of last level is 1600MB,
  // max_bytes_for_level_base = 80MB, and max_bytes_for_level_multiplier = 10.
  // Since L0 size is larger than max_bytes_for_level_base, this is a L0
  // compaction backlogged mode. So that the L1 size is determined to be 100MB.
  // Based on max_bytes_for_level_multiplier = 10, at least 3 non-0 levels will
  // be needed. The level multiplier will be calculated to be 4 and the three
  // levels' target to be [100MB, 400MB, 1600MB].
  //
  // In this mode, The number of levels will be no more than the normal mode,
  // and the level multiplier will be lower. The write amplification will
  // likely to be reduced.
  //
  //
  // max_bytes_for_level_multiplier_additional is ignored with this flag on.
  //
  // Turning this feature on or off for an existing DB can cause unexpected
  // LSM tree structure so it's not recommended.
  //
  // Default: false
  bool level_compaction_dynamic_level_bytes = false;

  // Default: 10.
  //
  // Dynamically changeable through SetOptions() API
  double max_bytes_for_level_multiplier = 10;

  // Different max-size multipliers for different levels.
  // These are multiplied by max_bytes_for_level_multiplier to arrive
  // at the max-size of each level.
  //
  // Default: 1
  //
  // Dynamically changeable through SetOptions() API
  std::vector<int> max_bytes_for_level_multiplier_additional =
      std::vector<int>(num_levels, 1);

  // We try to limit number of bytes in one compaction to be lower than this
  // threshold. But it's not guaranteed.
  // Value 0 will be sanitized.
  //
  // Default: target_file_size_base * 25
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_compaction_bytes = 0;

  // All writes will be slowed down to at least delayed_write_rate if estimated
  // bytes needed to be compaction exceed this threshold.
  //
  // Default: 64GB
  //
  // Dynamically changeable through SetOptions() API
  uint64_t soft_pending_compaction_bytes_limit = 64 * 1073741824ull;

  // All writes are stopped if estimated bytes needed to be compaction exceed
  // this threshold.
  //
  // Default: 256GB
  //
  // Dynamically changeable through SetOptions() API
  uint64_t hard_pending_compaction_bytes_limit = 256 * 1073741824ull;

  // The compaction style. Default: kCompactionStyleLevel
  CompactionStyle compaction_style = kCompactionStyleLevel;

  // If level compaction_style = kCompactionStyleLevel, for each level,
  // which files are prioritized to be picked to compact.
  // Default: kMinOverlappingRatio
  CompactionPri compaction_pri = kMinOverlappingRatio;

  // The options needed to support Universal Style compactions
  //
  // Dynamically changeable through SetOptions() API
  // Dynamic change example:
  // SetOptions("compaction_options_universal", "{size_ratio=2;}")
  CompactionOptionsUniversal compaction_options_universal;

  // The options for FIFO compaction style
  //
  // Dynamically changeable through SetOptions() API
  // Dynamic change example:
  // SetOptions("compaction_options_fifo", "{max_table_files_size=100;}")
  CompactionOptionsFIFO compaction_options_fifo;

  // An iteration->Next() sequentially skips over keys with the same
  // user-key unless this option is set. This number specifies the number
  // of keys (with the same userkey) that will be sequentially
  // skipped before a reseek is issued.
  //
  // Default: 8
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_sequential_skip_in_iterations = 8;

  // This is a factory that provides MemTableRep objects.
  // Default: a factory that provides a skip-list-based implementation of
  // MemTableRep.
  std::shared_ptr<MemTableRepFactory> memtable_factory =
      std::shared_ptr<SkipListFactory>(new SkipListFactory);

  // Block-based table related options are moved to BlockBasedTableOptions.
  // Related options that were originally here but now moved include:
  //   no_block_cache
  //   block_cache
  //   block_cache_compressed
  //   block_size
  //   block_size_deviation
  //   block_restart_interval
  //   filter_policy
  //   whole_key_filtering
  // If you'd like to customize some of these options, you will need to
  // use NewBlockBasedTableFactory() to construct a new table factory.

  // This option allows user to collect their own interested statistics of
  // the tables.
  // Default: empty vector -- no user-defined statistics collection will be
  // performed.
  typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      TablePropertiesCollectorFactories;
  TablePropertiesCollectorFactories table_properties_collector_factories;

  // Maximum number of successive merge operations on a key in the memtable.
  //
  // When a merge operation is added to the memtable and the maximum number of
  // successive merges is reached, the value of the key will be calculated and
  // inserted into the memtable instead of the merge operation. This will
  // ensure that there are never more than max_successive_merges merge
  // operations in the memtable.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  size_t max_successive_merges = 0;

  // This flag specifies that the implementation should optimize the filters
  // mainly for cases where keys are found rather than also optimize for keys
  // missed. This would be used in cases where the application knows that
  // there are very few misses or the performance in the case of misses is not
  // important.
  //
  // For now, this flag allows us to not store filters for the last level i.e
  // the largest level which contains data of the LSM store. For keys which
  // are hits, the filters in this level are not useful because we will search
  // for the data anyway. NOTE: the filters in other levels are still useful
  // even for key hit because they tell us whether to look in that level or go
  // to the higher level.
  //
  // Default: false
  bool optimize_filters_for_hits = false;

  // After writing every SST file, reopen it and read all the keys.
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool paranoid_file_checks = false;

  // In debug mode, RocksDB run consistency checks on the LSM every time the LSM
  // change (Flush, Compaction, AddFile). These checks are disabled in release
  // mode, use this option to enable them in release mode as well.
  // Default: false
  bool force_consistency_checks = false;

  // Measure IO stats in compactions and flushes, if true.
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool report_bg_io_stats = false;

  // Files older than TTL will go through the compaction process.
  // Supported in Level and FIFO compaction.
  // Pre-req: This needs max_open_files to be set to -1.
  // In Level: Non-bottom-level files older than TTL will go through the
  //           compation process.
  // In FIFO: Files older than TTL will be deleted.
  // unit: seconds. Ex: 1 day = 1 * 24 * 60 * 60
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  uint64_t ttl = 0;

  // Create ColumnFamilyOptions with default values for all fields
  AdvancedColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit AdvancedColumnFamilyOptions(const Options& options);

  // ---------------- OPTIONS NOT SUPPORTED ANYMORE ----------------

  // NOT SUPPORTED ANYMORE
  // This does not do anything anymore.
  int max_mem_compaction_level;

  // NOT SUPPORTED ANYMORE -- this options is no longer used
  // Puts are delayed to options.delayed_write_rate when any level has a
  // compaction score that exceeds soft_rate_limit. This is ignored when == 0.0.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  double soft_rate_limit = 0.0;

  // NOT SUPPORTED ANYMORE -- this options is no longer used
  double hard_rate_limit = 0.0;

  // NOT SUPPORTED ANYMORE -- this options is no longer used
  unsigned int rate_limit_delay_max_milliseconds = 100;

  // NOT SUPPORTED ANYMORE
  // Does not have any effect.
  bool purge_redundant_kvs_while_flush = true;
};

}  // namespace rocksdb
