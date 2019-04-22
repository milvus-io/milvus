// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <memory>
#include <vector>
#include <limits>
#include <unordered_map>

#include "rocksdb/advanced_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"

#ifdef max
#undef max
#endif

namespace rocksdb {

class Cache;
class CompactionFilter;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class Env;
enum InfoLogLevel : unsigned char;
class SstFileManager;
class FilterPolicy;
class Logger;
class MergeOperator;
class Snapshot;
class MemTableRepFactory;
class RateLimiter;
class Slice;
class Statistics;
class InternalKeyComparator;
class WalFilter;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
enum CompressionType : unsigned char {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kZlibCompression = 0x2,
  kBZip2Compression = 0x3,
  kLZ4Compression = 0x4,
  kLZ4HCCompression = 0x5,
  kXpressCompression = 0x6,
  kZSTD = 0x7,

  // Only use kZSTDNotFinalCompression if you have to use ZSTD lib older than
  // 0.8.0 or consider a possibility of downgrading the service or copying
  // the database files to another service running with an older version of
  // RocksDB that doesn't have kZSTD. Otherwise, you should use kZSTD. We will
  // eventually remove the option from the public API.
  kZSTDNotFinalCompression = 0x40,

  // kDisableCompressionOption is used to disable some compression options.
  kDisableCompressionOption = 0xff,
};

struct Options;
struct DbPath;

struct ColumnFamilyOptions : public AdvancedColumnFamilyOptions {
  // The function recovers options to a previous version. Only 4.6 or later
  // versions are supported.
  ColumnFamilyOptions* OldDefaults(int rocksdb_major_version = 4,
                                   int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB
  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  ColumnFamilyOptions* OptimizeForSmallDb();

  // Use this if you don't need to keep the data sorted, i.e. you'll never use
  // an iterator, only Put() and Get() API calls
  //
  // Not supported in ROCKSDB_LITE
  ColumnFamilyOptions* OptimizeForPointLookup(
      uint64_t block_cache_size_mb);

  // Default values for some parameters in ColumnFamilyOptions are not
  // optimized for heavy workloads and big datasets, which means you might
  // observe write stalls under some conditions. As a starting point for tuning
  // RocksDB options, use the following two functions:
  // * OptimizeLevelStyleCompaction -- optimizes level style compaction
  // * OptimizeUniversalStyleCompaction -- optimizes universal style compaction
  // Universal style compaction is focused on reducing Write Amplification
  // Factor for big data sets, but increases Space Amplification. You can learn
  // more about the different styles here:
  // https://github.com/facebook/rocksdb/wiki/Rocksdb-Architecture-Guide
  // Make sure to also call IncreaseParallelism(), which will provide the
  // biggest performance gains.
  // Note: we might use more memory than memtable_memory_budget during high
  // write rate period
  //
  // OptimizeUniversalStyleCompaction is not supported in ROCKSDB_LITE
  ColumnFamilyOptions* OptimizeLevelStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  ColumnFamilyOptions* OptimizeUniversalStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);

  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const Comparator* comparator = BytewiseComparator();

  // REQUIRES: The client must provide a merge operator if Merge operation
  // needs to be accessed. Calling Merge on a DB without a merge operator
  // would result in Status::NotSupported. The client must ensure that the
  // merge operator supplied here has the same name and *exactly* the same
  // semantics as the merge operator provided to previous open calls on
  // the same DB. The only exception is reserved for upgrade, where a DB
  // previously without a merge operator is introduced to Merge operation
  // for the first time. It's necessary to specify a merge operator when
  // opening the DB in this case.
  // Default: nullptr
  std::shared_ptr<MergeOperator> merge_operator = nullptr;

  // A single CompactionFilter instance to call into during compaction.
  // Allows an application to modify/delete a key-value during background
  // compaction.
  //
  // If the client requires a new compaction filter to be used for different
  // compaction runs, it can specify compaction_filter_factory instead of this
  // option.  The client should specify only one of the two.
  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both.
  //
  // If multithreaded compaction is being used, the supplied CompactionFilter
  // instance may be used from different threads concurrently and so should be
  // thread-safe.
  //
  // Default: nullptr
  const CompactionFilter* compaction_filter = nullptr;

  // This is a factory that provides compaction filter objects which allow
  // an application to modify/delete a key-value during background compaction.
  //
  // A new filter will be created on each compaction run.  If multithreaded
  // compaction is being used, each created CompactionFilter will only be used
  // from a single thread and so does not need to be thread-safe.
  //
  // Default: nullptr
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  //
  // Larger values increase performance, especially during bulk loads.
  // Up to max_write_buffer_number write buffers may be held in memory
  // at the same time,
  // so you may wish to adjust this parameter to control memory usage.
  // Also, a larger write buffer will result in a longer recovery time
  // the next time the database is opened.
  //
  // Note that write_buffer_size is enforced per column family.
  // See db_write_buffer_size for sharing memory across column families.
  //
  // Default: 64MB
  //
  // Dynamically changeable through SetOptions() API
  size_t write_buffer_size = 64 << 20;

  // Compress blocks using the specified compression algorithm.
  //
  // Default: kSnappyCompression, if it's supported. If snappy is not linked
  // with the library, the default is kNoCompression.
  //
  // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  //
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kSnappyCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  //
  // If you do not set `compression_opts.level`, or set it to
  // `CompressionOptions::kDefaultCompressionLevel`, we will attempt to pick the
  // default corresponding to `compression` as follows:
  //
  // - kZSTD: 3
  // - kZlibCompression: Z_DEFAULT_COMPRESSION (currently -1)
  // - kLZ4HCCompression: 0
  // - For all others, we do not specify a compression level
  //
  // Dynamically changeable through SetOptions() API
  CompressionType compression;

  // Compression algorithm that will be used for the bottommost level that
  // contain files.
  //
  // Default: kDisableCompressionOption (Disabled)
  CompressionType bottommost_compression = kDisableCompressionOption;

  // different options for compression algorithms used by bottommost_compression
  // if it is enabled. To enable it, please see the definition of
  // CompressionOptions.
  CompressionOptions bottommost_compression_opts;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Default: 4
  //
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger = 4;

  // If non-nullptr, use the specified function to determine the
  // prefixes for keys.  These prefixes will be placed in the filter.
  // Depending on the workload, this can reduce the number of read-IOP
  // cost for scans when a prefix is passed via ReadOptions to
  // db.NewIterator().  For prefix filtering to work properly,
  // "prefix_extractor" and "comparator" must be such that the following
  // properties hold:
  //
  // 1) key.starts_with(prefix(key))
  // 2) Compare(prefix(key), key) <= 0.
  // 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
  // 4) prefix(prefix(key)) == prefix(key)
  //
  // Default: nullptr
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 200MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 200MB, total file size for level-2 will be 2GB,
  // and total file size for level-3 will be 20GB.
  //
  // Default: 256MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_bytes_for_level_base = 256 * 1048576;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  //
  // Dynamically changeable through SetOptions() API
  bool disable_auto_compactions = false;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  std::shared_ptr<TableFactory> table_factory;

  // A list of paths where SST files for this column family
  // can be put into, with its target size. Similar to db_paths,
  // newer data is placed into paths specified earlier in the
  // vector while older data gradually moves to paths specified
  // later in the vector.
  // Note that, if a path is supplied to multiple column
  // families, it would have files and total size from all
  // the column families combined. User should provision for the
  // total size(from all the column families) in such cases.
  //
  // If left empty, db_paths will be used.
  // Default: empty
  std::vector<DbPath> cf_paths;

  // Compaction concurrent thread limiter for the column family.
  // If non-nullptr, use given concurrent thread limiter to control
  // the max outstanding compaction tasks. Limiter can be shared with
  // multiple column families across db instances.
  //
  // Default: nullptr
  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter = nullptr;

  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump(Logger* log) const;
};

enum class WALRecoveryMode : char {
  // Original levelDB recovery
  // We tolerate incomplete record in trailing data on all logs
  // Use case : This is legacy behavior
  kTolerateCorruptedTailRecords = 0x00,
  // Recover from clean shutdown
  // We don't expect to find any corruption in the WAL
  // Use case : This is ideal for unit tests and rare applications that
  // can require high consistency guarantee
  kAbsoluteConsistency = 0x01,
  // Recover to point-in-time consistency (default)
  // We stop the WAL playback on discovering WAL inconsistency
  // Use case : Ideal for systems that have disk controller cache like
  // hard disk, SSD without super capacitor that store related data
  kPointInTimeRecovery = 0x02,
  // Recovery after a disaster
  // We ignore any corruption in the WAL and try to salvage as much data as
  // possible
  // Use case : Ideal for last ditch effort to recover data or systems that
  // operate with low grade unrelated data
  kSkipAnyCorruptedRecords = 0x03,
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};


struct DBOptions {
  // The function recovers options to the option as in version 4.6.
  DBOptions* OldDefaults(int rocksdb_major_version = 4,
                         int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  DBOptions* OptimizeForSmallDb();

#ifndef ROCKSDB_LITE
  // By default, RocksDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by RocksDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);
#endif  // ROCKSDB_LITE

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing = false;

  // If true, missing column families will be automatically created.
  // Default: false
  bool create_missing_column_families = false;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists = false;

  // If true, RocksDB will aggressively check consistency of the data.
  // Also, if any of the  writes to the database fails (Put, Delete, Merge,
  // Write), the database will switch to read-only mode and fail all other
  // Write operations.
  // In most cases you want this to be set to true.
  // Default: true
  bool paranoid_checks = true;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env = Env::Default();

  // Use to control write rate of flush and compaction. Flush has higher
  // priority than compaction. Rate limiting is disabled if nullptr.
  // If rate limiter is enabled, bytes_per_sync is set to 1MB by default.
  // Default: nullptr
  std::shared_ptr<RateLimiter> rate_limiter = nullptr;

  // Use to track SST files and control their file deletion rate.
  //
  // Features:
  //  - Throttle the deletion rate of the SST files.
  //  - Keep track the total size of all SST files.
  //  - Set a maximum allowed space limit for SST files that when reached
  //    the DB wont do any further flushes or compactions and will set the
  //    background error.
  //  - Can be shared between multiple dbs.
  // Limitations:
  //  - Only track and throttle deletes of SST files in
  //    first db_path (db_name if db_paths is empty).
  //
  // Default: nullptr
  std::shared_ptr<SstFileManager> sst_file_manager = nullptr;

  // Any internal progress/error information generated by the db will
  // be written to info_log if it is non-nullptr, or to a file stored
  // in the same directory as the DB contents if info_log is nullptr.
  // Default: nullptr
  std::shared_ptr<Logger> info_log = nullptr;

#ifdef NDEBUG
      InfoLogLevel info_log_level = INFO_LEVEL;
#else
      InfoLogLevel info_log_level = DEBUG_LEVEL;
#endif  // NDEBUG

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set. Value -1 means
  // files opened are always kept open. You can estimate number of files based
  // on target_file_size_base and target_file_size_multiplier for level-based
  // compaction. For universal-style compaction, you can usually set it to -1.
  //
  // Default: -1
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_open_files = -1;

  // If max_open_files is -1, DB will open all files on DB::Open(). You can
  // use this option to increase the number of threads used to open the files.
  // Default: 16
  int max_file_opening_threads = 16;

  // Once write-ahead logs exceed this size, we will start forcing the flush of
  // column families whose memtables are backed by the oldest live WAL file
  // (i.e. the ones that are causing all the space amplification). If set to 0
  // (default), we will dynamically choose the WAL size limit to be
  // [sum of all write_buffer_size * max_write_buffer_number] * 4
  // This option takes effect only when there are more than one column family as
  // otherwise the wal size is dictated by the write_buffer_size.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t max_total_wal_size = 0;

  // If non-null, then we should collect metrics about database operations
  std::shared_ptr<Statistics> statistics = nullptr;

  // By default, writes to stable storage use fdatasync (on platforms
  // where this function is available). If this option is true,
  // fsync is used instead.
  //
  // fsync and fdatasync are equally safe for our purposes and fdatasync is
  // faster, so it is rarely necessary to set this option. It is provided
  // as a workaround for kernel/filesystem bugs, such as one that affected
  // fdatasync with ext4 in kernel versions prior to 3.7.
  bool use_fsync = false;

  // A list of paths where SST files can be put into, with its target size.
  // Newer data is placed into paths specified earlier in the vector while
  // older data gradually moves to paths specified later in the vector.
  //
  // For example, you have a flash device with 10GB allocated for the DB,
  // as well as a hard drive of 2TB, you should config it to be:
  //   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
  //
  // The system will try to guarantee data under each path is close to but
  // not larger than the target size. But current and future file sizes used
  // by determining where to place a file are based on best-effort estimation,
  // which means there is a chance that the actual size under the directory
  // is slightly more than target size under some workloads. User should give
  // some buffer room for those cases.
  //
  // If none of the paths has sufficient room to place a file, the file will
  // be placed to the last path anyway, despite to the target size.
  //
  // Placing newer data to earlier paths is also best-efforts. User should
  // expect user files to be placed in higher levels in some extreme cases.
  //
  // If left empty, only one path will be used, which is db_name passed when
  // opening the DB.
  // Default: empty
  std::vector<DbPath> db_paths;

  // This specifies the info LOG dir.
  // If it is empty, the log files will be in the same dir as data.
  // If it is non empty, the log files will be in the specified dir,
  // and the db data dir's absolute path will be used as the log file
  // name's prefix.
  std::string db_log_dir = "";

  // This specifies the absolute dir path for write-ahead logs (WAL).
  // If it is empty, the log files will be in the same dir as data,
  //   dbname is used as the data dir by default
  // If it is non empty, the log files will be in kept the specified dir.
  // When destroying the db,
  //   all log files in wal_dir and the dir itself is deleted
  std::string wal_dir = "";

  // The periodicity when obsolete files get deleted. The default
  // value is 6 hours. The files that get out of scope by compaction
  // process will still get automatically delete on every compaction,
  // regardless of this setting
  //
  // Default: 6 hours
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;

  // Maximum number of concurrent background jobs (compactions and flushes).
  //
  // Default: 2
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_background_jobs = 2;

  // NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
  // value of max_background_jobs. This option is ignored.
  //
  // Dynamically changeable through SetDBOptions() API.
  int base_background_compactions = -1;

  // NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
  // value of max_background_jobs. For backwards compatibility we will set
  // `max_background_jobs = max_background_compactions + max_background_flushes`
  // in the case where user sets at least one of `max_background_compactions` or
  // `max_background_flushes` (we replace -1 by 1 in case one option is unset).
  //
  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  //
  // If you're increasing this, also consider increasing number of threads in
  // LOW priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  //
  // Default: -1
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_background_compactions = -1;

  // This value represents the maximum number of threads that will
  // concurrently perform a compaction job by breaking it into multiple,
  // smaller ones that are run simultaneously.
  // Default: 1 (i.e. no subcompactions)
  uint32_t max_subcompactions = 1;

  // NOT SUPPORTED ANYMORE: RocksDB automatically decides this based on the
  // value of max_background_jobs. For backwards compatibility we will set
  // `max_background_jobs = max_background_compactions + max_background_flushes`
  // in the case where user sets at least one of `max_background_compactions` or
  // `max_background_flushes`.
  //
  // Maximum number of concurrent background memtable flush jobs, submitted by
  // default to the HIGH priority thread pool. If the HIGH priority thread pool
  // is configured to have zero threads, flush jobs will share the LOW priority
  // thread pool with compaction jobs.
  //
  // It is important to use both thread pools when the same Env is shared by
  // multiple db instances. Without a separate pool, long running compaction
  // jobs could potentially block memtable flush jobs of other db instances,
  // leading to unnecessary Put stalls.
  //
  // If you're increasing this, also consider increasing number of threads in
  // HIGH priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: -1
  int max_background_flushes = -1;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  size_t max_log_file_size = 0;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  // Not supported in ROCKSDB_LITE mode!
  size_t log_file_time_to_roll = 0;

  // Maximal info log files to be kept.
  // Default: 1000
  size_t keep_log_file_num = 1000;

  // Recycle log files.
  // If non-zero, we will reuse previously written log files for new
  // logs, overwriting the old data.  The value indicates how many
  // such files we will keep around at any point in time for later
  // use.  This is more efficient because the blocks are already
  // allocated and fdatasync does not need to update the inode after
  // each write.
  // Default: 0
  size_t recycle_log_file_num = 0;

  // manifest file is rolled over on reaching this limit.
  // The older manifest file be deleted.
  // The default value is 1GB so that the manifest file can grow, but not
  // reach the limit of storage capacity.
  uint64_t max_manifest_file_size = 1024 * 1024 * 1024;

  // Number of shards used for table cache.
  int table_cache_numshardbits = 6;

  // NOT SUPPORTED ANYMORE
  // int table_cache_remove_scan_count_limit;

  // The following two fields affect how archived logs will be deleted.
  // 1. If both set to 0, logs will be deleted asap and will not get into
  //    the archive.
  // 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
  //    WAL files will be checked every 10 min and if total size is greater
  //    then WAL_size_limit_MB, they will be deleted starting with the
  //    earliest until size_limit is met. All empty files will be deleted.
  // 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
  //    WAL files will be checked every WAL_ttl_seconds / 2 and those that
  //    are older than WAL_ttl_seconds will be deleted.
  // 4. If both are not 0, WAL files will be checked every 10 min and both
  //    checks will be performed with ttl being first.
  uint64_t WAL_ttl_seconds = 0;
  uint64_t WAL_size_limit_MB = 0;

  // Number of bytes to preallocate (via fallocate) the manifest
  // files.  Default is 4mb, which is reasonable to reduce random IO
  // as well as prevent overallocation for mounts that preallocate
  // large amounts of data (such as xfs's allocsize option).
  size_t manifest_preallocation_size = 4 * 1024 * 1024;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads = false;

  // Allow the OS to mmap file for writing.
  // DB::SyncWAL() only works if this is set to false.
  // Default: false
  bool allow_mmap_writes = false;

  // Enable direct I/O mode for read/write
  // they may or may not improve performance depending on the use case
  //
  // Files will be opened in "direct I/O" mode
  // which means that data r/w from the disk will not be cached or
  // buffered. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by these parameters.

  // Use O_DIRECT for user and compaction reads.
  // When true, we also force new_table_reader_for_compaction_inputs to true.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool use_direct_reads = false;

  // Use O_DIRECT for writes in background flush and compactions.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool use_direct_io_for_flush_and_compaction = false;

  // If false, fallocate() calls are bypassed
  bool allow_fallocate = true;

  // Disable child process inherit open files. Default: true
  bool is_fd_close_on_exec = true;

  // NOT SUPPORTED ANYMORE -- this options is no longer used
  bool skip_log_error_on_recovery = false;

  // if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
  //
  // Default: 600 (10 min)
  //
  // Dynamically changeable through SetDBOptions() API.
  unsigned int stats_dump_period_sec = 600;

  // if not zero, dump rocksdb.stats to RocksDB every stats_persist_period_sec
  // Default: 600
  unsigned int stats_persist_period_sec = 600;

  // if not zero, periodically take stats snapshots and store in memory, the
  // memory size for stats snapshots is capped at stats_history_buffer_size
  // Default: 1MB
  size_t stats_history_buffer_size = 1024 * 1024;

  // If set true, will hint the underlying file system that the file
  // access pattern is random, when a sst file is opened.
  // Default: true
  bool advise_random_on_open = true;

  // Amount of data to build up in memtables across all column
  // families before writing to disk.
  //
  // This is distinct from write_buffer_size, which enforces a limit
  // for a single memtable.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: 0 (disabled)
  size_t db_write_buffer_size = 0;

  // The memory usage of memtable will report to this object. The same object
  // can be passed into multiple DBs and it will track the sum of size of all
  // the DBs. If the total size of all live memtables of all the DBs exceeds
  // a limit, a flush will be triggered in the next DB to which the next write
  // is issued.
  //
  // If the object is only passed to one DB, the behavior is the same as
  // db_write_buffer_size. When write_buffer_manager is set, the value set will
  // override db_write_buffer_size.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: null
  std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;

  // Specify the file access pattern once a compaction is started.
  // It will be applied to all input files of a compaction.
  // Default: NORMAL
  enum AccessHint {
      NONE,
      NORMAL,
      SEQUENTIAL,
      WILLNEED
  };
  AccessHint access_hint_on_compaction_start = NORMAL;

  // If true, always create a new file descriptor and new table reader
  // for compaction inputs. Turn this parameter on may introduce extra
  // memory usage in the table reader, if it allocates extra memory
  // for indexes. This will allow file descriptor prefetch options
  // to be set for compaction input files and not to impact file
  // descriptors for the same file used by user queries.
  // Suggest to enable BlockBasedTableOptions.cache_index_and_filter_blocks
  // for this mode if using block-based table.
  //
  // Default: false
  bool new_table_reader_for_compaction_inputs = false;

  // If non-zero, we perform bigger reads when doing compaction. If you're
  // running RocksDB on spinning disks, you should set this to at least 2MB.
  // That way RocksDB's compaction is doing sequential instead of random reads.
  //
  // When non-zero, we also force new_table_reader_for_compaction_inputs to
  // true.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  size_t compaction_readahead_size = 0;

  // This is a maximum buffer size that is used by WinMmapReadableFile in
  // unbuffered disk I/O mode. We need to maintain an aligned buffer for
  // reads. We allow the buffer to grow until the specified value and then
  // for bigger requests allocate one shot buffers. In unbuffered mode we
  // always bypass read-ahead buffer at ReadaheadRandomAccessFile
  // When read-ahead is required we then make use of compaction_readahead_size
  // value and always try to read ahead. With read-ahead we always
  // pre-allocate buffer to the size instead of growing it up to a limit.
  //
  // This option is currently honored only on Windows
  //
  // Default: 1 Mb
  //
  // Special value: 0 - means do not maintain per instance buffer. Allocate
  //                per request buffer and avoid locking.
  size_t random_access_max_buffer_size = 1024 * 1024;

  // This is the maximum buffer size that is used by WritableFileWriter.
  // On Windows, we need to maintain an aligned buffer for writes.
  // We allow the buffer to grow until it's size hits the limit in buffered
  // IO and fix the buffer size when using direct IO to ensure alignment of
  // write requests if the logical sector size is unusual
  //
  // Default: 1024 * 1024 (1 MB)
  //
  // Dynamically changeable through SetDBOptions() API.
  size_t writable_file_max_buffer_size = 1024 * 1024;


  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex = false;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump(Logger* log) const;

  // Allows OS to incrementally sync files to disk while they are being
  // written, asynchronously, in the background. This operation can be used
  // to smooth out write I/Os over time. Users shouldn't rely on it for
  // persistency guarantee.
  // Issue one request for every bytes_per_sync written. 0 turns it off.
  //
  // You may consider using rate_limiter to regulate write rate to device.
  // When rate limiter is enabled, it automatically enables bytes_per_sync
  // to 1MB.
  //
  // This option applies to table files
  //
  // Default: 0, turned off
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t bytes_per_sync = 0;

  // Same as bytes_per_sync, but applies to WAL files
  //
  // Default: 0, turned off
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t wal_bytes_per_sync = 0;

  // A vector of EventListeners whose callback functions will be called
  // when specific RocksDB event happens.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // If true, then the status of the threads involved in this DB will
  // be tracked and available via GetThreadList() API.
  //
  // Default: false
  bool enable_thread_tracking = false;

  // The limited write rate to DB if soft_pending_compaction_bytes_limit or
  // level0_slowdown_writes_trigger is triggered, or we are writing to the
  // last mem table allowed and we allow more than 3 mem tables. It is
  // calculated using size of user write requests before compression.
  // RocksDB may decide to slow down more if the compaction still
  // gets behind further.
  // If the value is 0, we will infer a value from `rater_limiter` value
  // if it is not empty, or 16MB if `rater_limiter` is empty. Note that
  // if users change the rate in `rate_limiter` after DB is opened,
  // `delayed_write_rate` won't be adjusted.
  //
  // Unit: byte per second.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t delayed_write_rate = 0;

  // By default, a single write thread queue is maintained. The thread gets
  // to the head of the queue becomes write batch group leader and responsible
  // for writing to WAL and memtable for the batch group.
  //
  // If enable_pipelined_write is true, separate write thread queue is
  // maintained for WAL write and memtable write. A write thread first enter WAL
  // writer queue and then memtable writer queue. Pending thread on the WAL
  // writer queue thus only have to wait for previous writers to finish their
  // WAL writing but not the memtable writing. Enabling the feature may improve
  // write throughput and reduce latency of the prepare phase of two-phase
  // commit.
  //
  // Default: false
  bool enable_pipelined_write = false;

  // If true, allow multi-writers to update mem tables in parallel.
  // Only some memtable_factory-s support concurrent writes; currently it
  // is implemented only for SkipListFactory.  Concurrent memtable writes
  // are not compatible with inplace_update_support or filter_deletes.
  // It is strongly recommended to set enable_write_thread_adaptive_yield
  // if you are going to use this feature.
  //
  // Default: true
  bool allow_concurrent_memtable_write = true;

  // If true, threads synchronizing with the write batch group leader will
  // wait for up to write_thread_max_yield_usec before blocking on a mutex.
  // This can substantially improve throughput for concurrent workloads,
  // regardless of whether allow_concurrent_memtable_write is enabled.
  //
  // Default: true
  bool enable_write_thread_adaptive_yield = true;

  // The maximum number of microseconds that a write operation will use
  // a yielding spin loop to coordinate with other write threads before
  // blocking on a mutex.  (Assuming write_thread_slow_yield_usec is
  // set properly) increasing this value is likely to increase RocksDB
  // throughput at the expense of increased CPU usage.
  //
  // Default: 100
  uint64_t write_thread_max_yield_usec = 100;

  // The latency in microseconds after which a std::this_thread::yield
  // call (sched_yield on Linux) is considered to be a signal that
  // other processes or threads would like to use the current core.
  // Increasing this makes writer threads more likely to take CPU
  // by spinning, which will show up as an increase in the number of
  // involuntary context switches.
  //
  // Default: 3
  uint64_t write_thread_slow_yield_usec = 3;

  // If true, then DB::Open() will not update the statistics used to optimize
  // compaction decision by loading table properties from many files.
  // Turning off this feature will improve DBOpen time especially in
  // disk environment.
  //
  // Default: false
  bool skip_stats_update_on_db_open = false;

  // Recovery mode to control the consistency while replaying WAL
  // Default: kPointInTimeRecovery
  WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  // if set to false then recovery will fail when a prepared
  // transaction is encountered in the WAL
  bool allow_2pc = false;

  // A global cache for table-level rows.
  // Default: nullptr (disabled)
  // Not supported in ROCKSDB_LITE mode!
  std::shared_ptr<Cache> row_cache = nullptr;

#ifndef ROCKSDB_LITE
  // A filter object supplied to be invoked while processing write-ahead-logs
  // (WALs) during recovery. The filter provides a way to inspect log
  // records, ignoring a particular record or skipping replay.
  // The filter is invoked at startup and is invoked from a single-thread
  // currently.
  WalFilter* wal_filter = nullptr;
#endif  // ROCKSDB_LITE

  // If true, then DB::Open / CreateColumnFamily / DropColumnFamily
  // / SetOptions will fail if options file is not detected or properly
  // persisted.
  //
  // DEFAULT: false
  bool fail_if_options_file_error = false;

  // If true, then print malloc stats together with rocksdb.stats
  // when printing to LOG.
  // DEFAULT: false
  bool dump_malloc_stats = false;

  // By default RocksDB replay WAL logs and flush them on DB open, which may
  // create very small SST files. If this option is enabled, RocksDB will try
  // to avoid (but not guarantee not to) flush during recovery. Also, existing
  // WAL logs will be kept, so that if crash happened before flush, we still
  // have logs to recover from.
  //
  // DEFAULT: false
  bool avoid_flush_during_recovery = false;

  // By default RocksDB will flush all memtables on DB close if there are
  // unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
  // DB close. Unpersisted data WILL BE LOST.
  //
  // DEFAULT: false
  //
  // Dynamically changeable through SetDBOptions() API.
  bool avoid_flush_during_shutdown = false;

  // Set this option to true during creation of database if you want
  // to be able to ingest behind (call IngestExternalFile() skipping keys
  // that already exist, rather than overwriting matching keys).
  // Setting this option to true will affect 2 things:
  // 1) Disable some internal optimizations around SST file compression
  // 2) Reserve bottom-most level for ingested files only.
  // 3) Note that num_levels should be >= 3 if this option is turned on.
  //
  // DEFAULT: false
  // Immutable.
  bool allow_ingest_behind = false;

  // Needed to support differential snapshots.
  // If set to true then DB will only process deletes with sequence number
  // less than what was set by SetPreserveDeletesSequenceNumber(uint64_t ts).
  // Clients are responsible to periodically call this method to advance
  // the cutoff time. If this method is never called and preserve_deletes
  // is set to true NO deletes will ever be processed.
  // At the moment this only keeps normal deletes, SingleDeletes will
  // not be preserved.
  // DEFAULT: false
  // Immutable (TODO: make it dynamically changeable)
  bool preserve_deletes = false;

  // If enabled it uses two queues for writes, one for the ones with
  // disable_memtable and one for the ones that also write to memtable. This
  // allows the memtable writes not to lag behind other writes. It can be used
  // to optimize MySQL 2PC in which only the commits, which are serial, write to
  // memtable.
  bool two_write_queues = false;

  // If true WAL is not flushed automatically after each write. Instead it
  // relies on manual invocation of FlushWAL to write the WAL buffer to its
  // file.
  bool manual_wal_flush = false;

  // If true, RocksDB supports flushing multiple column families and committing
  // their results atomically to MANIFEST. Note that it is not
  // necessary to set atomic_flush to true if WAL is always enabled since WAL
  // allows the database to be restored to the last persistent state in WAL.
  // This option is useful when there are column families with writes NOT
  // protected by WAL.
  // For manual flush, application has to specify which column families to
  // flush atomically in DB::Flush.
  // For auto-triggered flush, RocksDB atomically flushes ALL column families.
  //
  // Currently, any WAL-enabled writes after atomic flush may be replayed
  // independently if the process crashes later and tries to recover.
  bool atomic_flush = false;
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  // The function recovers options to the option as in version 4.6.
  Options* OldDefaults(int rocksdb_major_version = 4,
                       int rocksdb_minor_version = 6);

  void Dump(Logger* log) const;

  void DumpCFOptions(Logger* log) const;

  // Some functions that make it easier to optimize RocksDB

  // Set appropriate parameters for bulk loading.
  // The reason that this is a function that returns "this" instead of a
  // constructor is to enable chaining of multiple similar calls in the future.
  //

  // All data will be in level 0 without any automatic compaction.
  // It's recommended to manually call CompactRange(NULL, NULL) before reading
  // from the database, because otherwise the read can be very slow.
  Options* PrepareForBulkLoad();

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  Options* OptimizeForSmallDb();
};

//
// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,     // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1,  // data in memtable or block cache
  kPersistedTier = 0x2,   // persisted data.  When WAL is disabled, this option
                          // will skip data in memtable.
                          // Note that this ReadTier currently only supports
                          // Get and MultiGet and does not support iterators.
  kMemtableTier = 0x3     // data in memtable. used for memtable-only iterators.
};

// Options that control read operations
struct ReadOptions {
  // If "snapshot" is non-nullptr, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is nullptr, use an implicit
  // snapshot of the state at the beginning of this read operation.
  // Default: nullptr
  const Snapshot* snapshot;

  // `iterate_lower_bound` defines the smallest key at which the backward
  // iterator can return an entry. Once the bound is passed, Valid() will be
  // false. `iterate_lower_bound` is inclusive ie the bound value is a valid
  // entry.
  //
  // If prefix_extractor is not null, the Seek target and `iterate_lower_bound`
  // need to have the same prefix. This is because ordering is not guaranteed
  // outside of prefix domain.
  //
  // Default: nullptr
  const Slice* iterate_lower_bound;

  // "iterate_upper_bound" defines the extent upto which the forward iterator
  // can returns entries. Once the bound is reached, Valid() will be false.
  // "iterate_upper_bound" is exclusive ie the bound value is
  // not a valid entry.  If iterator_extractor is not null, the Seek target
  // and iterate_upper_bound need to have the same prefix.
  // This is because ordering is not guaranteed outside of prefix domain.
  //
  // Default: nullptr
  const Slice* iterate_upper_bound;

  // If non-zero, NewIterator will create a new table reader which
  // performs reads of the given size. Using a large size (> 2MB) can
  // improve the performance of forward iteration on spinning disks.
  // Default: 0
  size_t readahead_size;

  // A threshold for the number of keys that can be skipped before failing an
  // iterator seek as incomplete. The default value of 0 should be used to
  // never fail a request as incomplete, even on skipping too many keys.
  // Default: 0
  uint64_t max_skippable_internal_keys;

  // Specify if this read request should process data that ALREADY
  // resides on a particular cache. If the required data is not
  // found at the specified cache, then Status::Incomplete is returned.
  // Default: kReadAllTier
  ReadTier read_tier;

  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  // Default: true
  bool verify_checksums;

  // Should the "data block"/"index block"" read for this iteration be placed in
  // block cache?
  // Callers may wish to set this field to false for bulk scans.
  // This would help not to the change eviction order of existing items in the
  // block cache. Default: true
  bool fill_cache;

  // Specify to create a tailing iterator -- a special iterator that has a
  // view of the complete database (i.e. it can also be used to read newly
  // added data) and is optimized for sequential reads. It will return records
  // that were inserted into the database after the creation of the iterator.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool tailing;

  // This options is not used anymore. It was to turn on a functionality that
  // has been removed.
  bool managed;

  // Enable a total order seek regardless of index format (e.g. hash index)
  // used in the table. Some table format (e.g. plain table) may not support
  // this option.
  // If true when calling Get(), we also skip prefix bloom when reading from
  // block based table. It provides a way to read existing data after
  // changing implementation of prefix extractor.
  bool total_order_seek;

  // Enforce that the iterator only iterates over the same prefix as the seek.
  // This option is effective only for prefix seeks, i.e. prefix_extractor is
  // non-null for the column family and total_order_seek is false.  Unlike
  // iterate_upper_bound, prefix_same_as_start only works within a prefix
  // but in both directions.
  // Default: false
  bool prefix_same_as_start;

  // Keep the blocks loaded by the iterator pinned in memory as long as the
  // iterator is not deleted, If used when reading from tables created with
  // BlockBasedTableOptions::use_delta_encoding = false,
  // Iterator's property "rocksdb.iterator.is-key-pinned" is guaranteed to
  // return 1.
  // Default: false
  bool pin_data;

  // If true, when PurgeObsoleteFile is called in CleanupIteratorState, we
  // schedule a background job in the flush job queue and delete obsolete files
  // in background.
  // Default: false
  bool background_purge_on_iterator_cleanup;

  // If true, keys deleted using the DeleteRange() API will be visible to
  // readers until they are naturally deleted during compaction. This improves
  // read performance in DBs with many range deletions.
  // Default: false
  bool ignore_range_deletions;

  // A callback to determine whether relevant keys for this scan exist in a
  // given table based on the table's properties. The callback is passed the
  // properties of each table during iteration. If the callback returns false,
  // the table will not be scanned. This option only affects Iterators and has
  // no impact on point lookups.
  // Default: empty (every table will be scanned)
  std::function<bool(const TableProperties&)> table_filter;

  // Needed to support differential snapshots. Has 2 effects:
  // 1) Iterator will skip all internal keys with seqnum < iter_start_seqnum
  // 2) if this param > 0 iterator will return INTERNAL keys instead of
  //    user keys; e.g. return tombstones as well.
  // Default: 0 (don't filter by seqnum, return user keys)
  SequenceNumber iter_start_seqnum;

  ReadOptions();
  ReadOptions(bool cksum, bool cache);
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fdatasync()".
  //
  // Default: false
  bool sync;

  // If true, writes will not first go to the write ahead log,
  // and the write may got lost after a crash.
  // Default: false
  bool disableWAL;

  // If true and if user is trying to write to column families that don't exist
  // (they were dropped),  ignore the write (don't return an error). If there
  // are multiple writes in a WriteBatch, other writes will succeed.
  // Default: false
  bool ignore_missing_column_families;

  // If true and we need to wait or sleep for the write request, fails
  // immediately with Status::Incomplete().
  // Default: false
  bool no_slowdown;

  // If true, this write request is of lower priority if compaction is
  // behind. In this case, no_slowdown = true, the request will be cancelled
  // immediately with Status::Incomplete() returned. Otherwise, it will be
  // slowed down. The slowdown value is determined by RocksDB to guarantee
  // it introduces minimum impacts to high priority writes.
  //
  // Default: false
  bool low_pri;

  WriteOptions()
      : sync(false),
        disableWAL(false),
        ignore_missing_column_families(false),
        no_slowdown(false),
        low_pri(false) {}
};

// Options that control flush operations
struct FlushOptions {
  // If true, the flush will wait until the flush is done.
  // Default: true
  bool wait;
  // If true, the flush would proceed immediately even it means writes will
  // stall for the duration of the flush; if false the operation will wait
  // until it's possible to do flush w/o causing stall or until required flush
  // is performed by someone else (foreground call or background thread).
  // Default: false
  bool allow_write_stall;
  FlushOptions() : wait(true), allow_write_stall(false) {}
};

// Create a Logger from provided DBOptions
extern Status CreateLoggerFromOptions(const std::string& dbname,
                                      const DBOptions& options,
                                      std::shared_ptr<Logger>* logger);

// CompactionOptions are used in CompactFiles() call.
struct CompactionOptions {
  // Compaction output compression type
  // Default: snappy
  // If set to `kDisableCompressionOption`, RocksDB will choose compression type
  // according to the `ColumnFamilyOptions`, taking into account the output
  // level if `compression_per_level` is specified.
  CompressionType compression;
  // Compaction will create files of size `output_file_size_limit`.
  // Default: MAX, which means that compaction will create a single file
  uint64_t output_file_size_limit;
  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions;

  CompactionOptions()
      : compression(kSnappyCompression),
        output_file_size_limit(std::numeric_limits<uint64_t>::max()),
        max_subcompactions(0) {}
};

// For level based compaction, we can configure if we want to skip/force
// bottommost level compaction.
enum class BottommostLevelCompaction {
  // Skip bottommost level compaction
  kSkip,
  // Only compact bottommost level if there is a compaction filter
  // This is the default option
  kIfHaveCompactionFilter,
  // Always compact bottommost level
  kForce,
};

// CompactRangeOptions is used by CompactRange() call.
struct CompactRangeOptions {
  // If true, no other compaction will run at the same time as this
  // manual compaction
  bool exclusive_manual_compaction = true;
  // If true, compacted files will be moved to the minimum level capable
  // of holding the data or given level (specified non-negative target_level).
  bool change_level = false;
  // If change_level is true and target_level have non-negative value, compacted
  // files will be moved to target_level.
  int target_level = -1;
  // Compaction outputs will be placed in options.db_paths[target_path_id].
  // Behavior is undefined if target_path_id is out of range.
  uint32_t target_path_id = 0;
  // By default level based compaction will only compact the bottommost level
  // if there is a compaction filter
  BottommostLevelCompaction bottommost_level_compaction =
      BottommostLevelCompaction::kIfHaveCompactionFilter;
  // If true, will execute immediately even if doing so would cause the DB to
  // enter write stall mode. Otherwise, it'll sleep until load is low enough.
  bool allow_write_stall = false;
  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions = 0;
};

// IngestExternalFileOptions is used by IngestExternalFile()
struct IngestExternalFileOptions {
  // Can be set to true to move the files instead of copying them.
  bool move_files = false;
  // If set to false, an ingested file keys could appear in existing snapshots
  // that where created before the file was ingested.
  bool snapshot_consistency = true;
  // If set to false, IngestExternalFile() will fail if the file key range
  // overlaps with existing keys or tombstones in the DB.
  bool allow_global_seqno = true;
  // If set to false and the file key range overlaps with the memtable key range
  // (memtable flush required), IngestExternalFile will fail.
  bool allow_blocking_flush = true;
  // Set to true if you would like duplicate keys in the file being ingested
  // to be skipped rather than overwriting existing data under that key.
  // Usecase: back-fill of some historical data in the database without
  // over-writing existing newer version of data.
  // This option could only be used if the DB has been running
  // with allow_ingest_behind=true since the dawn of time.
  // All files will be ingested at the bottommost level with seqno=0.
  bool ingest_behind = false;
  // Set to true if you would like to write global_seqno to a given offset in
  // the external SST file for backward compatibility. Older versions of
  // RocksDB writes a global_seqno to a given offset within ingested SST files,
  // and new versions of RocksDB do not. If you ingest an external SST using
  // new version of RocksDB and would like to be able to downgrade to an
  // older version of RocksDB, you should set 'write_global_seqno' to true. If
  // your service is just starting to use the new RocksDB, we recommend that
  // you set this option to false, which brings two benefits:
  // 1. No extra random write for global_seqno during ingestion.
  // 2. Without writing external SST file, it's possible to do checksum.
  // We have a plan to set this option to false by default in the future.
  bool write_global_seqno = true;
  // Set to true if you would like to verify the checksums of each block of the
  // external SST file before ingestion.
  // Warning: setting this to true causes slowdown in file ingestion because
  // the external SST file has to be read.
  bool verify_checksums_before_ingest = false;
};

// TraceOptions is used for StartTrace
struct TraceOptions {
  // To avoid the trace file size grows large than the storage space,
  // user can set the max trace file size in Bytes. Default is 64GB
  uint64_t max_trace_file_size = uint64_t{64} * 1024 * 1024 * 1024;
  // Specify trace sampling option, i.e. capture one per how many requests.
  // Default to 1 (capture every request).
  uint64_t sampling_frequency = 1;
};

}  // namespace rocksdb
