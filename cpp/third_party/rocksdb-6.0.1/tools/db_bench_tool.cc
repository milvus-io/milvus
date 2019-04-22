//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#ifdef GFLAGS
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "db/db_impl.h"
#include "db/malloc_stats.h"
#include "db/version_set.h"
#include "hdfs/env_hdfs.h"
#include "monitoring/histogram.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/sim_cache.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
#include "util/xxhash.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/persistent_cache/block_cache_tier.h"

#ifdef OS_WIN
#include <io.h>  // open/close
#endif

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(
    benchmarks,
    "fillseq,"
    "fillseqdeterministic,"
    "fillsync,"
    "fillrandom,"
    "filluniquerandomdeterministic,"
    "overwrite,"
    "readrandom,"
    "newiterator,"
    "newiteratorwhilewriting,"
    "seekrandom,"
    "seekrandomwhilewriting,"
    "seekrandomwhilemerging,"
    "readseq,"
    "readreverse,"
    "compact,"
    "compactall,"
    "multireadrandom,"
    "mixgraph,"
    "readseq,"
    "readtocache,"
    "readreverse,"
    "readwhilewriting,"
    "readwhilemerging,"
    "readwhilescanning,"
    "readrandomwriterandom,"
    "updaterandom,"
    "xorupdaterandom,"
    "randomwithverify,"
    "fill100K,"
    "crc32c,"
    "xxhash,"
    "compress,"
    "uncompress,"
    "acquireload,"
    "fillseekseq,"
    "randomtransaction,"
    "randomreplacekeys,"
    "timeseries",

    "Comma-separated list of operations to run in the specified"
    " order. Available benchmarks:\n"
    "\tfillseq       -- write N values in sequential key"
    " order in async mode\n"
    "\tfillseqdeterministic       -- write N values in the specified"
    " key order and keep the shape of the LSM tree\n"
    "\tfillrandom    -- write N values in random key order in async"
    " mode\n"
    "\tfilluniquerandomdeterministic       -- write N values in a random"
    " key order and keep the shape of the LSM tree\n"
    "\toverwrite     -- overwrite N values in random key order in"
    " async mode\n"
    "\tfillsync      -- write N/100 values in random key order in "
    "sync mode\n"
    "\tfill100K      -- write N/1000 100K values in random order in"
    " async mode\n"
    "\tdeleteseq     -- delete N keys in sequential order\n"
    "\tdeleterandom  -- delete N keys in random order\n"
    "\treadseq       -- read N times sequentially\n"
    "\treadtocache   -- 1 thread reading database sequentially\n"
    "\treadreverse   -- read N times in reverse order\n"
    "\treadrandom    -- read N times in random order\n"
    "\treadmissing   -- read N missing keys in random order\n"
    "\treadwhilewriting      -- 1 writer, N threads doing random "
    "reads\n"
    "\treadwhilemerging      -- 1 merger, N threads doing random "
    "reads\n"
    "\treadwhilescanning     -- 1 thread doing full table scan, "
    "N threads doing random reads\n"
    "\treadrandomwriterandom -- N threads doing random-read, "
    "random-write\n"
    "\tupdaterandom  -- N threads doing read-modify-write for random "
    "keys\n"
    "\txorupdaterandom  -- N threads doing read-XOR-write for "
    "random keys\n"
    "\tappendrandom  -- N threads doing read-modify-write with "
    "growing values\n"
    "\tmergerandom   -- same as updaterandom/appendrandom using merge"
    " operator. "
    "Must be used with merge_operator\n"
    "\treadrandommergerandom -- perform N random read-or-merge "
    "operations. Must be used with merge_operator\n"
    "\tnewiterator   -- repeated iterator creation\n"
    "\tseekrandom    -- N random seeks, call Next seek_nexts times "
    "per seek\n"
    "\tseekrandomwhilewriting -- seekrandom and 1 thread doing "
    "overwrite\n"
    "\tseekrandomwhilemerging -- seekrandom and 1 thread doing "
    "merge\n"
    "\tcrc32c        -- repeated crc32c of 4K of data\n"
    "\txxhash        -- repeated xxHash of 4K of data\n"
    "\tacquireload   -- load N*1000 times\n"
    "\tfillseekseq   -- write N values in sequential key, then read "
    "them by seeking to each key\n"
    "\trandomtransaction     -- execute N random transactions and "
    "verify correctness\n"
    "\trandomreplacekeys     -- randomly replaces N keys by deleting "
    "the old version and putting the new version\n\n"
    "\ttimeseries            -- 1 writer generates time series data "
    "and multiple readers doing random reads on id\n\n"
    "Meta operations:\n"
    "\tcompact     -- Compact the entire DB; If multiple, randomly choose one\n"
    "\tcompactall  -- Compact the entire DB\n"
    "\tstats       -- Print DB stats\n"
    "\tresetstats  -- Reset DB stats\n"
    "\tlevelstats  -- Print the number of files and bytes per level\n"
    "\tsstables    -- Print sstable info\n"
    "\theapprofile -- Dump a heap profile (if supported by this port)\n"
    "\treplay      -- replay the trace file specified with trace_file\n");

DEFINE_int64(num, 1000000, "Number of key/values to place in database");

DEFINE_int64(numdistinct, 1000,
             "Number of distinct keys to use. Used in RandomWithVerify to "
             "read/write on fewer keys so that gets are more likely to find the"
             " key and puts are more likely to update the same key");

DEFINE_int64(merge_keys, -1,
             "Number of distinct keys to use for MergeRandom and "
             "ReadRandomMergeRandom. "
             "If negative, there will be FLAGS_num keys.");
DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");

DEFINE_int32(
    num_hot_column_families, 0,
    "Number of Hot Column Families. If more than 0, only write to this "
    "number of column families. After finishing all the writes to them, "
    "create new set of column families and insert to them. Only used "
    "when num_column_families > 1.");

DEFINE_string(column_family_distribution, "",
              "Comma-separated list of percentages, where the ith element "
              "indicates the probability of an op using the ith column family. "
              "The number of elements must be `num_hot_column_families` if "
              "specified; otherwise, it must be `num_column_families`. The "
              "sum of elements must be 100. E.g., if `num_column_families=4`, "
              "and `num_hot_column_families=0`, a valid list could be "
              "\"10,20,30,40\".");

DEFINE_int64(reads, -1, "Number of read operations to do.  "
             "If negative, do FLAGS_num reads.");

DEFINE_int64(deletes, -1, "Number of delete operations to do.  "
             "If negative, do FLAGS_num deletions.");

DEFINE_int32(bloom_locality, 0, "Control bloom filter probes locality");

DEFINE_int64(seed, 0, "Seed base for random number generators. "
             "When 0 it is deterministic.");

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(duration, 0, "Time in seconds for the random-ops tests to run."
             " When 0 then num & reads determine the test duration");

DEFINE_int32(value_size, 100, "Size of each value");

DEFINE_int32(seek_nexts, 0,
             "How many times to call Next() after Seek() in "
             "fillseekseq, seekrandom, seekrandomwhilewriting and "
             "seekrandomwhilemerging");

DEFINE_bool(reverse_iterator, false,
            "When true use Prev rather than Next for iterators that do "
            "Seek and then Next");

DEFINE_int64(max_scan_distance, 0,
             "Used to define iterate_upper_bound (or iterate_lower_bound "
             "if FLAGS_reverse_iterator is set to true) when value is nonzero");

DEFINE_bool(use_uint64_comparator, false, "use Uint64 user comparator");

DEFINE_int64(batch_size, 1, "Batch size");

static bool ValidateKeySize(const char* /*flagname*/, int32_t /*value*/) {
  return true;
}

static bool ValidateUint32Range(const char* flagname, uint64_t value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    fprintf(stderr, "Invalid value for --%s: %lu, overflow\n", flagname,
            (unsigned long)value);
    return false;
  }
  return true;
}

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_int32(num_multi_db, 0,
             "Number of DBs used in the benchmark. 0 means single DB.");

DEFINE_double(compression_ratio, 0.5, "Arrange to generate values that shrink"
              " to this fraction of their original size after compression");

DEFINE_double(read_random_exp_range, 0.0,
              "Read random's key will be generated using distribution of "
              "num * exp(-r) where r is uniform number from 0 to this value. "
              "The larger the number is, the more skewed the reads are. "
              "Only used in readrandom and multireadrandom benchmarks.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_bool(enable_numa, false,
            "Make operations aware of NUMA architecture and bind memory "
            "and cpus corresponding to nodes together. In NUMA, memory "
            "in same node as CPUs are closer when compared to memory in "
            "other nodes. Reads can be faster when the process is bound to "
            "CPU and memory of same node. Use \"$numactl --hardware\" command "
            "to see NUMA memory architecture.");

DEFINE_int64(db_write_buffer_size, rocksdb::Options().db_write_buffer_size,
             "Number of bytes to buffer in all memtables before compacting");

DEFINE_bool(cost_write_buffer_to_cache, false,
            "The usage of memtable is costed to the block cache");

DEFINE_int64(write_buffer_size, rocksdb::Options().write_buffer_size,
             "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             rocksdb::Options().max_write_buffer_number,
             "The number of in-memory memtables. Each memtable is of size"
             " write_buffer_size bytes.");

DEFINE_int32(min_write_buffer_number_to_merge,
             rocksdb::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together"
             "before writing to storage. This is cheap because it is an"
             "in-memory merge. If this feature is not enabled, then all these"
             "write buffers are flushed to L0 as separate files and this "
             "increases read amplification because a get request has to check"
             " in all of these files. Also, an in-memory merge may result in"
             " writing less data to storage if there are duplicate records "
             " in each of these individual write buffers.");

DEFINE_int32(max_write_buffer_number_to_maintain,
             rocksdb::Options().max_write_buffer_number_to_maintain,
             "The total maximum number of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_int32(max_background_jobs,
             rocksdb::Options().max_background_jobs,
             "The maximum number of concurrent background jobs that can occur "
             "in parallel.");

DEFINE_int32(num_bottom_pri_threads, 0,
             "The number of threads in the bottom-priority thread pool (used "
             "by universal compaction only).");

DEFINE_int32(num_high_pri_threads, 0,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(num_low_pri_threads, 0,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(max_background_compactions,
             rocksdb::Options().max_background_compactions,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(base_background_compactions, -1, "DEPRECATED");

DEFINE_uint64(subcompactions, 1,
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");
static const bool FLAGS_subcompactions_dummy
    __attribute__((__unused__)) = RegisterFlagValidator(&FLAGS_subcompactions,
                                                    &ValidateUint32Range);

DEFINE_int32(max_background_flushes,
             rocksdb::Options().max_background_flushes,
             "The maximum number of concurrent background flushes"
             " that can occur in parallel.");

static rocksdb::CompactionStyle FLAGS_compaction_style_e;
DEFINE_int32(compaction_style, (int32_t) rocksdb::Options().compaction_style,
             "style of compaction: level-based, universal and fifo");

static rocksdb::CompactionPri FLAGS_compaction_pri_e;
DEFINE_int32(compaction_pri, (int32_t)rocksdb::Options().compaction_pri,
             "priority of files to compaction: by size or by data age");

DEFINE_int32(universal_size_ratio, 0,
             "Percentage flexibility while comparing file size"
             " (for universal compaction only).");

DEFINE_int32(universal_min_merge_width, 0, "The minimum number of files in a"
             " single compaction run (for universal compaction only).");

DEFINE_int32(universal_max_merge_width, 0, "The max number of files to compact"
             " in universal style compaction");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction");

DEFINE_int32(universal_compression_size_percent, -1,
             "The percentage of the database to compress for universal "
             "compaction. -1 means compress everything.");

DEFINE_bool(universal_allow_trivial_move, false,
            "Allow trivial move in universal compaction.");

DEFINE_int64(cache_size, 8 << 20,  // 8MB
             "Number of bytes to use as a cache of uncompressed data");

DEFINE_int32(cache_numshardbits, 6,
             "Number of shards for the block cache"
             " is 2 ** cache_numshardbits. Negative means use default settings."
             " This is applied only if FLAGS_cache_size is non-negative.");

DEFINE_double(cache_high_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for high pri blocks. "
              "If > 0.0, we also enable "
              "cache_index_and_filter_blocks_with_high_priority.");

DEFINE_bool(use_clock_cache, false,
            "Replace default LRU block cache with clock cache.");

DEFINE_int64(simcache_size, -1,
             "Number of bytes to use as a simcache of "
             "uncompressed data. Nagative value disables simcache.");

DEFINE_bool(cache_index_and_filter_blocks, false,
            "Cache index/filter blocks in block cache.");

DEFINE_bool(partition_index_and_filters, false,
            "Partition index and filter blocks.");

DEFINE_bool(partition_index, false, "Partition index blocks");

DEFINE_int64(metadata_block_size,
             rocksdb::BlockBasedTableOptions().metadata_block_size,
             "Max partition size when partitioning index/filters");

// The default reduces the overhead of reading time with flash. With HDD, which
// offers much less throughput, however, this number better to be set to 1.
DEFINE_int32(ops_between_duration_checks, 1000,
             "Check duration limit every x ops");

DEFINE_bool(pin_l0_filter_and_index_blocks_in_cache, false,
            "Pin index/filter blocks of L0 files in block cache.");

DEFINE_bool(
    pin_top_level_index_and_filter, false,
    "Pin top-level index of partitioned index/filter blocks in block cache.");

DEFINE_int32(block_size,
             static_cast<int32_t>(rocksdb::BlockBasedTableOptions().block_size),
             "Number of bytes in a block.");

DEFINE_int32(
    format_version,
    static_cast<int32_t>(rocksdb::BlockBasedTableOptions().format_version),
    "Format version of SST files.");

DEFINE_int32(block_restart_interval,
             rocksdb::BlockBasedTableOptions().block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in data block.");

DEFINE_int32(index_block_restart_interval,
             rocksdb::BlockBasedTableOptions().index_block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in index block.");

DEFINE_int32(read_amp_bytes_per_bit,
             rocksdb::BlockBasedTableOptions().read_amp_bytes_per_bit,
             "Number of bytes per bit to be used in block read-amp bitmap");

DEFINE_bool(enable_index_compression,
            rocksdb::BlockBasedTableOptions().enable_index_compression,
            "Compress the index block");

DEFINE_bool(block_align, rocksdb::BlockBasedTableOptions().block_align,
            "Align data blocks on page size");

DEFINE_bool(use_data_block_hash_index, false,
            "if use kDataBlockBinaryAndHash "
            "instead of kDataBlockBinarySearch. "
            "This is valid if only we use BlockTable");

DEFINE_double(data_block_hash_table_util_ratio, 0.75,
              "util ratio for data block hash index table. "
              "This is only valid if use_data_block_hash_index is "
              "set to true");

DEFINE_int64(compressed_cache_size, -1,
             "Number of bytes to use as a cache of compressed data.");

DEFINE_int64(row_cache_size, 0,
             "Number of bytes to use as a cache of individual rows"
             " (0 = disabled).");

DEFINE_int32(open_files, rocksdb::Options().max_open_files,
             "Maximum number of files to keep open at the same time"
             " (use default if == 0)");

DEFINE_int32(file_opening_threads, rocksdb::Options().max_file_opening_threads,
             "If open_files is set to -1, this option set the number of "
             "threads that will be used to open files during DB::Open()");

DEFINE_bool(new_table_reader_for_compaction_inputs, true,
             "If true, uses a separate file handle for compaction inputs");

DEFINE_int32(compaction_readahead_size, 0, "Compaction readahead size");

DEFINE_int32(random_access_max_buffer_size, 1024 * 1024,
             "Maximum windows randomaccess buffer size");

DEFINE_int32(writable_file_max_buffer_size, 1024 * 1024,
             "Maximum write buffer for Writable File");

DEFINE_int32(bloom_bits, -1, "Bloom filter bits per key. Negative means"
             " use default settings.");
DEFINE_double(memtable_bloom_size_ratio, 0,
              "Ratio of memtable size used for bloom filter. 0 means no bloom "
              "filter.");
DEFINE_bool(memtable_whole_key_filtering, false,
            "Try to use whole key bloom filter in memtables.");
DEFINE_bool(memtable_use_huge_page, false,
            "Try to use huge page in memtables.");

DEFINE_bool(use_existing_db, false, "If true, do not destroy the existing"
            " database.  If you set this flag and also specify a benchmark that"
            " wants a fresh database, that benchmark will fail.");

DEFINE_bool(show_table_properties, false,
            "If true, then per-level table"
            " properties will be printed on every stats-interval when"
            " stats_interval is set and stats_per_interval is on.");

DEFINE_string(db, "", "Use the db with the following name.");

// Read cache flags

DEFINE_string(read_cache_path, "",
              "If not empty string, a read cache will be used in this path");

DEFINE_int64(read_cache_size, 4LL * 1024 * 1024 * 1024,
             "Maximum size of the read cache");

DEFINE_bool(read_cache_direct_write, true,
            "Whether to use Direct IO for writing to the read cache");

DEFINE_bool(read_cache_direct_read, true,
            "Whether to use Direct IO for reading from read cache");

DEFINE_bool(use_keep_filter, false, "Whether to use a noop compaction filter");

static bool ValidateCacheNumshardbits(const char* flagname, int32_t value) {
  if (value >= 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be < 20\n",
            flagname, value);
    return false;
  }
  return true;
}

DEFINE_bool(verify_checksum, true,
            "Verify checksum for every block read"
            " from storage");

DEFINE_bool(statistics, false, "Database statistics");
DEFINE_string(statistics_string, "", "Serialized statistics string");
static class std::shared_ptr<rocksdb::Statistics> dbstats;

DEFINE_int64(writes, -1, "Number of write operations to do. If negative, do"
             " --num reads.");

DEFINE_bool(finish_after_writes, false, "Write thread terminates after all writes are finished");

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync");

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_string(wal_dir, "", "If not empty, use the given dir for WAL");

DEFINE_string(truth_db, "/dev/shm/truth_db/dbbench",
              "Truth key/values used when using verify");

DEFINE_int32(num_levels, 7, "The total number of levels");

DEFINE_int64(target_file_size_base, rocksdb::Options().target_file_size_base,
             "Target file size at level-1");

DEFINE_int32(target_file_size_multiplier,
             rocksdb::Options().target_file_size_multiplier,
             "A multiplier to compute target level-N file size (N >= 2)");

DEFINE_uint64(max_bytes_for_level_base,
              rocksdb::Options().max_bytes_for_level_base,
              "Max bytes for level-1");

DEFINE_bool(level_compaction_dynamic_level_bytes, false,
            "Whether level size base is dynamic");

DEFINE_double(max_bytes_for_level_multiplier, 10,
              "A multiplier to compute max bytes for level-N (N >= 2)");

static std::vector<int> FLAGS_max_bytes_for_level_multiplier_additional_v;
DEFINE_string(max_bytes_for_level_multiplier_additional, "",
              "A vector that specifies additional fanout per level");

DEFINE_int32(level0_stop_writes_trigger,
             rocksdb::Options().level0_stop_writes_trigger,
             "Number of files in level-0"
             " that will trigger put stop.");

DEFINE_int32(level0_slowdown_writes_trigger,
             rocksdb::Options().level0_slowdown_writes_trigger,
             "Number of files in level-0"
             " that will slow down writes.");

DEFINE_int32(level0_file_num_compaction_trigger,
             rocksdb::Options().level0_file_num_compaction_trigger,
             "Number of files in level-0"
             " when compactions start");

static bool ValidateInt32Percent(const char* flagname, int32_t value) {
  if (value <= 0 || value>=100) {
    fprintf(stderr, "Invalid value for --%s: %d, 0< pct <100 \n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(readwritepercent, 90, "Ratio of reads to reads/writes (expressed"
             " as percentage) for the ReadRandomWriteRandom workload. The "
             "default value 90 means 90% operations out of all reads and writes"
             " operations are reads. In other words, 9 gets for every 1 put.");

DEFINE_int32(mergereadpercent, 70, "Ratio of merges to merges&reads (expressed"
             " as percentage) for the ReadRandomMergeRandom workload. The"
             " default value 70 means 70% out of all read and merge operations"
             " are merges. In other words, 7 merges for every 3 gets.");

DEFINE_int32(deletepercent, 2, "Percentage of deletes out of reads/writes/"
             "deletes (used in RandomWithVerify only). RandomWithVerify "
             "calculates writepercent as (100 - FLAGS_readwritepercent - "
             "deletepercent), so deletepercent must be smaller than (100 - "
             "FLAGS_readwritepercent)");

DEFINE_bool(optimize_filters_for_hits, false,
            "Optimizes bloom filters for workloads for most lookups return "
            "a value. For now this doesn't create bloom filters for the max "
            "level of the LSM to reduce metadata that should fit in RAM. ");

DEFINE_uint64(delete_obsolete_files_period_micros, 0,
              "Ignored. Left here for backward compatibility");

DEFINE_int64(writes_before_delete_range, 0,
             "Number of writes before DeleteRange is called regularly.");

DEFINE_int64(writes_per_range_tombstone, 0,
             "Number of writes between range tombstones");

DEFINE_int64(range_tombstone_width, 100, "Number of keys in tombstone's range");

DEFINE_int64(max_num_range_tombstones, 0,
             "Maximum number of range tombstones "
             "to insert.");

DEFINE_bool(expand_range_tombstones, false,
            "Expand range tombstone into sequential regular tombstones.");

#ifndef ROCKSDB_LITE
// Transactions Options
DEFINE_bool(optimistic_transaction_db, false,
            "Open a OptimisticTransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_bool(transaction_db, false,
            "Open a TransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_uint64(transaction_sets, 2,
              "Number of keys each transaction will "
              "modify (use in RandomTransaction only).  Max: 9999");

DEFINE_bool(transaction_set_snapshot, false,
            "Setting to true will have each transaction call SetSnapshot()"
            " upon creation.");

DEFINE_int32(transaction_sleep, 0,
             "Max microseconds to sleep in between "
             "reading and writing a value (used in RandomTransaction only). ");

DEFINE_uint64(transaction_lock_timeout, 100,
              "If using a transaction_db, specifies the lock wait timeout in"
              " milliseconds before failing a transaction waiting on a lock");
DEFINE_string(
    options_file, "",
    "The path to a RocksDB options file.  If specified, then db_bench will "
    "run with the RocksDB options in the default column family of the "
    "specified options file. "
    "Note that with this setting, db_bench will ONLY accept the following "
    "RocksDB options related command-line arguments, all other arguments "
    "that are related to RocksDB options will be ignored:\n"
    "\t--use_existing_db\n"
    "\t--statistics\n"
    "\t--row_cache_size\n"
    "\t--row_cache_numshardbits\n"
    "\t--enable_io_prio\n"
    "\t--dump_malloc_stats\n"
    "\t--num_multi_db\n");

// FIFO Compaction Options
DEFINE_uint64(fifo_compaction_max_table_files_size_mb, 0,
              "The limit of total table file sizes to trigger FIFO compaction");

DEFINE_bool(fifo_compaction_allow_compaction, true,
            "Allow compaction in FIFO compaction.");

DEFINE_uint64(fifo_compaction_ttl, 0, "TTL for the SST Files in seconds.");

// Blob DB Options
DEFINE_bool(use_blob_db, false,
            "Open a BlobDB instance. "
            "Required for large value benchmark.");

DEFINE_bool(blob_db_enable_gc, false, "Enable BlobDB garbage collection.");

DEFINE_bool(blob_db_is_fifo, false, "Enable FIFO eviction strategy in BlobDB.");

DEFINE_uint64(blob_db_max_db_size, 0,
              "Max size limit of the directory where blob files are stored.");

DEFINE_uint64(blob_db_max_ttl_range, 86400,
              "TTL range to generate BlobDB data (in seconds).");

DEFINE_uint64(blob_db_ttl_range_secs, 3600,
              "TTL bucket size to use when creating blob files.");

DEFINE_uint64(blob_db_min_blob_size, 0,
              "Smallest blob to store in a file. Blobs smaller than this "
              "will be inlined with the key in the LSM tree.");

DEFINE_uint64(blob_db_bytes_per_sync, 0, "Bytes to sync blob file at.");

DEFINE_uint64(blob_db_file_size, 256 * 1024 * 1024,
              "Target size of each blob file.");

#endif  // ROCKSDB_LITE

DEFINE_bool(report_bg_io_stats, false,
            "Measure times spents on I/Os while in compactions. ");

DEFINE_bool(use_stderr_info_logger, false,
            "Write info logs to stderr instead of to LOG file. ");

DEFINE_string(trace_file, "", "Trace workload to a file. ");

static enum rocksdb::CompressionType StringToCompressionType(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "none"))
    return rocksdb::kNoCompression;
  else if (!strcasecmp(ctype, "snappy"))
    return rocksdb::kSnappyCompression;
  else if (!strcasecmp(ctype, "zlib"))
    return rocksdb::kZlibCompression;
  else if (!strcasecmp(ctype, "bzip2"))
    return rocksdb::kBZip2Compression;
  else if (!strcasecmp(ctype, "lz4"))
    return rocksdb::kLZ4Compression;
  else if (!strcasecmp(ctype, "lz4hc"))
    return rocksdb::kLZ4HCCompression;
  else if (!strcasecmp(ctype, "xpress"))
    return rocksdb::kXpressCompression;
  else if (!strcasecmp(ctype, "zstd"))
    return rocksdb::kZSTD;

  fprintf(stdout, "Cannot parse compression type '%s'\n", ctype);
  return rocksdb::kSnappyCompression;  // default value
}

static std::string ColumnFamilyName(size_t i) {
  if (i == 0) {
    return rocksdb::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");
static enum rocksdb::CompressionType FLAGS_compression_type_e =
    rocksdb::kSnappyCompression;

DEFINE_int32(compression_level, rocksdb::CompressionOptions().level,
             "Compression level. The meaning of this value is library-"
             "dependent. If unset, we try to use the default for the library "
             "specified in `--compression_type`");

DEFINE_int32(compression_max_dict_bytes,
             rocksdb::CompressionOptions().max_dict_bytes,
             "Maximum size of dictionary used to prime the compression "
             "library.");

DEFINE_int32(compression_zstd_max_train_bytes,
             rocksdb::CompressionOptions().zstd_max_train_bytes,
             "Maximum size of training data passed to zstd's dictionary "
             "trainer.");

DEFINE_int32(min_level_to_compress, -1, "If non-negative, compression starts"
             " from this level. Levels with number < min_level_to_compress are"
             " not compressed. Otherwise, apply compression_type to "
             "all levels.");

static bool ValidateTableCacheNumshardbits(const char* flagname,
                                           int32_t value) {
  if (0 >= value || value > 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be  0 < val <= 20\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(table_cache_numshardbits, 4, "");

#ifndef ROCKSDB_LITE
DEFINE_string(env_uri, "", "URI for registry Env lookup. Mutually exclusive"
              " with --hdfs.");
#endif  // ROCKSDB_LITE
DEFINE_string(hdfs, "", "Name of hdfs environment. Mutually exclusive with"
              " --env_uri.");
static rocksdb::Env* FLAGS_env = rocksdb::Env::Default();

DEFINE_int64(stats_interval, 0, "Stats are reported every N operations when "
             "this is greater than zero. When 0 the interval grows over time.");

DEFINE_int64(stats_interval_seconds, 0, "Report stats every N seconds. This "
             "overrides stats_interval when both are > 0.");

DEFINE_int32(stats_per_interval, 0, "Reports additional stats per interval when"
             " this is greater than 0.");

DEFINE_int64(report_interval_seconds, 0,
             "If greater than zero, it will write simple stats in CVS format "
             "to --report_file every N seconds");

DEFINE_string(report_file, "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");

DEFINE_int32(thread_status_per_interval, 0,
             "Takes and report a snapshot of the current status of each thread"
             " when this is greater than 0.");

DEFINE_int32(perf_level, rocksdb::PerfLevel::kDisable, "Level of perf collection");

static bool ValidateRateLimit(const char* flagname, double value) {
  const double EPSILON = 1e-10;
  if ( value < -EPSILON ) {
    fprintf(stderr, "Invalid value for --%s: %12.6f, must be >= 0.0\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_double(soft_rate_limit, 0.0, "DEPRECATED");

DEFINE_double(hard_rate_limit, 0.0, "DEPRECATED");

DEFINE_uint64(soft_pending_compaction_bytes_limit, 64ull * 1024 * 1024 * 1024,
              "Slowdown writes if pending compaction bytes exceed this number");

DEFINE_uint64(hard_pending_compaction_bytes_limit, 128ull * 1024 * 1024 * 1024,
              "Stop writes if pending compaction bytes exceed this number");

DEFINE_uint64(delayed_write_rate, 8388608u,
              "Limited bytes allowed to DB when soft_rate_limit or "
              "level0_slowdown_writes_trigger triggers");

DEFINE_bool(enable_pipelined_write, true,
            "Allow WAL and memtable writes to be pipelined");

DEFINE_bool(allow_concurrent_memtable_write, true,
            "Allow multi-writers to update mem tables in parallel.");

DEFINE_bool(inplace_update_support, rocksdb::Options().inplace_update_support,
            "Support in-place memtable update for smaller or same-size values");

DEFINE_uint64(inplace_update_num_locks,
              rocksdb::Options().inplace_update_num_locks,
              "Number of RW locks to protect in-place memtable updates");

DEFINE_bool(enable_write_thread_adaptive_yield, true,
            "Use a yielding spin loop for brief writer thread waits.");

DEFINE_uint64(
    write_thread_max_yield_usec, 100,
    "Maximum microseconds for enable_write_thread_adaptive_yield operation.");

DEFINE_uint64(write_thread_slow_yield_usec, 3,
              "The threshold at which a slow yield is considered a signal that "
              "other processes or threads want the core.");

DEFINE_int32(rate_limit_delay_max_milliseconds, 1000,
             "When hard_rate_limit is set then this is the max time a put will"
             " be stalled.");

DEFINE_uint64(rate_limiter_bytes_per_sec, 0, "Set options.rate_limiter value.");

DEFINE_bool(rate_limiter_auto_tuned, false,
            "Enable dynamic adjustment of rate limit according to demand for "
            "background I/O");


DEFINE_bool(sine_write_rate, false,
            "Use a sine wave write_rate_limit");

DEFINE_uint64(sine_write_rate_interval_milliseconds, 10000,
              "Interval of which the sine wave write_rate_limit is recalculated");

DEFINE_double(sine_a, 1,
             "A in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_b, 1,
             "B in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_c, 0,
             "C in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_d, 1,
             "D in f(x) = A sin(bx + c) + d");

DEFINE_bool(rate_limit_bg_reads, false,
            "Use options.rate_limiter on compaction reads");

DEFINE_uint64(
    benchmark_write_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the writes going into RocksDB. This "
    "is the global rate in bytes/second.");

// the parameters of mix_graph
DEFINE_double(key_dist_a, 0.0,
              "The parameter 'a' of key access distribution model "
              "f(x)=a*x^b");
DEFINE_double(key_dist_b, 0.0,
              "The parameter 'b' of key access distribution model "
              "f(x)=a*x^b");
DEFINE_double(value_theta, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(value_k, 0.0,
              "The parameter 'k' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(value_sigma, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_theta, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_k, 0.0,
              "The parameter 'k' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_sigma, 0.0,
              "The parameter 'sigma' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(mix_get_ratio, 1.0,
              "The ratio of Get queries of mix_graph workload");
DEFINE_double(mix_put_ratio, 0.0,
              "The ratio of Put queries of mix_graph workload");
DEFINE_double(mix_seek_ratio, 0.0,
              "The ratio of Seek queries of mix_graph workload");
DEFINE_int64(mix_max_scan_len, 10000, "The max scan length of Iterator");
DEFINE_int64(mix_ave_kv_size, 512,
             "The average key-value size of this workload");
DEFINE_int64(mix_max_value_size, 1024, "The max value size of this workload");
DEFINE_double(
    sine_mix_rate_noise, 0.0,
    "Add the noise ratio to the sine rate, it is between 0.0 and 1.0");
DEFINE_bool(sine_mix_rate, false,
            "Enable the sine QPS control on the mix workload");
DEFINE_uint64(
    sine_mix_rate_interval_milliseconds, 10000,
    "Interval of which the sine wave read_rate_limit is recalculated");
DEFINE_int64(mix_accesses, -1,
             "The total query accesses of mix_graph workload");

DEFINE_uint64(
    benchmark_read_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the reads from RocksDB. This "
    "is the global rate in ops/second.");

DEFINE_uint64(max_compaction_bytes, rocksdb::Options().max_compaction_bytes,
              "Max bytes allowed in one compaction");

#ifndef ROCKSDB_LITE
DEFINE_bool(readonly, false, "Run read only benchmarks.");

DEFINE_bool(print_malloc_stats, false,
            "Print malloc stats to stdout after benchmarks finish.");
#endif  // ROCKSDB_LITE

DEFINE_bool(disable_auto_compactions, false, "Do not auto trigger compactions");

DEFINE_uint64(wal_ttl_seconds, 0, "Set the TTL for the WAL Files in seconds.");
DEFINE_uint64(wal_size_limit_MB, 0, "Set the size limit for the WAL Files"
              " in MB.");
DEFINE_uint64(max_total_wal_size, 0, "Set total max WAL size");

DEFINE_bool(mmap_read, rocksdb::Options().allow_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, rocksdb::Options().allow_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(use_direct_reads, rocksdb::Options().use_direct_reads,
            "Use O_DIRECT for reading data");

DEFINE_bool(use_direct_io_for_flush_and_compaction,
            rocksdb::Options().use_direct_io_for_flush_and_compaction,
            "Use O_DIRECT for background flush and compaction writes");

DEFINE_bool(advise_random_on_open, rocksdb::Options().advise_random_on_open,
            "Advise random access on table file open");

DEFINE_string(compaction_fadvice, "NORMAL",
              "Access pattern advice when a file is compacted");
static auto FLAGS_compaction_fadvice_e =
  rocksdb::Options().access_hint_on_compaction_start;

DEFINE_bool(use_tailing_iterator, false,
            "Use tailing iterator to access a series of keys instead of get");

DEFINE_bool(use_adaptive_mutex, rocksdb::Options().use_adaptive_mutex,
            "Use adaptive mutex");

DEFINE_uint64(bytes_per_sync,  rocksdb::Options().bytes_per_sync,
              "Allows OS to incrementally sync SST files to disk while they are"
              " being written, in the background. Issue one request for every"
              " bytes_per_sync written. 0 turns it off.");

DEFINE_uint64(wal_bytes_per_sync,  rocksdb::Options().wal_bytes_per_sync,
              "Allows OS to incrementally sync WAL files to disk while they are"
              " being written, in the background. Issue one request for every"
              " wal_bytes_per_sync written. 0 turns it off.");

DEFINE_bool(use_single_deletes, true,
            "Use single deletes (used in RandomReplaceKeys only).");

DEFINE_double(stddev, 2000.0,
              "Standard deviation of normal distribution used for picking keys"
              " (used in RandomReplaceKeys only).");

DEFINE_int32(key_id_range, 100000,
             "Range of possible value of key id (used in TimeSeries only).");

DEFINE_string(expire_style, "none",
              "Style to remove expired time entries. Can be one of the options "
              "below: none (do not expired data), compaction_filter (use a "
              "compaction filter to remove expired data), delete (seek IDs and "
              "remove expired data) (used in TimeSeries only).");

DEFINE_uint64(
    time_range, 100000,
    "Range of timestamp that store in the database (used in TimeSeries"
    " only).");

DEFINE_int32(num_deletion_threads, 1,
             "Number of threads to do deletion (used in TimeSeries and delete "
             "expire_style only).");

DEFINE_int32(max_successive_merges, 0, "Maximum number of successive merge"
             " operations on a key in the memtable");

static bool ValidatePrefixSize(const char* flagname, int32_t value) {
  if (value < 0 || value>=2000000000) {
    fprintf(stderr, "Invalid value for --%s: %d. 0<= PrefixSize <=2000000000\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(prefix_size, 0, "control the prefix size for HashSkipList and "
             "plain table");
DEFINE_int64(keys_per_prefix, 0, "control average number of keys generated "
             "per prefix, 0 means no special handling of the prefix, "
             "i.e. use the prefix comes with the generated random number.");
DEFINE_int32(memtable_insert_with_hint_prefix_size, 0,
             "If non-zero, enable "
             "memtable insert with hint with the given prefix size.");
DEFINE_bool(enable_io_prio, false, "Lower the background flush/compaction "
            "threads' IO priority");
DEFINE_bool(enable_cpu_prio, false, "Lower the background flush/compaction "
            "threads' CPU priority");
DEFINE_bool(identity_as_first_hash, false, "the first hash function of cuckoo "
            "table becomes an identity function. This is only valid when key "
            "is 8 bytes");
DEFINE_bool(dump_malloc_stats, true, "Dump malloc stats in LOG ");
DEFINE_uint64(stats_dump_period_sec, rocksdb::Options().stats_dump_period_sec,
              "Gap between printing stats to log in seconds");
DEFINE_uint64(stats_persist_period_sec,
              rocksdb::Options().stats_persist_period_sec,
              "Gap between persisting stats in seconds");
DEFINE_uint64(stats_history_buffer_size,
              rocksdb::Options().stats_history_buffer_size,
              "Max number of stats snapshots to keep in memory");

enum RepFactory {
  kSkipList,
  kPrefixHash,
  kVectorRep,
  kHashLinkedList,
};

static enum RepFactory StringToRepFactory(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "skip_list"))
    return kSkipList;
  else if (!strcasecmp(ctype, "prefix_hash"))
    return kPrefixHash;
  else if (!strcasecmp(ctype, "vector"))
    return kVectorRep;
  else if (!strcasecmp(ctype, "hash_linkedlist"))
    return kHashLinkedList;

  fprintf(stdout, "Cannot parse memreptable %s\n", ctype);
  return kSkipList;
}

static enum RepFactory FLAGS_rep_factory;
DEFINE_string(memtablerep, "skip_list", "");
DEFINE_int64(hash_bucket_count, 1024 * 1024, "hash bucket count");
DEFINE_bool(use_plain_table, false, "if use plain table "
            "instead of block-based table format");
DEFINE_bool(use_cuckoo_table, false, "if use cuckoo table format");
DEFINE_double(cuckoo_hash_ratio, 0.9, "Hash ratio for Cuckoo SST table.");
DEFINE_bool(use_hash_search, false, "if use kHashSearch "
            "instead of kBinarySearch. "
            "This is valid if only we use BlockTable");
DEFINE_bool(use_block_based_filter, false, "if use kBlockBasedFilter "
            "instead of kFullFilter for filter block. "
            "This is valid if only we use BlockTable");
DEFINE_string(merge_operator, "", "The merge operator to use with the database."
              "If a new merge operator is specified, be sure to use fresh"
              " database The possible merge operators are defined in"
              " utilities/merge_operators.h");
DEFINE_int32(skip_list_lookahead, 0, "Used with skip_list memtablerep; try "
             "linear search first for this many steps from the previous "
             "position");
DEFINE_bool(report_file_operations, false, "if report number of file "
            "operations");

static const bool FLAGS_soft_rate_limit_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_soft_rate_limit, &ValidateRateLimit);

static const bool FLAGS_hard_rate_limit_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_hard_rate_limit, &ValidateRateLimit);

static const bool FLAGS_prefix_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

static const bool FLAGS_key_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_key_size, &ValidateKeySize);

static const bool FLAGS_cache_numshardbits_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_cache_numshardbits,
                          &ValidateCacheNumshardbits);

static const bool FLAGS_readwritepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_readwritepercent, &ValidateInt32Percent);

DEFINE_int32(disable_seek_compaction, false,
             "Not used, left here for backwards compatibility");

static const bool FLAGS_deletepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_deletepercent, &ValidateInt32Percent);
static const bool FLAGS_table_cache_numshardbits_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_table_cache_numshardbits,
                          &ValidateTableCacheNumshardbits);

namespace rocksdb {

namespace {
struct ReportFileOpCounters {
  std::atomic<int> open_counter_;
  std::atomic<int> read_counter_;
  std::atomic<int> append_counter_;
  std::atomic<uint64_t> bytes_read_;
  std::atomic<uint64_t> bytes_written_;
};

// A special Env to records and report file operations in db_bench
class ReportFileOpEnv : public EnvWrapper {
 public:
  explicit ReportFileOpEnv(Env* base) : EnvWrapper(base) { reset(); }

  void reset() {
    counters_.open_counter_ = 0;
    counters_.read_counter_ = 0;
    counters_.append_counter_ = 0;
    counters_.bytes_read_ = 0;
    counters_.bytes_written_ = 0;
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     private:
      std::unique_ptr<SequentialFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(unique_ptr<SequentialFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Read(size_t n, Slice* result, char* scratch) override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }

      Status Skip(uint64_t n) override { return target_->Skip(n); }
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     private:
      std::unique_ptr<RandomAccessFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(unique_ptr<RandomAccessFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(offset, n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class CountingFile : public WritableFile {
     private:
      std::unique_ptr<WritableFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(unique_ptr<WritableFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Append(const Slice& data) override {
        counters_->append_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Append(data);
        counters_->bytes_written_.fetch_add(data.size(),
                                            std::memory_order_relaxed);
        return rv;
      }

      Status Truncate(uint64_t size) override { return target_->Truncate(size); }
      Status Close() override { return target_->Close(); }
      Status Flush() override { return target_->Flush(); }
      Status Sync() override { return target_->Sync(); }
    };

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  // getter
  ReportFileOpCounters* counters() { return &counters_; }

 private:
  ReportFileOpCounters counters_;
};

}  // namespace

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  unsigned int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < (unsigned)std::max(1048576, FLAGS_value_size)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }

  Slice GenerateWithTTL(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

struct DBWithColumnFamilies {
  std::vector<ColumnFamilyHandle*> cfh;
  DB* db;
#ifndef ROCKSDB_LITE
  OptimisticTransactionDB* opt_txn_db;
#endif  // ROCKSDB_LITE
  std::atomic<size_t> num_created;  // Need to be updated after all the
                                    // new entries in cfh are set.
  size_t num_hot;  // Number of column families to be queried at each moment.
                   // After each CreateNewCf(), another num_hot number of new
                   // Column families will be created and used to be queried.
  port::Mutex create_cf_mutex;  // Only one thread can execute CreateNewCf()
  std::vector<int> cfh_idx_to_prob;  // ith index holds probability of operating
                                     // on cfh[i].

  DBWithColumnFamilies()
      : db(nullptr)
#ifndef ROCKSDB_LITE
        , opt_txn_db(nullptr)
#endif  // ROCKSDB_LITE
  {
    cfh.clear();
    num_created = 0;
    num_hot = 0;
  }

  DBWithColumnFamilies(const DBWithColumnFamilies& other)
      : cfh(other.cfh),
        db(other.db),
#ifndef ROCKSDB_LITE
        opt_txn_db(other.opt_txn_db),
#endif  // ROCKSDB_LITE
        num_created(other.num_created.load()),
        num_hot(other.num_hot),
        cfh_idx_to_prob(other.cfh_idx_to_prob) {
  }

  void DeleteDBs() {
    std::for_each(cfh.begin(), cfh.end(),
                  [](ColumnFamilyHandle* cfhi) { delete cfhi; });
    cfh.clear();
#ifndef ROCKSDB_LITE
    if (opt_txn_db) {
      delete opt_txn_db;
      opt_txn_db = nullptr;
    } else {
      delete db;
      db = nullptr;
    }
#else
    delete db;
    db = nullptr;
#endif  // ROCKSDB_LITE
  }

  ColumnFamilyHandle* GetCfh(int64_t rand_num) {
    assert(num_hot > 0);
    size_t rand_offset = 0;
    if (!cfh_idx_to_prob.empty()) {
      assert(cfh_idx_to_prob.size() == num_hot);
      int sum = 0;
      while (sum + cfh_idx_to_prob[rand_offset] < rand_num % 100) {
        sum += cfh_idx_to_prob[rand_offset];
        ++rand_offset;
      }
      assert(rand_offset < cfh_idx_to_prob.size());
    } else {
      rand_offset = rand_num % num_hot;
    }
    return cfh[num_created.load(std::memory_order_acquire) - num_hot +
               rand_offset];
  }

  // stage: assume CF from 0 to stage * num_hot has be created. Need to create
  //        stage * num_hot + 1 to stage * (num_hot + 1).
  void CreateNewCf(ColumnFamilyOptions options, int64_t stage) {
    MutexLock l(&create_cf_mutex);
    if ((stage + 1) * num_hot <= num_created) {
      // Already created.
      return;
    }
    auto new_num_created = num_created + num_hot;
    assert(new_num_created <= cfh.size());
    for (size_t i = num_created; i < new_num_created; i++) {
      Status s =
          db->CreateColumnFamily(options, ColumnFamilyName(i), &(cfh[i]));
      if (!s.ok()) {
        fprintf(stderr, "create column family error: %s\n",
                s.ToString().c_str());
        abort();
      }
    }
    num_created.store(new_num_created, std::memory_order_release);
  }
};

// a class that reports stats to CSV file
class ReporterAgent {
 public:
  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs)
      : env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());
    if (s.ok()) {
      s = report_file_->Append(Header() + "\n");
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }

    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }

  ~ReporterAgent() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
  }

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

 private:
  std::string Header() const { return "secs_elapsed,interval_qps"; }
  void SleepAndReport() {
    uint64_t kMicrosInSecond = 1000 * 1000;
    auto time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      std::string report = ToString(secs_elapsed) + "," +
                           ToString(total_ops_done_snapshot - last_report_) +
                           "\n";
      auto s = report_file_->Append(report);
      if (s.ok()) {
        s = report_file_->Flush();
      }
      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }

  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  rocksdb::port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
                          OperationTypeString = {
  {kRead, "read"},
  {kWrite, "write"},
  {kDelete, "delete"},
  {kSeek, "seek"},
  {kMerge, "merge"},
  {kUpdate, "update"},
  {kCompress, "compress"},
  {kCompress, "uncompress"},
  {kCrc, "crc"},
  {kHash, "hash"},
  {kOthers, "op"}
};

class CombinedStats;
class Stats {
 private:
  int id_;
  uint64_t start_;
  uint64_t sine_interval_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>> hist_;
  std::string message_;
  bool exclude_from_merge_;
  ReporterAgent* reporter_agent_;  // does not own
  friend class CombinedStats;

 public:
  Stats() { Start(-1); }

  void SetReporterAgent(ReporterAgent* reporter_agent) {
    reporter_agent_ = reporter_agent;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = FLAGS_env->NowMicros();
    sine_interval_ = FLAGS_env->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_)
      return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({ it->first, it->second });
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = FLAGS_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    FLAGS_env->GetThreadList(&thread_list);

    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n",
        "ThreadID", "ThreadType", "cfName", "Operation",
        "ElapsedTime", "Stage", "State", "OperationProperties");

    int64_t current_time = 0;
    Env::Default()->GetCurrentTime(&current_time);
    for (auto ts : thread_list) {
      fprintf(stderr, "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
          ts.thread_id,
          ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
          ts.cf_name.c_str(),
          ThreadStatus::GetOperationName(ts.operation_type).c_str(),
          ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
          ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
          ThreadStatus::GetStateName(ts.state_type).c_str());

      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64" |",
            op_prop.first.c_str(), op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }

  void ResetSineInterval() {
    sine_interval_ = FLAGS_env->NowMicros();
  }

  uint64_t GetSineInterval() {
    return sine_interval_;
  }

  uint64_t GetStart() {
    return start_;
  }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = FLAGS_env->NowMicros();
  }

  void FinishedOps(DBWithColumnFamilies* db_with_cfh, DB* db, int64_t num_ops,
                   enum OperationType op_type = kOthers) {
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }
    if (FLAGS_histogram) {
      uint64_t now = FLAGS_env->NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (hist_.find(op_type) == hist_.end())
      {
        auto hist_temp = std::make_shared<HistogramImpl>();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros > 20000 && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!FLAGS_stats_interval) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = FLAGS_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += FLAGS_stats_interval;

        } else {

          fprintf(stderr,
                  "%s ... thread %d: (%" PRIu64 ",%" PRIu64 ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  FLAGS_env->TimeToString(now/1000000).c_str(),
                  id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) /
                  (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          if (id_ == 0 && FLAGS_stats_per_interval) {
            std::string stats;

            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats))
                  fprintf(stderr, "%s\n", stats.c_str());
                if (FLAGS_show_table_properties) {
                  for (int level = 0; level < FLAGS_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                ToString(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s\n", stats.c_str());
              }
              if (FLAGS_show_table_properties) {
                for (int level = 0; level < FLAGS_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              ToString(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      if (id_ == 0 && FLAGS_thread_status_per_interval) {
        PrintThreadStatus();
      }
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_/elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (long)throughput,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stdout, "Microseconds per %s:\n%s\n",
                OperationTypeString[it->first].c_str(),
                it->second->ToString().c_str());
      }
    }
    if (FLAGS_report_file_operations) {
      ReportFileOpEnv* env = static_cast<ReportFileOpEnv*>(FLAGS_env);
      ReportFileOpCounters* counters = env->counters();
      fprintf(stdout, "Num files opened: %d\n",
              counters->open_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Read(): %d\n",
              counters->read_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Append(): %d\n",
              counters->append_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes read: %" PRIu64 "\n",
              counters->bytes_read_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes written: %" PRIu64 "\n",
              counters->bytes_written_.load(std::memory_order_relaxed));
      env->reset();
    }
    fflush(stdout);
  }
};

class CombinedStats {
 public:
  void AddStats(const Stats& stat) {
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
      total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
      double mbs = (total_bytes_ / 1048576.0);
      throughput_mbs_.emplace_back(mbs / elapsed);
    }
  }

  void Report(const std::string& bench_name) {
    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              CalcAvg(throughput_mbs_), name, num_runs,
              static_cast<int>(CalcMedian(throughput_ops_)),
              CalcMedian(throughput_mbs_));
    } else {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)));
    }
  }

 private:
  double CalcAvg(std::vector<double> data) {
    double avg = 0;
    for (double x : data) {
      avg += x;
    }
    avg = avg / data.size();
    return avg;
  }

  double CalcMedian(std::vector<double> data) {
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
      // Odd number of entries
      return data[mid];
    } else {
      // Even number of entries
      return (data[mid] + data[mid - 1]) / 2;
    }
  }

  std::vector<double> throughput_ops_;
  std::vector<double> throughput_mbs_;
};

class TimestampEmulator {
 private:
  std::atomic<uint64_t> timestamp_;

 public:
  TimestampEmulator() : timestamp_(0) {}
  uint64_t Get() const { return timestamp_.load(); }
  void Inc() { timestamp_++; }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
  int perf_level;
  std::shared_ptr<RateLimiter> write_rate_limiter;
  std::shared_ptr<RateLimiter> read_rate_limiter;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;

  SharedState() : cv(&mu), perf_level(FLAGS_perf_level) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random64 rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  /* implicit */ ThreadState(int index)
      : tid(index),
        rand((FLAGS_seed ? FLAGS_seed : 1000) + index) {
  }
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = FLAGS_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = FLAGS_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  const SliceTransform* prefix_extractor_;
  DBWithColumnFamilies db_;
  std::vector<DBWithColumnFamilies> multi_dbs_;
  int64_t num_;
  int value_size_;
  int key_size_;
  int prefix_size_;
  int64_t keys_per_prefix_;
  int64_t entries_per_batch_;
  int64_t writes_before_delete_range_;
  int64_t writes_per_range_tombstone_;
  int64_t range_tombstone_width_;
  int64_t max_num_range_tombstones_;
  WriteOptions write_options_;
  Options open_options_;  // keep options around to properly destroy db later
#ifndef ROCKSDB_LITE
  TraceOptions trace_options_;
#endif
  int64_t reads_;
  int64_t deletes_;
  double read_random_exp_range_;
  int64_t writes_;
  int64_t readwrites_;
  int64_t merge_keys_;
  bool report_file_operations_;
  bool use_blob_db_;

  class ErrorHandlerListener : public EventListener {
   public:
#ifndef ROCKSDB_LITE
    ErrorHandlerListener()
        : mutex_(),
          cv_(&mutex_),
          no_auto_recovery_(false),
          recovery_complete_(false) {}

    ~ErrorHandlerListener() override {}

    void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                              Status /*bg_error*/,
                              bool* auto_recovery) override {
      if (*auto_recovery && no_auto_recovery_) {
        *auto_recovery = false;
      }
    }

    void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
      InstrumentedMutexLock l(&mutex_);
      recovery_complete_ = true;
      cv_.SignalAll();
    }

    bool WaitForRecovery(uint64_t /*abs_time_us*/) {
      InstrumentedMutexLock l(&mutex_);
      if (!recovery_complete_) {
        cv_.Wait(/*abs_time_us*/);
      }
      if (recovery_complete_) {
        recovery_complete_ = false;
        return true;
      }
      return false;
    }

    void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

   private:
    InstrumentedMutex mutex_;
    InstrumentedCondVar cv_;
    bool no_auto_recovery_;
    bool recovery_complete_;
#else   // ROCKSDB_LITE
    bool WaitForRecovery(uint64_t /*abs_time_us*/) { return true; }
    void EnableAutoRecovery(bool /*enable*/) {}
#endif  // ROCKSDB_LITE
  };

  std::shared_ptr<ErrorHandlerListener> listener_;

  bool SanityCheck() {
    if (FLAGS_compression_ratio > 1) {
      fprintf(stderr, "compression_ratio should be between 0 and 1\n");
      return false;
    }
    return true;
  }

  inline bool CompressSlice(const CompressionInfo& compression_info,
                            const Slice& input, std::string* compressed) {
    bool ok = true;
    switch (FLAGS_compression_type_e) {
      case rocksdb::kSnappyCompression:
        ok = Snappy_Compress(compression_info, input.data(), input.size(),
                             compressed);
        break;
      case rocksdb::kZlibCompression:
        ok = Zlib_Compress(compression_info, 2, input.data(), input.size(),
                           compressed);
        break;
      case rocksdb::kBZip2Compression:
        ok = BZip2_Compress(compression_info, 2, input.data(), input.size(),
                            compressed);
        break;
      case rocksdb::kLZ4Compression:
        ok = LZ4_Compress(compression_info, 2, input.data(), input.size(),
                          compressed);
        break;
      case rocksdb::kLZ4HCCompression:
        ok = LZ4HC_Compress(compression_info, 2, input.data(), input.size(),
                            compressed);
        break;
      case rocksdb::kXpressCompression:
        ok = XPRESS_Compress(input.data(),
          input.size(), compressed);
        break;
      case rocksdb::kZSTD:
        ok = ZSTD_Compress(compression_info, input.data(), input.size(),
                           compressed);
        break;
      default:
        ok = false;
    }
    return ok;
  }

  void PrintHeader() {
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", FLAGS_key_size);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stdout, "Prefix:    %d bytes\n", FLAGS_prefix_size);
    fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_key_size + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((FLAGS_key_size + FLAGS_value_size * FLAGS_compression_ratio)
              * num_)
             / 1048576.0));
    fprintf(stdout, "Write rate: %" PRIu64 " bytes/second\n",
            FLAGS_benchmark_write_rate_limit);
    fprintf(stdout, "Read rate: %" PRIu64 " ops/second\n",
            FLAGS_benchmark_read_rate_limit);
    if (FLAGS_enable_numa) {
      fprintf(stderr, "Running in NUMA enabled mode.\n");
#ifndef NUMA
      fprintf(stderr, "NUMA is not defined in the system.\n");
      exit(1);
#else
      if (numa_available() == -1) {
        fprintf(stderr, "NUMA is not supported by the system.\n");
        exit(1);
      }
#endif
    }

    auto compression = CompressionTypeToString(FLAGS_compression_type_e);
    fprintf(stdout, "Compression: %s\n", compression.c_str());

    switch (FLAGS_rep_factory) {
      case kPrefixHash:
        fprintf(stdout, "Memtablerep: prefix_hash\n");
        break;
      case kSkipList:
        fprintf(stdout, "Memtablerep: skip_list\n");
        break;
      case kVectorRep:
        fprintf(stdout, "Memtablerep: vector\n");
        break;
      case kHashLinkedList:
        fprintf(stdout, "Memtablerep: hash_linkedlist\n");
        break;
    }
    fprintf(stdout, "Perf Level: %d\n", FLAGS_perf_level);

    PrintWarnings(compression.c_str());
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings(const char* compression) {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    if (FLAGS_compression_type_e != rocksdb::kNoCompression) {
      // The test string should not be too small.
      const int len = FLAGS_block_size;
      std::string input_str(len, 'y');
      std::string compressed;
      CompressionOptions opts;
      CompressionContext context(FLAGS_compression_type_e);
      CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                           FLAGS_compression_type_e);
      bool result = CompressSlice(info, Slice(input_str), &compressed);

      if (!result) {
        fprintf(stdout, "WARNING: %s compression is not enabled\n",
                compression);
      } else if (compressed.size() >= input_str.size()) {
        fprintf(stdout, "WARNING: %s compression is not effective\n",
                compression);
      }
    }
  }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static Slice TrimSpace(Slice s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit-1])) {
      limit--;
    }
    return Slice(s.data() + start, limit - start);
  }
#endif

  void PrintEnvironment() {
    fprintf(stderr, "RocksDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(nullptr);
    char buf[52];
    // Lint complains about ctime() usage, so replace it with ctime_r(). The
    // requirement is to provide a buffer which is at least 26 bytes.
    fprintf(stderr, "Date:       %s",
            ctime_r(&now, buf));  // ctime_r() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

  static bool KeyExpired(const TimestampEmulator* timestamp_emulator,
                         const Slice& key) {
    const char* pos = key.data();
    pos += 8;
    uint64_t timestamp = 0;
    if (port::kLittleEndian) {
      int bytes_to_fill = 8;
      for (int i = 0; i < bytes_to_fill; ++i) {
        timestamp |= (static_cast<uint64_t>(static_cast<unsigned char>(pos[i]))
                      << ((bytes_to_fill - i - 1) << 3));
      }
    } else {
      memcpy(&timestamp, pos, sizeof(timestamp));
    }
    return timestamp_emulator->Get() - timestamp > FLAGS_time_range;
  }

  class ExpiredTimeFilter : public CompactionFilter {
   public:
    explicit ExpiredTimeFilter(
        const std::shared_ptr<TimestampEmulator>& timestamp_emulator)
        : timestamp_emulator_(timestamp_emulator) {}
    bool Filter(int /*level*/, const Slice& key,
                const Slice& /*existing_value*/, std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return KeyExpired(timestamp_emulator_.get(), key);
    }
    const char* Name() const override { return "ExpiredTimeFilter"; }

   private:
    std::shared_ptr<TimestampEmulator> timestamp_emulator_;
  };

  class KeepFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return false;
    }

    const char* Name() const override { return "KeepFilter"; }
  };

  std::shared_ptr<Cache> NewCache(int64_t capacity) {
    if (capacity <= 0) {
      return nullptr;
    }
    if (FLAGS_use_clock_cache) {
      auto cache = NewClockCache((size_t)capacity, FLAGS_cache_numshardbits);
      if (!cache) {
        fprintf(stderr, "Clock cache not supported.");
        exit(1);
      }
      return cache;
    } else {
      return NewLRUCache((size_t)capacity, FLAGS_cache_numshardbits,
                         false /*strict_capacity_limit*/,
                         FLAGS_cache_high_pri_pool_ratio);
    }
  }

 public:
  Benchmark()
      : cache_(NewCache(FLAGS_cache_size)),
        compressed_cache_(NewCache(FLAGS_compressed_cache_size)),
        filter_policy_(FLAGS_bloom_bits >= 0
                           ? NewBloomFilterPolicy(FLAGS_bloom_bits,
                                                  FLAGS_use_block_based_filter)
                           : nullptr),
        prefix_extractor_(NewFixedPrefixTransform(FLAGS_prefix_size)),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        key_size_(FLAGS_key_size),
        prefix_size_(FLAGS_prefix_size),
        keys_per_prefix_(FLAGS_keys_per_prefix),
        entries_per_batch_(1),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        read_random_exp_range_(0.0),
        writes_(FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes),
        readwrites_(
            (FLAGS_writes < 0 && FLAGS_reads < 0)
                ? FLAGS_num
                : ((FLAGS_writes > FLAGS_reads) ? FLAGS_writes : FLAGS_reads)),
        merge_keys_(FLAGS_merge_keys < 0 ? FLAGS_num : FLAGS_merge_keys),
        report_file_operations_(FLAGS_report_file_operations),
#ifndef ROCKSDB_LITE
        use_blob_db_(FLAGS_use_blob_db)
#else
        use_blob_db_(false)
#endif  // !ROCKSDB_LITE
  {
    // use simcache instead of cache
    if (FLAGS_simcache_size >= 0) {
      if (FLAGS_cache_numshardbits >= 1) {
        cache_ =
            NewSimCache(cache_, FLAGS_simcache_size, FLAGS_cache_numshardbits);
      } else {
        cache_ = NewSimCache(cache_, FLAGS_simcache_size, 0);
      }
    }

    if (report_file_operations_) {
      if (!FLAGS_hdfs.empty()) {
        fprintf(stderr,
                "--hdfs and --report_file_operations cannot be enabled "
                "at the same time");
        exit(1);
      }
      FLAGS_env = new ReportFileOpEnv(rocksdb::Env::Default());
    }

    if (FLAGS_prefix_size > FLAGS_key_size) {
      fprintf(stderr, "prefix size is larger than key size");
      exit(1);
    }

    std::vector<std::string> files;
    FLAGS_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        FLAGS_env->DeleteFile(FLAGS_db + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      Options options;
      if (!FLAGS_wal_dir.empty()) {
        options.wal_dir = FLAGS_wal_dir;
      }
#ifndef ROCKSDB_LITE
      if (use_blob_db_) {
        blob_db::DestroyBlobDB(FLAGS_db, options, blob_db::BlobDBOptions());
      }
#endif  // !ROCKSDB_LITE
      DestroyDB(FLAGS_db, options);
      if (!FLAGS_wal_dir.empty()) {
        FLAGS_env->DeleteDir(FLAGS_wal_dir);
      }

      if (FLAGS_num_multi_db > 1) {
        FLAGS_env->CreateDir(FLAGS_db);
        if (!FLAGS_wal_dir.empty()) {
          FLAGS_env->CreateDir(FLAGS_wal_dir);
        }
      }
    }

    listener_.reset(new ErrorHandlerListener());
  }

  ~Benchmark() {
    db_.DeleteDBs();
    delete prefix_extractor_;
    if (cache_.get() != nullptr) {
      // this will leak, but we're shutting down so nobody cares
      cache_->DisownData();
    }
  }

  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
  }

  // Generate key according to the given specification and random number.
  // The resulting key will have the following format (if keys_per_prefix_
  // is positive), extra trailing bytes are either cut off or padded with '0'.
  // The prefix value is derived from key value.
  //   ----------------------------
  //   | prefix 00000 | key 00000 |
  //   ----------------------------
  // If keys_per_prefix_ is 0, the key is simply a binary representation of
  // random number followed by trailing '0's
  //   ----------------------------
  //   |        key 00000         |
  //   ----------------------------
  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    if (keys_per_prefix_ > 0) {
      int64_t num_prefix = num_keys / keys_per_prefix_;
      int64_t prefix = v % num_prefix;
      int bytes_to_fill = std::min(prefix_size_, 8);
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
      }
      if (prefix_size_ > 8) {
        // fill the rest with 0s
        memset(pos + 8, '0', prefix_size_ - 8);
      }
      pos += prefix_size_;
    }

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
  }

  std::string GetPathForMultiple(std::string base_name, size_t id) {
    if (!base_name.empty()) {
#ifndef OS_WIN
      if (base_name.back() != '/') {
        base_name += '/';
      }
#else
      if (base_name.back() != '\\') {
        base_name += '\\';
      }
#endif
    }
    return base_name + ToString(id);
  }

void VerifyDBFromDB(std::string& truth_db_name) {
  DBWithColumnFamilies truth_db;
  auto s = DB::OpenForReadOnly(open_options_, truth_db_name, &truth_db.db);
  if (!s.ok()) {
    fprintf(stderr, "open error: %s\n", s.ToString().c_str());
    exit(1);
  }
  ReadOptions ro;
  ro.total_order_seek = true;
  std::unique_ptr<Iterator> truth_iter(truth_db.db->NewIterator(ro));
  std::unique_ptr<Iterator> db_iter(db_.db->NewIterator(ro));
  // Verify that all the key/values in truth_db are retrivable in db with ::Get
  fprintf(stderr, "Verifying db >= truth_db with ::Get...\n");
  for (truth_iter->SeekToFirst(); truth_iter->Valid(); truth_iter->Next()) {
      std::string value;
      s = db_.db->Get(ro, truth_iter->key(), &value);
      assert(s.ok());
      // TODO(myabandeh): provide debugging hints
      assert(Slice(value) == truth_iter->value());
  }
  // Verify that the db iterator does not give any extra key/value
  fprintf(stderr, "Verifying db == truth_db...\n");
  for (db_iter->SeekToFirst(), truth_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next(), truth_iter->Next()) {
    assert(truth_iter->Valid());
    assert(truth_iter->value() == db_iter->value());
  }
  // No more key should be left unchecked in truth_db
  assert(!truth_iter->Valid());
  fprintf(stderr, "...Verified\n");
}

  void Run() {
    if (!SanityCheck()) {
      exit(1);
    }
    Open(&open_options_);
    PrintHeader();
    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    std::unique_ptr<ExpiredTimeFilter> filter;
    while (std::getline(benchmark_stream, name, ',')) {
      // Sanitize parameters
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      writes_ = (FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes);
      deletes_ = (FLAGS_deletes < 0 ? FLAGS_num : FLAGS_deletes);
      value_size_ = FLAGS_value_size;
      key_size_ = FLAGS_key_size;
      entries_per_batch_ = FLAGS_batch_size;
      writes_before_delete_range_ = FLAGS_writes_before_delete_range;
      writes_per_range_tombstone_ = FLAGS_writes_per_range_tombstone;
      range_tombstone_width_ = FLAGS_range_tombstone_width;
      max_num_range_tombstones_ = FLAGS_max_num_range_tombstones;
      write_options_ = WriteOptions();
      read_random_exp_range_ = FLAGS_read_random_exp_range;
      if (FLAGS_sync) {
        write_options_.sync = true;
      }
      write_options_.disableWAL = FLAGS_disable_wal;

      void (Benchmark::*method)(ThreadState*) = nullptr;
      void (Benchmark::*post_process_method)() = nullptr;

      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      int num_repeat = 1;
      int num_warmup = 0;
      if (!name.empty() && *name.rbegin() == ']') {
        auto it = name.find('[');
        if (it == std::string::npos) {
          fprintf(stderr, "unknown benchmark arguments '%s'\n", name.c_str());
          exit(1);
        }
        std::string args = name.substr(it + 1);
        args.resize(args.size() - 1);
        name.resize(it);

        std::string bench_arg;
        std::stringstream args_stream(args);
        while (std::getline(args_stream, bench_arg, '-')) {
          if (bench_arg.empty()) {
            continue;
          }
          if (bench_arg[0] == 'X') {
            // Repeat the benchmark n times
            std::string num_str = bench_arg.substr(1);
            num_repeat = std::stoi(num_str);
          } else if (bench_arg[0] == 'W') {
            // Warm up the benchmark for n times
            std::string num_str = bench_arg.substr(1);
            num_warmup = std::stoi(num_str);
          }
        }
      }

      // Both fillseqdeterministic and filluniquerandomdeterministic
      // fill the levels except the max level with UNIQUE_RANDOM
      // and fill the max level with fillseq and filluniquerandom, respectively
      if (name == "fillseqdeterministic" ||
          name == "filluniquerandomdeterministic") {
        if (!FLAGS_disable_auto_compactions) {
          fprintf(stderr,
                  "Please disable_auto_compactions in FillDeterministic "
                  "benchmark\n");
          exit(1);
        }
        if (num_threads > 1) {
          fprintf(stderr,
                  "filldeterministic multithreaded not supported"
                  ", use 1 thread\n");
          num_threads = 1;
        }
        fresh_db = true;
        if (name == "fillseqdeterministic") {
          method = &Benchmark::WriteSeqDeterministic;
        } else {
          method = &Benchmark::WriteUniqueRandomDeterministic;
        }
      } else if (name == "fillseq") {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillbatch") {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillrandom") {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "filluniquerandom") {
        fresh_db = true;
        if (num_threads > 1) {
          fprintf(stderr,
                  "filluniquerandom multithreaded not supported"
                  ", use 1 thread");
          num_threads = 1;
        }
        method = &Benchmark::WriteUniqueRandom;
      } else if (name == "overwrite") {
        method = &Benchmark::WriteRandom;
      } else if (name == "fillsync") {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "fill100K") {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == "readseq") {
        method = &Benchmark::ReadSequential;
      } else if (name == "readtocache") {
        method = &Benchmark::ReadSequential;
        num_threads = 1;
        reads_ = num_;
      } else if (name == "readreverse") {
        method = &Benchmark::ReadReverse;
      } else if (name == "readrandom") {
        method = &Benchmark::ReadRandom;
      } else if (name == "readrandomfast") {
        method = &Benchmark::ReadRandomFast;
      } else if (name == "multireadrandom") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        method = &Benchmark::MultiReadRandom;
      } else if (name == "mixgraph") {
        method = &Benchmark::MixGraph;
      } else if (name == "readmissing") {
        ++key_size_;
        method = &Benchmark::ReadRandom;
      } else if (name == "newiterator") {
        method = &Benchmark::IteratorCreation;
      } else if (name == "newiteratorwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::IteratorCreationWhileWriting;
      } else if (name == "seekrandom") {
        method = &Benchmark::SeekRandom;
      } else if (name == "seekrandomwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::SeekRandomWhileWriting;
      } else if (name == "seekrandomwhilemerging") {
        num_threads++;  // Add extra thread for merging
        method = &Benchmark::SeekRandomWhileMerging;
      } else if (name == "readrandomsmall") {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == "deleteseq") {
        method = &Benchmark::DeleteSeq;
      } else if (name == "deleterandom") {
        method = &Benchmark::DeleteRandom;
      } else if (name == "readwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == "readwhilemerging") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileMerging;
      } else if (name == "readwhilescanning") {
        num_threads++;  // Add extra thread for scaning
        method = &Benchmark::ReadWhileScanning;
      } else if (name == "readrandomwriterandom") {
        method = &Benchmark::ReadRandomWriteRandom;
      } else if (name == "readrandommergerandom") {
        if (FLAGS_merge_operator.empty()) {
          fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                  name.c_str());
          exit(1);
        }
        method = &Benchmark::ReadRandomMergeRandom;
      } else if (name == "updaterandom") {
        method = &Benchmark::UpdateRandom;
      } else if (name == "xorupdaterandom") {
        method = &Benchmark::XORUpdateRandom;
      } else if (name == "appendrandom") {
        method = &Benchmark::AppendRandom;
      } else if (name == "mergerandom") {
        if (FLAGS_merge_operator.empty()) {
          fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                  name.c_str());
          exit(1);
        }
        method = &Benchmark::MergeRandom;
      } else if (name == "randomwithverify") {
        method = &Benchmark::RandomWithVerify;
      } else if (name == "fillseekseq") {
        method = &Benchmark::WriteSeqSeekSeq;
      } else if (name == "compact") {
        method = &Benchmark::Compact;
      } else if (name == "compactall") {
        CompactAll();
      } else if (name == "crc32c") {
        method = &Benchmark::Crc32c;
      } else if (name == "xxhash") {
        method = &Benchmark::xxHash;
      } else if (name == "acquireload") {
        method = &Benchmark::AcquireLoad;
      } else if (name == "compress") {
        method = &Benchmark::Compress;
      } else if (name == "uncompress") {
        method = &Benchmark::Uncompress;
#ifndef ROCKSDB_LITE
      } else if (name == "randomtransaction") {
        method = &Benchmark::RandomTransaction;
        post_process_method = &Benchmark::RandomTransactionVerify;
#endif  // ROCKSDB_LITE
      } else if (name == "randomreplacekeys") {
        fresh_db = true;
        method = &Benchmark::RandomReplaceKeys;
      } else if (name == "timeseries") {
        timestamp_emulator_.reset(new TimestampEmulator());
        if (FLAGS_expire_style == "compaction_filter") {
          filter.reset(new ExpiredTimeFilter(timestamp_emulator_));
          fprintf(stdout, "Compaction filter is used to remove expired data");
          open_options_.compaction_filter = filter.get();
        }
        fresh_db = true;
        method = &Benchmark::TimeSeries;
      } else if (name == "stats") {
        PrintStats("rocksdb.stats");
      } else if (name == "resetstats") {
        ResetStats();
      } else if (name == "verify") {
        VerifyDBFromDB(FLAGS_truth_db);
      } else if (name == "levelstats") {
        PrintStats("rocksdb.levelstats");
      } else if (name == "sstables") {
        PrintStats("rocksdb.sstables");
      } else if (name == "replay") {
        if (num_threads > 1) {
          fprintf(stderr, "Multi-threaded replay is not yet supported\n");
          exit(1);
        }
        if (FLAGS_trace_file == "") {
          fprintf(stderr, "Please set --trace_file to be replayed from\n");
          exit(1);
        }
        method = &Benchmark::Replay;
      } else if (!name.empty()) {  // No error message for empty name
        fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
        exit(1);
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.c_str());
          method = nullptr;
        } else {
          if (db_.db != nullptr) {
            db_.DeleteDBs();
            DestroyDB(FLAGS_db, open_options_);
          }
          Options options = open_options_;
          for (size_t i = 0; i < multi_dbs_.size(); i++) {
            delete multi_dbs_[i].db;
            if (!open_options_.wal_dir.empty()) {
              options.wal_dir = GetPathForMultiple(open_options_.wal_dir, i);
            }
            DestroyDB(GetPathForMultiple(FLAGS_db, i), options);
          }
          multi_dbs_.clear();
        }
        Open(&open_options_);  // use open_options for the last accessed
      }

      if (method != nullptr) {
        fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());

#ifndef ROCKSDB_LITE
        // A trace_file option can be provided both for trace and replay
        // operations. But db_bench does not support tracing and replaying at
        // the same time, for now. So, start tracing only when it is not a
        // replay.
        if (FLAGS_trace_file != "" && name != "replay") {
          std::unique_ptr<TraceWriter> trace_writer;
          Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(),
                                        FLAGS_trace_file, &trace_writer);
          if (!s.ok()) {
            fprintf(stderr, "Encountered an error starting a trace, %s\n",
                    s.ToString().c_str());
            exit(1);
          }
          s = db_.db->StartTrace(trace_options_, std::move(trace_writer));
          if (!s.ok()) {
            fprintf(stderr, "Encountered an error starting a trace, %s\n",
                    s.ToString().c_str());
            exit(1);
          }
          fprintf(stdout, "Tracing the workload to: [%s]\n",
                  FLAGS_trace_file.c_str());
        }
#endif  // ROCKSDB_LITE

        if (num_warmup > 0) {
          printf("Warming up benchmark by running %d times\n", num_warmup);
        }

        for (int i = 0; i < num_warmup; i++) {
          RunBenchmark(num_threads, name, method);
        }

        if (num_repeat > 1) {
          printf("Running benchmark for %d times\n", num_repeat);
        }

        CombinedStats combined_stats;
        for (int i = 0; i < num_repeat; i++) {
          Stats stats = RunBenchmark(num_threads, name, method);
          combined_stats.AddStats(stats);
        }
        if (num_repeat > 1) {
          combined_stats.Report(name);
        }
      }
      if (post_process_method != nullptr) {
        (this->*post_process_method)();
      }
    }

#ifndef ROCKSDB_LITE
    if (name != "replay" && FLAGS_trace_file != "") {
      Status s = db_.db->EndTrace();
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error ending the trace, %s\n",
                s.ToString().c_str());
      }
    }
#endif  // ROCKSDB_LITE

    if (FLAGS_statistics) {
      fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
    }
    if (FLAGS_simcache_size >= 0) {
      fprintf(stdout, "SIMULATOR CACHE STATISTICS:\n%s\n",
              static_cast_with_check<SimCache, Cache>(cache_.get())
                  ->ToString()
                  .c_str());
    }
  }

 private:
  std::shared_ptr<TimestampEmulator> timestamp_emulator_;

  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    SetPerfLevel(static_cast<PerfLevel> (shared->perf_level));
    perf_context.EnablePerLevelPerfContext();
    thread->stats.Start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  Stats RunBenchmark(int n, Slice name,
                     void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      shared.write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }
    if (FLAGS_benchmark_read_rate_limit > 0) {
      shared.read_rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_benchmark_read_rate_limit, 100000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kReadsOnly));
    }

    std::unique_ptr<ReporterAgent> reporter_agent;
    if (FLAGS_report_interval_seconds > 0) {
      reporter_agent.reset(new ReporterAgent(FLAGS_env, FLAGS_report_file,
                                             FLAGS_report_interval_seconds));
    }

    ThreadArg* arg = new ThreadArg[n];

    for (int i = 0; i < n; i++) {
#ifdef NUMA
      if (FLAGS_enable_numa) {
        // Performs a local allocation of memory to threads in numa node.
        int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
        numa_exit_on_error = 1;
        int numa_node = i % n_nodes;
        bitmask* nodes = numa_allocate_nodemask();
        numa_bitmask_clearall(nodes);
        numa_bitmask_setbit(nodes, numa_node);
        // numa_bind() call binds the process to the node and these
        // properties are passed on to the thread that is created in
        // StartThread method called later in the loop.
        numa_bind(nodes);
        numa_set_strict(1);
        numa_free_nodemask(nodes);
      }
#endif
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
      arg[i].thread->shared = &shared;
      FLAGS_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = FLAGS_block_size; // use --block_size option for db_bench
    std::string labels = "(" + ToString(FLAGS_block_size) + " per op)";
    const char* label = labels.c_str();

    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedOps(nullptr, nullptr, 1, kCrc);
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void xxHash(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    unsigned int xxh32 = 0;
    while (bytes < 500 * 1048576) {
      xxh32 = XXH32(data.data(), size, 0);
      thread->stats.FinishedOps(nullptr, nullptr, 1, kHash);
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... xxh32=0x%x\r", static_cast<unsigned int>(xxh32));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    std::atomic<void*> ap(&dummy);
    int count = 0;
    void *ptr = nullptr;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.load(std::memory_order_acquire);
      }
      count++;
      thread->stats.FinishedOps(nullptr, nullptr, 1, kOthers);
    }
    if (ptr == nullptr) exit(1);  // Disable unused variable warning.
  }

  void Compress(ThreadState *thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(FLAGS_block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    CompressionOptions opts;
    CompressionContext context(FLAGS_compression_type_e);
    CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                         FLAGS_compression_type_e);
    // Compress 1G
    while (ok && bytes < int64_t(1) << 30) {
      compressed.clear();
      ok = CompressSlice(info, input, &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedOps(nullptr, nullptr, 1, kCompress);
    }

    if (!ok) {
      thread->stats.AddMessage("(compression failure)");
    } else {
      char buf[340];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void Uncompress(ThreadState *thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(FLAGS_block_size);
    std::string compressed;

    CompressionContext compression_ctx(FLAGS_compression_type_e);
    CompressionOptions compression_opts;
    CompressionInfo compression_info(compression_opts, compression_ctx,
                                     CompressionDict::GetEmptyDict(),
                                     FLAGS_compression_type_e);
    UncompressionContext uncompression_ctx(FLAGS_compression_type_e);
    UncompressionInfo uncompression_info(uncompression_ctx,
                                         UncompressionDict::GetEmptyDict(),
                                         FLAGS_compression_type_e);

    bool ok = CompressSlice(compression_info, input, &compressed);
    int64_t bytes = 0;
    int decompress_size;
    while (ok && bytes < 1024 * 1048576) {
      CacheAllocationPtr uncompressed;
      switch (FLAGS_compression_type_e) {
        case rocksdb::kSnappyCompression: {
          // get size and allocate here to make comparison fair
          size_t ulength = 0;
          if (!Snappy_GetUncompressedLength(compressed.data(),
                                            compressed.size(), &ulength)) {
            ok = false;
            break;
          }
          uncompressed = AllocateBlock(ulength, nullptr);
          ok = Snappy_Uncompress(compressed.data(), compressed.size(),
                                 uncompressed.get());
          break;
        }
      case rocksdb::kZlibCompression:
        uncompressed = Zlib_Uncompress(uncompression_info, compressed.data(),
                                       compressed.size(), &decompress_size, 2);
        ok = uncompressed.get() != nullptr;
        break;
      case rocksdb::kBZip2Compression:
        uncompressed = BZip2_Uncompress(compressed.data(), compressed.size(),
                                        &decompress_size, 2);
        ok = uncompressed.get() != nullptr;
        break;
      case rocksdb::kLZ4Compression:
        uncompressed = LZ4_Uncompress(uncompression_info, compressed.data(),
                                      compressed.size(), &decompress_size, 2);
        ok = uncompressed.get() != nullptr;
        break;
      case rocksdb::kLZ4HCCompression:
        uncompressed = LZ4_Uncompress(uncompression_info, compressed.data(),
                                      compressed.size(), &decompress_size, 2);
        ok = uncompressed.get() != nullptr;
        break;
      case rocksdb::kXpressCompression:
        uncompressed.reset(XPRESS_Uncompress(
            compressed.data(), compressed.size(), &decompress_size));
        ok = uncompressed.get() != nullptr;
        break;
      case rocksdb::kZSTD:
        uncompressed = ZSTD_Uncompress(uncompression_info, compressed.data(),
                                       compressed.size(), &decompress_size);
        ok = uncompressed.get() != nullptr;
        break;
      default:
        ok = false;
      }
      bytes += input.size();
      thread->stats.FinishedOps(nullptr, nullptr, 1, kUncompress);
    }

    if (!ok) {
      thread->stats.AddMessage("(compression failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  // Returns true if the options is initialized from the specified
  // options file.
  bool InitializeOptionsFromFile(Options* opts) {
#ifndef ROCKSDB_LITE
    printf("Initializing RocksDB Options from the specified file\n");
    DBOptions db_opts;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    if (FLAGS_options_file != "") {
      auto s = LoadOptionsFromFile(FLAGS_options_file, Env::Default(), &db_opts,
                                   &cf_descs);
      if (s.ok()) {
        *opts = Options(db_opts, cf_descs[0].options);
        return true;
      }
      fprintf(stderr, "Unable to load options file %s --- %s\n",
              FLAGS_options_file.c_str(), s.ToString().c_str());
      exit(1);
    }
#else
    (void)opts;
#endif
    return false;
  }

  void InitializeOptionsFromFlags(Options* opts) {
    printf("Initializing RocksDB Options from command-line flags\n");
    Options& options = *opts;

    assert(db_.db == nullptr);

    options.max_open_files = FLAGS_open_files;
    if (FLAGS_cost_write_buffer_to_cache || FLAGS_db_write_buffer_size != 0) {
      options.write_buffer_manager.reset(
          new WriteBufferManager(FLAGS_db_write_buffer_size, cache_));
    }
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;
    options.max_write_buffer_number_to_maintain =
        FLAGS_max_write_buffer_number_to_maintain;
    options.max_background_jobs = FLAGS_max_background_jobs;
    options.max_background_compactions = FLAGS_max_background_compactions;
    options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
    options.max_background_flushes = FLAGS_max_background_flushes;
    options.compaction_style = FLAGS_compaction_style_e;
    options.compaction_pri = FLAGS_compaction_pri_e;
    options.allow_mmap_reads = FLAGS_mmap_read;
    options.allow_mmap_writes = FLAGS_mmap_write;
    options.use_direct_reads = FLAGS_use_direct_reads;
    options.use_direct_io_for_flush_and_compaction =
        FLAGS_use_direct_io_for_flush_and_compaction;
#ifndef ROCKSDB_LITE
    options.ttl = FLAGS_fifo_compaction_ttl;
    options.compaction_options_fifo = CompactionOptionsFIFO(
        FLAGS_fifo_compaction_max_table_files_size_mb * 1024 * 1024,
        FLAGS_fifo_compaction_allow_compaction);
#endif  // ROCKSDB_LITE
    if (FLAGS_prefix_size != 0) {
      options.prefix_extractor.reset(
          NewFixedPrefixTransform(FLAGS_prefix_size));
    }
    if (FLAGS_use_uint64_comparator) {
      options.comparator = test::Uint64Comparator();
      if (FLAGS_key_size != 8) {
        fprintf(stderr, "Using Uint64 comparator but key size is not 8.\n");
        exit(1);
      }
    }
    if (FLAGS_use_stderr_info_logger) {
      options.info_log.reset(new StderrLogger());
    }
    options.memtable_huge_page_size = FLAGS_memtable_use_huge_page ? 2048 : 0;
    options.memtable_prefix_bloom_size_ratio = FLAGS_memtable_bloom_size_ratio;
    options.memtable_whole_key_filtering = FLAGS_memtable_whole_key_filtering;
    if (FLAGS_memtable_insert_with_hint_prefix_size > 0) {
      options.memtable_insert_with_hint_prefix_extractor.reset(
          NewCappedPrefixTransform(
              FLAGS_memtable_insert_with_hint_prefix_size));
    }
    options.bloom_locality = FLAGS_bloom_locality;
    options.max_file_opening_threads = FLAGS_file_opening_threads;
    options.new_table_reader_for_compaction_inputs =
        FLAGS_new_table_reader_for_compaction_inputs;
    options.compaction_readahead_size = FLAGS_compaction_readahead_size;
    options.random_access_max_buffer_size = FLAGS_random_access_max_buffer_size;
    options.writable_file_max_buffer_size = FLAGS_writable_file_max_buffer_size;
    options.use_fsync = FLAGS_use_fsync;
    options.num_levels = FLAGS_num_levels;
    options.target_file_size_base = FLAGS_target_file_size_base;
    options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
    options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    options.level_compaction_dynamic_level_bytes =
        FLAGS_level_compaction_dynamic_level_bytes;
    options.max_bytes_for_level_multiplier =
        FLAGS_max_bytes_for_level_multiplier;
    if ((FLAGS_prefix_size == 0) && (FLAGS_rep_factory == kPrefixHash ||
                                     FLAGS_rep_factory == kHashLinkedList)) {
      fprintf(stderr, "prefix_size should be non-zero if PrefixHash or "
                      "HashLinkedList memtablerep is used\n");
      exit(1);
    }
    switch (FLAGS_rep_factory) {
      case kSkipList:
        options.memtable_factory.reset(new SkipListFactory(
            FLAGS_skip_list_lookahead));
        break;
#ifndef ROCKSDB_LITE
      case kPrefixHash:
        options.memtable_factory.reset(
            NewHashSkipListRepFactory(FLAGS_hash_bucket_count));
        break;
      case kHashLinkedList:
        options.memtable_factory.reset(NewHashLinkListRepFactory(
            FLAGS_hash_bucket_count));
        break;
      case kVectorRep:
        options.memtable_factory.reset(
          new VectorRepFactory
        );
        break;
#else
      default:
        fprintf(stderr, "Only skip list is supported in lite mode\n");
        exit(1);
#endif  // ROCKSDB_LITE
    }
    if (FLAGS_use_plain_table) {
#ifndef ROCKSDB_LITE
      if (FLAGS_rep_factory != kPrefixHash &&
          FLAGS_rep_factory != kHashLinkedList) {
        fprintf(stderr, "Waring: plain table is used with skipList\n");
      }

      int bloom_bits_per_key = FLAGS_bloom_bits;
      if (bloom_bits_per_key < 0) {
        bloom_bits_per_key = 0;
      }

      PlainTableOptions plain_table_options;
      plain_table_options.user_key_len = FLAGS_key_size;
      plain_table_options.bloom_bits_per_key = bloom_bits_per_key;
      plain_table_options.hash_table_ratio = 0.75;
      options.table_factory = std::shared_ptr<TableFactory>(
          NewPlainTableFactory(plain_table_options));
#else
      fprintf(stderr, "Plain table is not supported in lite mode\n");
      exit(1);
#endif  // ROCKSDB_LITE
    } else if (FLAGS_use_cuckoo_table) {
#ifndef ROCKSDB_LITE
      if (FLAGS_cuckoo_hash_ratio > 1 || FLAGS_cuckoo_hash_ratio < 0) {
        fprintf(stderr, "Invalid cuckoo_hash_ratio\n");
        exit(1);
      }

      if (!FLAGS_mmap_read) {
        fprintf(stderr, "cuckoo table format requires mmap read to operate\n");
        exit(1);
      }

      rocksdb::CuckooTableOptions table_options;
      table_options.hash_table_ratio = FLAGS_cuckoo_hash_ratio;
      table_options.identity_as_first_hash = FLAGS_identity_as_first_hash;
      options.table_factory = std::shared_ptr<TableFactory>(
          NewCuckooTableFactory(table_options));
#else
      fprintf(stderr, "Cuckoo table is not supported in lite mode\n");
      exit(1);
#endif  // ROCKSDB_LITE
    } else {
      BlockBasedTableOptions block_based_options;
      if (FLAGS_use_hash_search) {
        if (FLAGS_prefix_size == 0) {
          fprintf(stderr,
              "prefix_size not assigned when enable use_hash_search \n");
          exit(1);
        }
        block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
      } else {
        block_based_options.index_type = BlockBasedTableOptions::kBinarySearch;
      }
      if (FLAGS_partition_index_and_filters || FLAGS_partition_index) {
        if (FLAGS_use_hash_search) {
          fprintf(stderr,
                  "use_hash_search is incompatible with "
                  "partition index and is ignored");
        }
        block_based_options.index_type =
            BlockBasedTableOptions::kTwoLevelIndexSearch;
        block_based_options.metadata_block_size = FLAGS_metadata_block_size;
        if (FLAGS_partition_index_and_filters) {
          block_based_options.partition_filters = true;
        }
      }
      if (cache_ == nullptr) {
        block_based_options.no_block_cache = true;
      }
      block_based_options.cache_index_and_filter_blocks =
          FLAGS_cache_index_and_filter_blocks;
      block_based_options.pin_l0_filter_and_index_blocks_in_cache =
          FLAGS_pin_l0_filter_and_index_blocks_in_cache;
      block_based_options.pin_top_level_index_and_filter =
          FLAGS_pin_top_level_index_and_filter;
      if (FLAGS_cache_high_pri_pool_ratio > 1e-6) {  // > 0.0 + eps
        block_based_options.cache_index_and_filter_blocks_with_high_priority =
            true;
      }
      block_based_options.block_cache = cache_;
      block_based_options.block_cache_compressed = compressed_cache_;
      block_based_options.block_size = FLAGS_block_size;
      block_based_options.block_restart_interval = FLAGS_block_restart_interval;
      block_based_options.index_block_restart_interval =
          FLAGS_index_block_restart_interval;
      block_based_options.filter_policy = filter_policy_;
      block_based_options.format_version =
          static_cast<uint32_t>(FLAGS_format_version);
      block_based_options.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;
      block_based_options.enable_index_compression =
          FLAGS_enable_index_compression;
      block_based_options.block_align = FLAGS_block_align;
      if (FLAGS_use_data_block_hash_index) {
        block_based_options.data_block_index_type =
            rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
      } else {
        block_based_options.data_block_index_type =
            rocksdb::BlockBasedTableOptions::kDataBlockBinarySearch;
      }
      block_based_options.data_block_hash_table_util_ratio =
          FLAGS_data_block_hash_table_util_ratio;
      if (FLAGS_read_cache_path != "") {
#ifndef ROCKSDB_LITE
        Status rc_status;

        // Read cache need to be provided with a the Logger, we will put all
        // reac cache logs in the read cache path in a file named rc_LOG
        rc_status = FLAGS_env->CreateDirIfMissing(FLAGS_read_cache_path);
        std::shared_ptr<Logger> read_cache_logger;
        if (rc_status.ok()) {
          rc_status = FLAGS_env->NewLogger(FLAGS_read_cache_path + "/rc_LOG",
                                           &read_cache_logger);
        }

        if (rc_status.ok()) {
          PersistentCacheConfig rc_cfg(FLAGS_env, FLAGS_read_cache_path,
                                       FLAGS_read_cache_size,
                                       read_cache_logger);

          rc_cfg.enable_direct_reads = FLAGS_read_cache_direct_read;
          rc_cfg.enable_direct_writes = FLAGS_read_cache_direct_write;
          rc_cfg.writer_qdepth = 4;
          rc_cfg.writer_dispatch_size = 4 * 1024;

          auto pcache = std::make_shared<BlockCacheTier>(rc_cfg);
          block_based_options.persistent_cache = pcache;
          rc_status = pcache->Open();
        }

        if (!rc_status.ok()) {
          fprintf(stderr, "Error initializing read cache, %s\n",
                  rc_status.ToString().c_str());
          exit(1);
        }
#else
        fprintf(stderr, "Read cache is not supported in LITE\n");
        exit(1);

#endif
      }
      options.table_factory.reset(
          NewBlockBasedTableFactory(block_based_options));
    }
    if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() > 0) {
      if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() !=
          (unsigned int)FLAGS_num_levels) {
        fprintf(stderr, "Insufficient number of fanouts specified %d\n",
                (int)FLAGS_max_bytes_for_level_multiplier_additional_v.size());
        exit(1);
      }
      options.max_bytes_for_level_multiplier_additional =
        FLAGS_max_bytes_for_level_multiplier_additional_v;
    }
    options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
    options.level0_file_num_compaction_trigger =
        FLAGS_level0_file_num_compaction_trigger;
    options.level0_slowdown_writes_trigger =
      FLAGS_level0_slowdown_writes_trigger;
    options.compression = FLAGS_compression_type_e;
    options.WAL_ttl_seconds = FLAGS_wal_ttl_seconds;
    options.WAL_size_limit_MB = FLAGS_wal_size_limit_MB;
    options.max_total_wal_size = FLAGS_max_total_wal_size;

    if (FLAGS_min_level_to_compress >= 0) {
      assert(FLAGS_min_level_to_compress <= FLAGS_num_levels);
      options.compression_per_level.resize(FLAGS_num_levels);
      for (int i = 0; i < FLAGS_min_level_to_compress; i++) {
        options.compression_per_level[i] = kNoCompression;
      }
      for (int i = FLAGS_min_level_to_compress;
           i < FLAGS_num_levels; i++) {
        options.compression_per_level[i] = FLAGS_compression_type_e;
      }
    }
    options.soft_rate_limit = FLAGS_soft_rate_limit;
    options.hard_rate_limit = FLAGS_hard_rate_limit;
    options.soft_pending_compaction_bytes_limit =
        FLAGS_soft_pending_compaction_bytes_limit;
    options.hard_pending_compaction_bytes_limit =
        FLAGS_hard_pending_compaction_bytes_limit;
    options.delayed_write_rate = FLAGS_delayed_write_rate;
    options.allow_concurrent_memtable_write =
        FLAGS_allow_concurrent_memtable_write;
    options.inplace_update_support = FLAGS_inplace_update_support;
    options.inplace_update_num_locks = FLAGS_inplace_update_num_locks;
    options.enable_write_thread_adaptive_yield =
        FLAGS_enable_write_thread_adaptive_yield;
    options.enable_pipelined_write = FLAGS_enable_pipelined_write;
    options.write_thread_max_yield_usec = FLAGS_write_thread_max_yield_usec;
    options.write_thread_slow_yield_usec = FLAGS_write_thread_slow_yield_usec;
    options.rate_limit_delay_max_milliseconds =
      FLAGS_rate_limit_delay_max_milliseconds;
    options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
    options.max_compaction_bytes = FLAGS_max_compaction_bytes;
    options.disable_auto_compactions = FLAGS_disable_auto_compactions;
    options.optimize_filters_for_hits = FLAGS_optimize_filters_for_hits;

    // fill storage options
    options.advise_random_on_open = FLAGS_advise_random_on_open;
    options.access_hint_on_compaction_start = FLAGS_compaction_fadvice_e;
    options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
    options.bytes_per_sync = FLAGS_bytes_per_sync;
    options.wal_bytes_per_sync = FLAGS_wal_bytes_per_sync;

    // merge operator options
    options.merge_operator = MergeOperators::CreateFromStringId(
        FLAGS_merge_operator);
    if (options.merge_operator == nullptr && !FLAGS_merge_operator.empty()) {
      fprintf(stderr, "invalid merge operator: %s\n",
              FLAGS_merge_operator.c_str());
      exit(1);
    }
    options.max_successive_merges = FLAGS_max_successive_merges;
    options.report_bg_io_stats = FLAGS_report_bg_io_stats;

    // set universal style compaction configurations, if applicable
    if (FLAGS_universal_size_ratio != 0) {
      options.compaction_options_universal.size_ratio =
        FLAGS_universal_size_ratio;
    }
    if (FLAGS_universal_min_merge_width != 0) {
      options.compaction_options_universal.min_merge_width =
        FLAGS_universal_min_merge_width;
    }
    if (FLAGS_universal_max_merge_width != 0) {
      options.compaction_options_universal.max_merge_width =
        FLAGS_universal_max_merge_width;
    }
    if (FLAGS_universal_max_size_amplification_percent != 0) {
      options.compaction_options_universal.max_size_amplification_percent =
        FLAGS_universal_max_size_amplification_percent;
    }
    if (FLAGS_universal_compression_size_percent != -1) {
      options.compaction_options_universal.compression_size_percent =
        FLAGS_universal_compression_size_percent;
    }
    options.compaction_options_universal.allow_trivial_move =
        FLAGS_universal_allow_trivial_move;
    if (FLAGS_thread_status_per_interval > 0) {
      options.enable_thread_tracking = true;
    }

#ifndef ROCKSDB_LITE
    if (FLAGS_readonly && FLAGS_transaction_db) {
      fprintf(stderr, "Cannot use readonly flag with transaction_db\n");
      exit(1);
    }
#endif  // ROCKSDB_LITE

  }

  void InitializeOptionsGeneral(Options* opts) {
    Options& options = *opts;

    options.create_missing_column_families = FLAGS_num_column_families > 1;
    options.statistics = dbstats;
    options.wal_dir = FLAGS_wal_dir;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.dump_malloc_stats = FLAGS_dump_malloc_stats;
    options.stats_dump_period_sec =
        static_cast<unsigned int>(FLAGS_stats_dump_period_sec);
    options.stats_persist_period_sec =
        static_cast<unsigned int>(FLAGS_stats_persist_period_sec);
    options.stats_history_buffer_size =
        static_cast<size_t>(FLAGS_stats_history_buffer_size);

    options.compression_opts.level = FLAGS_compression_level;
    options.compression_opts.max_dict_bytes = FLAGS_compression_max_dict_bytes;
    options.compression_opts.zstd_max_train_bytes =
        FLAGS_compression_zstd_max_train_bytes;
    // If this is a block based table, set some related options
    if (options.table_factory->Name() == BlockBasedTableFactory::kName &&
        options.table_factory->GetOptions() != nullptr) {
      BlockBasedTableOptions* table_options =
          reinterpret_cast<BlockBasedTableOptions*>(
              options.table_factory->GetOptions());
      if (FLAGS_cache_size) {
        table_options->block_cache = cache_;
      }
      if (FLAGS_bloom_bits >= 0) {
        table_options->filter_policy.reset(NewBloomFilterPolicy(
            FLAGS_bloom_bits, FLAGS_use_block_based_filter));
      }
    }
    if (FLAGS_row_cache_size) {
      if (FLAGS_cache_numshardbits >= 1) {
        options.row_cache =
            NewLRUCache(FLAGS_row_cache_size, FLAGS_cache_numshardbits);
      } else {
        options.row_cache = NewLRUCache(FLAGS_row_cache_size);
      }
    }
    if (FLAGS_enable_io_prio) {
      FLAGS_env->LowerThreadPoolIOPriority(Env::LOW);
      FLAGS_env->LowerThreadPoolIOPriority(Env::HIGH);
    }
    if (FLAGS_enable_cpu_prio) {
      FLAGS_env->LowerThreadPoolCPUPriority(Env::LOW);
      FLAGS_env->LowerThreadPoolCPUPriority(Env::HIGH);
    }
    options.env = FLAGS_env;
    if (FLAGS_sine_write_rate) {
      FLAGS_benchmark_write_rate_limit = static_cast<uint64_t>(SineRate(0));
    }

    if (FLAGS_rate_limiter_bytes_per_sec > 0) {
      if (FLAGS_rate_limit_bg_reads &&
          !FLAGS_new_table_reader_for_compaction_inputs) {
        fprintf(stderr,
                "rate limit compaction reads must have "
                "new_table_reader_for_compaction_inputs set\n");
        exit(1);
      }
      options.rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_rate_limiter_bytes_per_sec, 100 * 1000 /* refill_period_us */,
          10 /* fairness */,
          FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                    : RateLimiter::Mode::kWritesOnly,
          FLAGS_rate_limiter_auto_tuned));
    }

    options.listeners.emplace_back(listener_);
    if (FLAGS_num_multi_db <= 1) {
      OpenDb(options, FLAGS_db, &db_);
    } else {
      multi_dbs_.clear();
      multi_dbs_.resize(FLAGS_num_multi_db);
      auto wal_dir = options.wal_dir;
      for (int i = 0; i < FLAGS_num_multi_db; i++) {
        if (!wal_dir.empty()) {
          options.wal_dir = GetPathForMultiple(wal_dir, i);
        }
        OpenDb(options, GetPathForMultiple(FLAGS_db, i), &multi_dbs_[i]);
      }
      options.wal_dir = wal_dir;
    }

    // KeepFilter is a noop filter, this can be used to test compaction filter
    if (FLAGS_use_keep_filter) {
      options.compaction_filter = new KeepFilter();
      fprintf(stdout, "A noop compaction filter is used\n");
    }
  }

  void Open(Options* opts) {
    if (!InitializeOptionsFromFile(opts)) {
      InitializeOptionsFromFlags(opts);
    }

    InitializeOptionsGeneral(opts);
  }

  void OpenDb(Options options, const std::string& db_name,
      DBWithColumnFamilies* db) {
    Status s;
    // Open with column families if necessary.
    if (FLAGS_num_column_families > 1) {
      size_t num_hot = FLAGS_num_column_families;
      if (FLAGS_num_hot_column_families > 0 &&
          FLAGS_num_hot_column_families < FLAGS_num_column_families) {
        num_hot = FLAGS_num_hot_column_families;
      } else {
        FLAGS_num_hot_column_families = FLAGS_num_column_families;
      }
      std::vector<ColumnFamilyDescriptor> column_families;
      for (size_t i = 0; i < num_hot; i++) {
        column_families.push_back(ColumnFamilyDescriptor(
              ColumnFamilyName(i), ColumnFamilyOptions(options)));
      }
      std::vector<int> cfh_idx_to_prob;
      if (!FLAGS_column_family_distribution.empty()) {
        std::stringstream cf_prob_stream(FLAGS_column_family_distribution);
        std::string cf_prob;
        int sum = 0;
        while (std::getline(cf_prob_stream, cf_prob, ',')) {
          cfh_idx_to_prob.push_back(std::stoi(cf_prob));
          sum += cfh_idx_to_prob.back();
        }
        if (sum != 100) {
          fprintf(stderr, "column_family_distribution items must sum to 100\n");
          exit(1);
        }
        if (cfh_idx_to_prob.size() != num_hot) {
          fprintf(stderr,
                  "got %" ROCKSDB_PRIszt
                  " column_family_distribution items; expected "
                  "%" ROCKSDB_PRIszt "\n",
                  cfh_idx_to_prob.size(), num_hot);
          exit(1);
        }
      }
#ifndef ROCKSDB_LITE
      if (FLAGS_readonly) {
        s = DB::OpenForReadOnly(options, db_name, column_families,
            &db->cfh, &db->db);
      } else if (FLAGS_optimistic_transaction_db) {
        s = OptimisticTransactionDB::Open(options, db_name, column_families,
                                          &db->cfh, &db->opt_txn_db);
        if (s.ok()) {
          db->db = db->opt_txn_db->GetBaseDB();
        }
      } else if (FLAGS_transaction_db) {
        TransactionDB* ptr;
        TransactionDBOptions txn_db_options;
        s = TransactionDB::Open(options, txn_db_options, db_name,
                                column_families, &db->cfh, &ptr);
        if (s.ok()) {
          db->db = ptr;
        }
      } else {
        s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
      }
#else
      s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
#endif  // ROCKSDB_LITE
      db->cfh.resize(FLAGS_num_column_families);
      db->num_created = num_hot;
      db->num_hot = num_hot;
      db->cfh_idx_to_prob = std::move(cfh_idx_to_prob);
#ifndef ROCKSDB_LITE
    } else if (FLAGS_readonly) {
      s = DB::OpenForReadOnly(options, db_name, &db->db);
    } else if (FLAGS_optimistic_transaction_db) {
      s = OptimisticTransactionDB::Open(options, db_name, &db->opt_txn_db);
      if (s.ok()) {
        db->db = db->opt_txn_db->GetBaseDB();
      }
    } else if (FLAGS_transaction_db) {
      TransactionDB* ptr = nullptr;
      TransactionDBOptions txn_db_options;
      s = CreateLoggerFromOptions(db_name, options, &options.info_log);
      if (s.ok()) {
        s = TransactionDB::Open(options, txn_db_options, db_name, &ptr);
      }
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_blob_db) {
      blob_db::BlobDBOptions blob_db_options;
      blob_db_options.enable_garbage_collection = FLAGS_blob_db_enable_gc;
      blob_db_options.is_fifo = FLAGS_blob_db_is_fifo;
      blob_db_options.max_db_size = FLAGS_blob_db_max_db_size;
      blob_db_options.ttl_range_secs = FLAGS_blob_db_ttl_range_secs;
      blob_db_options.min_blob_size = FLAGS_blob_db_min_blob_size;
      blob_db_options.bytes_per_sync = FLAGS_blob_db_bytes_per_sync;
      blob_db_options.blob_file_size = FLAGS_blob_db_file_size;
      blob_db::BlobDB* ptr = nullptr;
      s = blob_db::BlobDB::Open(options, blob_db_options, db_name, &ptr);
      if (s.ok()) {
        db->db = ptr;
      }
#endif  // ROCKSDB_LITE
    } else {
      s = DB::Open(options, db_name, &db->db);
    }
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
  };

  void WriteSeqDeterministic(ThreadState* thread) {
    DoDeterministicCompact(thread, open_options_.compaction_style, SEQUENTIAL);
  }

  void WriteUniqueRandomDeterministic(ThreadState* thread) {
    DoDeterministicCompact(thread, open_options_.compaction_style,
                           UNIQUE_RANDOM);
  }

  void WriteSeq(ThreadState* thread) {
    DoWrite(thread, SEQUENTIAL);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, RANDOM);
  }

  void WriteUniqueRandom(ThreadState* thread) {
    DoWrite(thread, UNIQUE_RANDOM);
  }

  class KeyGenerator {
   public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {
      if (mode_ == UNIQUE_RANDOM) {
        // NOTE: if memory consumption of this approach becomes a concern,
        // we can either break it into pieces and only random shuffle a section
        // each time. Alternatively, use a bit map implementation
        // (https://reviews.facebook.net/differential/diff/54627/)
        values_.resize(num_);
        for (uint64_t i = 0; i < num_; ++i) {
          values_[i] = i;
        }
        std::shuffle(
            values_.begin(), values_.end(),
            std::default_random_engine(static_cast<unsigned int>(FLAGS_seed)));
      }
    }

    uint64_t Next() {
      switch (mode_) {
        case SEQUENTIAL:
          return next_++;
        case RANDOM:
          return rand_->Next() % num_;
        case UNIQUE_RANDOM:
          assert(next_ < num_);
          return values_[next_++];
      }
      assert(false);
      return std::numeric_limits<uint64_t>::max();
    }

   private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
  };

  DB* SelectDB(ThreadState* thread) {
    return SelectDBWithCfh(thread)->db;
  }

  DBWithColumnFamilies* SelectDBWithCfh(ThreadState* thread) {
    return SelectDBWithCfh(thread->rand.Next());
  }

  DBWithColumnFamilies* SelectDBWithCfh(uint64_t rand_int) {
    if (db_.db != nullptr) {
      return &db_;
    } else  {
      return &multi_dbs_[rand_int % multi_dbs_.size()];
    }
  }

  double SineRate(double x) {
    return FLAGS_sine_a*sin((FLAGS_sine_b*x) + FLAGS_sine_c) + FLAGS_sine_d;
  }

  void DoWrite(ThreadState* thread, WriteMode write_mode) {
    const int test_duration = write_mode == RANDOM ? FLAGS_duration : 0;
    const int64_t num_ops = writes_ == 0 ? num_ : writes_;

    size_t num_key_gens = 1;
    if (db_.db == nullptr) {
      num_key_gens = multi_dbs_.size();
    }
    std::vector<std::unique_ptr<KeyGenerator>> key_gens(num_key_gens);
    int64_t max_ops = num_ops * num_key_gens;
    int64_t ops_per_stage = max_ops;
    if (FLAGS_num_column_families > 1 && FLAGS_num_hot_column_families > 0) {
      ops_per_stage = (max_ops - 1) / (FLAGS_num_column_families /
                                       FLAGS_num_hot_column_families) +
                      1;
    }

    Duration duration(test_duration, max_ops, ops_per_stage);
    for (size_t i = 0; i < num_key_gens; i++) {
      key_gens[i].reset(new KeyGenerator(&(thread->rand), write_mode,
                                         num_ + max_num_range_tombstones_,
                                         ops_per_stage));
    }

    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<const char[]> begin_key_guard;
    Slice begin_key = AllocateKey(&begin_key_guard);
    std::unique_ptr<const char[]> end_key_guard;
    Slice end_key = AllocateKey(&end_key_guard);
    std::vector<std::unique_ptr<const char[]>> expanded_key_guards;
    std::vector<Slice> expanded_keys;
    if (FLAGS_expand_range_tombstones) {
      expanded_key_guards.resize(range_tombstone_width_);
      for (auto& expanded_key_guard : expanded_key_guards) {
        expanded_keys.emplace_back(AllocateKey(&expanded_key_guard));
      }
    }

    int64_t stage = 0;
    int64_t num_written = 0;
    while (!duration.Done(entries_per_batch_)) {
      if (duration.GetStage() != stage) {
        stage = duration.GetStage();
        if (db_.db != nullptr) {
          db_.CreateNewCf(open_options_, stage);
        } else {
          for (auto& db : multi_dbs_) {
            db.CreateNewCf(open_options_, stage);
          }
        }
      }

      size_t id = thread->rand.Next() % num_key_gens;
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(id);
      batch.Clear();

      if (thread->shared->write_rate_limiter.get() != nullptr) {
        thread->shared->write_rate_limiter->Request(
            entries_per_batch_ * (value_size_ + key_size_), Env::IO_HIGH,
            nullptr /* stats */, RateLimiter::OpType::kWrite);
        // Set time at which last op finished to Now() to hide latency and
        // sleep from rate limiter. Also, do the check once per batch, not
        // once per write.
        thread->stats.ResetLastOpTime();
      }

      for (int64_t j = 0; j < entries_per_batch_; j++) {
        int64_t rand_num = key_gens[id]->Next();
        GenerateKeyFromInt(rand_num, FLAGS_num, &key);
        if (use_blob_db_) {
#ifndef ROCKSDB_LITE
          Slice val = gen.Generate(value_size_);
          int ttl = rand() % FLAGS_blob_db_max_ttl_range;
          blob_db::BlobDB* blobdb =
              static_cast<blob_db::BlobDB*>(db_with_cfh->db);
          s = blobdb->PutWithTTL(write_options_, key, val, ttl);
#endif  //  ROCKSDB_LITE
        } else if (FLAGS_num_column_families <= 1) {
          batch.Put(key, gen.Generate(value_size_));
        } else {
          // We use same rand_num as seed for key and column family so that we
          // can deterministically find the cfh corresponding to a particular
          // key while reading the key.
          batch.Put(db_with_cfh->GetCfh(rand_num), key,
                    gen.Generate(value_size_));
        }
        bytes += value_size_ + key_size_;
        ++num_written;
        if (writes_per_range_tombstone_ > 0 &&
            num_written > writes_before_delete_range_ &&
            (num_written - writes_before_delete_range_) /
                    writes_per_range_tombstone_ <=
                max_num_range_tombstones_ &&
            (num_written - writes_before_delete_range_) %
                    writes_per_range_tombstone_ ==
                0) {
          int64_t begin_num = key_gens[id]->Next();
          if (FLAGS_expand_range_tombstones) {
            for (int64_t offset = 0; offset < range_tombstone_width_;
                 ++offset) {
              GenerateKeyFromInt(begin_num + offset, FLAGS_num,
                                 &expanded_keys[offset]);
              if (use_blob_db_) {
#ifndef ROCKSDB_LITE
                s = db_with_cfh->db->Delete(write_options_,
                                            expanded_keys[offset]);
#endif  //  ROCKSDB_LITE
              } else if (FLAGS_num_column_families <= 1) {
                batch.Delete(expanded_keys[offset]);
              } else {
                batch.Delete(db_with_cfh->GetCfh(rand_num),
                             expanded_keys[offset]);
              }
            }
          } else {
            GenerateKeyFromInt(begin_num, FLAGS_num, &begin_key);
            GenerateKeyFromInt(begin_num + range_tombstone_width_, FLAGS_num,
                               &end_key);
            if (use_blob_db_) {
#ifndef ROCKSDB_LITE
              s = db_with_cfh->db->DeleteRange(
                  write_options_, db_with_cfh->db->DefaultColumnFamily(),
                  begin_key, end_key);
#endif  //  ROCKSDB_LITE
            } else if (FLAGS_num_column_families <= 1) {
              batch.DeleteRange(begin_key, end_key);
            } else {
              batch.DeleteRange(db_with_cfh->GetCfh(rand_num), begin_key,
                                end_key);
            }
          }
        }
      }
      if (!use_blob_db_) {
        s = db_with_cfh->db->Write(write_options_, &batch);
      }
      thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db,
                                entries_per_batch_, kWrite);
      if (FLAGS_sine_write_rate) {
        uint64_t now = FLAGS_env->NowMicros();

        uint64_t usecs_since_last;
        if (now > thread->stats.GetSineInterval()) {
          usecs_since_last = now - thread->stats.GetSineInterval();
        } else {
          usecs_since_last = 0;
        }

        if (usecs_since_last >
            (FLAGS_sine_write_rate_interval_milliseconds * uint64_t{1000})) {
          double usecs_since_start =
                  static_cast<double>(now - thread->stats.GetStart());
          thread->stats.ResetSineInterval();
          uint64_t write_rate =
                  static_cast<uint64_t>(SineRate(usecs_since_start / 1000000.0));
          thread->shared->write_rate_limiter.reset(
                  NewGenericRateLimiter(write_rate));
        }
      }
      if (!s.ok()) {
        s = listener_->WaitForRecovery(600000000) ? Status::OK() : s;
      }

      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }

  Status DoDeterministicCompact(ThreadState* thread,
                                CompactionStyle compaction_style,
                                WriteMode write_mode) {
#ifndef ROCKSDB_LITE
    ColumnFamilyMetaData meta;
    std::vector<DB*> db_list;
    if (db_.db != nullptr) {
      db_list.push_back(db_.db);
    } else {
      for (auto& db : multi_dbs_) {
        db_list.push_back(db.db);
      }
    }
    std::vector<Options> options_list;
    for (auto db : db_list) {
      options_list.push_back(db->GetOptions());
      if (compaction_style != kCompactionStyleFIFO) {
        db->SetOptions({{"disable_auto_compactions", "1"},
                        {"level0_slowdown_writes_trigger", "400000000"},
                        {"level0_stop_writes_trigger", "400000000"}});
      } else {
        db->SetOptions({{"disable_auto_compactions", "1"}});
      }
    }

    assert(!db_list.empty());
    auto num_db = db_list.size();
    size_t num_levels = static_cast<size_t>(open_options_.num_levels);
    size_t output_level = open_options_.num_levels - 1;
    std::vector<std::vector<std::vector<SstFileMetaData>>> sorted_runs(num_db);
    std::vector<size_t> num_files_at_level0(num_db, 0);
    if (compaction_style == kCompactionStyleLevel) {
      if (num_levels == 0) {
        return Status::InvalidArgument("num_levels should be larger than 1");
      }
      bool should_stop = false;
      while (!should_stop) {
        if (sorted_runs[0].empty()) {
          DoWrite(thread, write_mode);
        } else {
          DoWrite(thread, UNIQUE_RANDOM);
        }
        for (size_t i = 0; i < num_db; i++) {
          auto db = db_list[i];
          db->Flush(FlushOptions());
          db->GetColumnFamilyMetaData(&meta);
          if (num_files_at_level0[i] == meta.levels[0].files.size() ||
              writes_ == 0) {
            should_stop = true;
            continue;
          }
          sorted_runs[i].emplace_back(
              meta.levels[0].files.begin(),
              meta.levels[0].files.end() - num_files_at_level0[i]);
          num_files_at_level0[i] = meta.levels[0].files.size();
          if (sorted_runs[i].back().size() == 1) {
            should_stop = true;
            continue;
          }
          if (sorted_runs[i].size() == output_level) {
            auto& L1 = sorted_runs[i].back();
            L1.erase(L1.begin(), L1.begin() + L1.size() / 3);
            should_stop = true;
            continue;
          }
        }
        writes_ /= static_cast<int64_t>(open_options_.max_bytes_for_level_multiplier);
      }
      for (size_t i = 0; i < num_db; i++) {
        if (sorted_runs[i].size() < num_levels - 1) {
          fprintf(stderr, "n is too small to fill %" ROCKSDB_PRIszt " levels\n", num_levels);
          exit(1);
        }
      }
      for (size_t i = 0; i < num_db; i++) {
        auto db = db_list[i];
        auto compactionOptions = CompactionOptions();
        compactionOptions.compression = FLAGS_compression_type_e;
        auto options = db->GetOptions();
        MutableCFOptions mutable_cf_options(options);
        for (size_t j = 0; j < sorted_runs[i].size(); j++) {
          compactionOptions.output_file_size_limit =
              MaxFileSizeForLevel(mutable_cf_options,
                  static_cast<int>(output_level), compaction_style);
          std::cout << sorted_runs[i][j].size() << std::endl;
          db->CompactFiles(compactionOptions, {sorted_runs[i][j].back().name,
                                               sorted_runs[i][j].front().name},
                           static_cast<int>(output_level - j) /*level*/);
        }
      }
    } else if (compaction_style == kCompactionStyleUniversal) {
      auto ratio = open_options_.compaction_options_universal.size_ratio;
      bool should_stop = false;
      while (!should_stop) {
        if (sorted_runs[0].empty()) {
          DoWrite(thread, write_mode);
        } else {
          DoWrite(thread, UNIQUE_RANDOM);
        }
        for (size_t i = 0; i < num_db; i++) {
          auto db = db_list[i];
          db->Flush(FlushOptions());
          db->GetColumnFamilyMetaData(&meta);
          if (num_files_at_level0[i] == meta.levels[0].files.size() ||
              writes_ == 0) {
            should_stop = true;
            continue;
          }
          sorted_runs[i].emplace_back(
              meta.levels[0].files.begin(),
              meta.levels[0].files.end() - num_files_at_level0[i]);
          num_files_at_level0[i] = meta.levels[0].files.size();
          if (sorted_runs[i].back().size() == 1) {
            should_stop = true;
            continue;
          }
          num_files_at_level0[i] = meta.levels[0].files.size();
        }
        writes_ =  static_cast<int64_t>(writes_* static_cast<double>(100) / (ratio + 200));
      }
      for (size_t i = 0; i < num_db; i++) {
        if (sorted_runs[i].size() < num_levels) {
          fprintf(stderr, "n is too small to fill %" ROCKSDB_PRIszt  " levels\n", num_levels);
          exit(1);
        }
      }
      for (size_t i = 0; i < num_db; i++) {
        auto db = db_list[i];
        auto compactionOptions = CompactionOptions();
        compactionOptions.compression = FLAGS_compression_type_e;
        auto options = db->GetOptions();
        MutableCFOptions mutable_cf_options(options);
        for (size_t j = 0; j < sorted_runs[i].size(); j++) {
          compactionOptions.output_file_size_limit =
              MaxFileSizeForLevel(mutable_cf_options,
                  static_cast<int>(output_level), compaction_style);
          db->CompactFiles(
              compactionOptions,
              {sorted_runs[i][j].back().name, sorted_runs[i][j].front().name},
              (output_level > j ? static_cast<int>(output_level - j)
                                : 0) /*level*/);
        }
      }
    } else if (compaction_style == kCompactionStyleFIFO) {
      if (num_levels != 1) {
        return Status::InvalidArgument(
          "num_levels should be 1 for FIFO compaction");
      }
      if (FLAGS_num_multi_db != 0) {
        return Status::InvalidArgument("Doesn't support multiDB");
      }
      auto db = db_list[0];
      std::vector<std::string> file_names;
      while (true) {
        if (sorted_runs[0].empty()) {
          DoWrite(thread, write_mode);
        } else {
          DoWrite(thread, UNIQUE_RANDOM);
        }
        db->Flush(FlushOptions());
        db->GetColumnFamilyMetaData(&meta);
        auto total_size = meta.levels[0].size;
        if (total_size >=
          db->GetOptions().compaction_options_fifo.max_table_files_size) {
          for (auto file_meta : meta.levels[0].files) {
            file_names.emplace_back(file_meta.name);
          }
          break;
        }
      }
      // TODO(shuzhang1989): Investigate why CompactFiles not working
      // auto compactionOptions = CompactionOptions();
      // db->CompactFiles(compactionOptions, file_names, 0);
      auto compactionOptions = CompactRangeOptions();
      db->CompactRange(compactionOptions, nullptr, nullptr);
    } else {
      fprintf(stdout,
              "%-12s : skipped (-compaction_stype=kCompactionStyleNone)\n",
              "filldeterministic");
      return Status::InvalidArgument("None compaction is not supported");
    }

// Verify seqno and key range
// Note: the seqno get changed at the max level by implementation
// optimization, so skip the check of the max level.
#ifndef NDEBUG
    for (size_t k = 0; k < num_db; k++) {
      auto db = db_list[k];
      db->GetColumnFamilyMetaData(&meta);
      // verify the number of sorted runs
      if (compaction_style == kCompactionStyleLevel) {
        assert(num_levels - 1 == sorted_runs[k].size());
      } else if (compaction_style == kCompactionStyleUniversal) {
        assert(meta.levels[0].files.size() + num_levels - 1 ==
               sorted_runs[k].size());
      } else if (compaction_style == kCompactionStyleFIFO) {
        // TODO(gzh): FIFO compaction
        db->GetColumnFamilyMetaData(&meta);
        auto total_size = meta.levels[0].size;
        assert(total_size <=
          db->GetOptions().compaction_options_fifo.max_table_files_size);
          break;
      }

      // verify smallest/largest seqno and key range of each sorted run
      auto max_level = num_levels - 1;
      int level;
      for (size_t i = 0; i < sorted_runs[k].size(); i++) {
        level = static_cast<int>(max_level - i);
        SequenceNumber sorted_run_smallest_seqno = kMaxSequenceNumber;
        SequenceNumber sorted_run_largest_seqno = 0;
        std::string sorted_run_smallest_key, sorted_run_largest_key;
        bool first_key = true;
        for (auto fileMeta : sorted_runs[k][i]) {
          sorted_run_smallest_seqno =
              std::min(sorted_run_smallest_seqno, fileMeta.smallest_seqno);
          sorted_run_largest_seqno =
              std::max(sorted_run_largest_seqno, fileMeta.largest_seqno);
          if (first_key ||
              db->DefaultColumnFamily()->GetComparator()->Compare(
                  fileMeta.smallestkey, sorted_run_smallest_key) < 0) {
            sorted_run_smallest_key = fileMeta.smallestkey;
          }
          if (first_key ||
              db->DefaultColumnFamily()->GetComparator()->Compare(
                  fileMeta.largestkey, sorted_run_largest_key) > 0) {
            sorted_run_largest_key = fileMeta.largestkey;
          }
          first_key = false;
        }
        if (compaction_style == kCompactionStyleLevel ||
            (compaction_style == kCompactionStyleUniversal && level > 0)) {
          SequenceNumber level_smallest_seqno = kMaxSequenceNumber;
          SequenceNumber level_largest_seqno = 0;
          for (auto fileMeta : meta.levels[level].files) {
            level_smallest_seqno =
                std::min(level_smallest_seqno, fileMeta.smallest_seqno);
            level_largest_seqno =
                std::max(level_largest_seqno, fileMeta.largest_seqno);
          }
          assert(sorted_run_smallest_key ==
                 meta.levels[level].files.front().smallestkey);
          assert(sorted_run_largest_key ==
                 meta.levels[level].files.back().largestkey);
          if (level != static_cast<int>(max_level)) {
            // compaction at max_level would change sequence number
            assert(sorted_run_smallest_seqno == level_smallest_seqno);
            assert(sorted_run_largest_seqno == level_largest_seqno);
          }
        } else if (compaction_style == kCompactionStyleUniversal) {
          // level <= 0 means sorted runs on level 0
          auto level0_file =
              meta.levels[0].files[sorted_runs[k].size() - 1 - i];
          assert(sorted_run_smallest_key == level0_file.smallestkey);
          assert(sorted_run_largest_key == level0_file.largestkey);
          if (level != static_cast<int>(max_level)) {
            assert(sorted_run_smallest_seqno == level0_file.smallest_seqno);
            assert(sorted_run_largest_seqno == level0_file.largest_seqno);
          }
        }
      }
    }
#endif
    // print the size of each sorted_run
    for (size_t k = 0; k < num_db; k++) {
      auto db = db_list[k];
      fprintf(stdout,
              "---------------------- DB %" ROCKSDB_PRIszt " LSM ---------------------\n", k);
      db->GetColumnFamilyMetaData(&meta);
      for (auto& levelMeta : meta.levels) {
        if (levelMeta.files.empty()) {
          continue;
        }
        if (levelMeta.level == 0) {
          for (auto& fileMeta : levelMeta.files) {
            fprintf(stdout, "Level[%d]: %s(size: %" ROCKSDB_PRIszt " bytes)\n",
                    levelMeta.level, fileMeta.name.c_str(), fileMeta.size);
          }
        } else {
          fprintf(stdout, "Level[%d]: %s - %s(total size: %" PRIi64 " bytes)\n",
                  levelMeta.level, levelMeta.files.front().name.c_str(),
                  levelMeta.files.back().name.c_str(), levelMeta.size);
        }
      }
    }
    for (size_t i = 0; i < num_db; i++) {
      db_list[i]->SetOptions(
          {{"disable_auto_compactions",
            std::to_string(options_list[i].disable_auto_compactions)},
           {"level0_slowdown_writes_trigger",
            std::to_string(options_list[i].level0_slowdown_writes_trigger)},
           {"level0_stop_writes_trigger",
            std::to_string(options_list[i].level0_stop_writes_trigger)}});
    }
    return Status::OK();
#else
    (void)thread;
    (void)compaction_style;
    (void)write_mode;
    fprintf(stderr, "Rocksdb Lite doesn't support filldeterministic\n");
    return Status::NotSupported(
        "Rocksdb Lite doesn't support filldeterministic");
#endif  // ROCKSDB_LITE
  }

  void ReadSequential(ThreadState* thread) {
    if (db_.db != nullptr) {
      ReadSequential(thread, db_.db);
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        ReadSequential(thread, db_with_cfh.db);
      }
    }
  }

  void ReadSequential(ThreadState* thread, DB* db) {
    ReadOptions options(FLAGS_verify_checksum, true);
    options.tailing = FLAGS_use_tailing_iterator;

    Iterator* iter = db->NewIterator(options);
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedOps(nullptr, db, 1, kRead);
      ++i;

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          i % 1024 == 1023) {
        thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH,
                                                   nullptr /* stats */,
                                                   RateLimiter::OpType::kRead);
      }
    }

    delete iter;
    thread->stats.AddBytes(bytes);
    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  void ReadReverse(ThreadState* thread) {
    if (db_.db != nullptr) {
      ReadReverse(thread, db_.db);
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        ReadReverse(thread, db_with_cfh.db);
      }
    }
  }

  void ReadReverse(ThreadState* thread, DB* db) {
    Iterator* iter = db->NewIterator(ReadOptions(FLAGS_verify_checksum, true));
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedOps(nullptr, db, 1, kRead);
      ++i;
      if (thread->shared->read_rate_limiter.get() != nullptr &&
          i % 1024 == 1023) {
        thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH,
                                                   nullptr /* stats */,
                                                   RateLimiter::OpType::kRead);
      }
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandomFast(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t nonexist = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::string value;
    DB* db = SelectDBWithCfh(thread)->db;

    int64_t pot = 1;
    while (pot < FLAGS_num) {
      pot <<= 1;
    }

    Duration duration(FLAGS_duration, reads_);
    do {
      for (int i = 0; i < 100; ++i) {
        int64_t key_rand = thread->rand.Next() & (pot - 1);
        GenerateKeyFromInt(key_rand, FLAGS_num, &key);
        ++read;
        auto status = db->Get(options, key, &value);
        if (status.ok()) {
          ++found;
        } else if (!status.IsNotFound()) {
          fprintf(stderr, "Get returned an error: %s\n",
                  status.ToString().c_str());
          abort();
        }
        if (key_rand >= FLAGS_num) {
          ++nonexist;
        }
      }
      if (thread->shared->read_rate_limiter.get() != nullptr) {
        thread->shared->read_rate_limiter->Request(
            100, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(nullptr, db, 100, kRead);
    } while (!duration.Done(100));

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found, "
             "issued %" PRIu64 " non-exist keys)\n",
             found, read, nonexist);

    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  int64_t GetRandomKey(Random64* rand) {
    uint64_t rand_int = rand->Next();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
      key_rand = rand_int % FLAGS_num;
    } else {
      const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
      long double order = -static_cast<long double>(rand_int % kBigInt) /
                          static_cast<long double>(kBigInt) *
                          read_random_exp_range_;
      long double exp_ran = std::exp(order);
      uint64_t rand_num =
          static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num));
      // Map to a different number to avoid locality.
      const uint64_t kBigPrime = 0x5bd1e995;
      // Overflow is like %(2^64). Will have little impact of results.
      key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num);
    }
    return key_rand;
  }

  void ReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      // We use same key_rand as seed for key and column family so that we can
      // deterministically find the cfh corresponding to a particular key, as it
      // is done in DoWrite method.
      int64_t key_rand = GetRandomKey(&thread->rand);
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      read++;
      Status s;
      if (FLAGS_num_column_families > 1) {
        s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
                                 &pinnable_val);
      } else {
        pinnable_val.Reset();
        s = db_with_cfh->db->Get(options,
                                 db_with_cfh->db->DefaultColumnFamily(), key,
                                 &pinnable_val);
      }
      if (s.ok()) {
        found++;
        bytes += key.size() + pinnable_val.size();
      } else if (!s.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
        abort();
      }

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n",
             found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  // Calls MultiGet over a list of keys from a random distribution.
  // Returns the total number of keys found.
  void MultiReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t num_multireads = 0;
    int64_t found = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    std::vector<Slice> keys;
    std::vector<std::unique_ptr<const char[]> > key_guards;
    std::vector<std::string> values(entries_per_batch_);
    while (static_cast<int64_t>(keys.size()) < entries_per_batch_) {
      key_guards.push_back(std::unique_ptr<const char[]>());
      keys.push_back(AllocateKey(&key_guards.back()));
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      for (int64_t i = 0; i < entries_per_batch_; ++i) {
        GenerateKeyFromInt(GetRandomKey(&thread->rand), FLAGS_num, &keys[i]);
      }
      std::vector<Status> statuses = db->MultiGet(options, keys, &values);
      assert(static_cast<int64_t>(statuses.size()) == entries_per_batch_);

      read += entries_per_batch_;
      num_multireads++;
      for (int64_t i = 0; i < entries_per_batch_; ++i) {
        if (statuses[i].ok()) {
          ++found;
        } else if (!statuses[i].IsNotFound()) {
          fprintf(stderr, "MultiGet returned an error: %s\n",
                  statuses[i].ToString().c_str());
          abort();
        }
      }
      if (thread->shared->read_rate_limiter.get() != nullptr &&
          num_multireads % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256 * entries_per_batch_, Env::IO_HIGH, nullptr /* stats */,
            RateLimiter::OpType::kRead);
      }
      thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)",
             found, read);
    thread->stats.AddMessage(msg);
  }

  // THe reverse function of Pareto function
  int64_t ParetoCdfInversion(double u, double theta, double k, double sigma) {
    double ret;
    if (k == 0.0) {
      ret = theta - sigma * std::log(u);
    } else {
      ret = theta + sigma * (std::pow(u, -1 * k) - 1) / k;
    }
    return static_cast<int64_t>(ceil(ret));
  }
  // inversion of y=ax^b
  int64_t PowerCdfInversion(double u, double a, double b) {
    double ret;
    ret = std::pow((u / a), (1 / b));
    return static_cast<int64_t>(ceil(ret));
  }

  // Add the noice to the QPS
  double AddNoise(double origin, double noise_ratio) {
    if (noise_ratio < 0.0 || noise_ratio > 1.0) {
      return origin;
    }
    int band_int = static_cast<int>(FLAGS_sine_a);
    double delta = (rand() % band_int - band_int / 2) * noise_ratio;
    if (origin + delta < 0) {
      return origin;
    } else {
      return (origin + delta);
    }
  }

  // decide the query type
  // 0 Get, 1 Put, 2 Seek, 3 SeekForPrev, 4 Delete, 5 SingleDelete, 6 merge
  class QueryDecider {
   public:
    std::vector<int> type_;
    std::vector<double> ratio_;
    int range_;

    QueryDecider() {}
    ~QueryDecider() {}

    Status Initiate(std::vector<double> ratio_input) {
      int range_max = 1000;
      double sum = 0.0;
      for (auto& ratio : ratio_input) {
        sum += ratio;
      }
      range_ = 0;
      for (auto& ratio : ratio_input) {
        range_ += static_cast<int>(ceil(range_max * (ratio / sum)));
        type_.push_back(range_);
        ratio_.push_back(ratio / sum);
      }
      return Status::OK();
    }

    int GetType(int64_t rand_num) {
      if (rand_num < 0) {
        rand_num = rand_num * (-1);
      }
      assert(range_ != 0);
      int pos = static_cast<int>(rand_num % range_);
      for (int i = 0; i < static_cast<int>(type_.size()); i++) {
        if (pos < type_[i]) {
          return i;
        }
      }
      return 0;
    }
  };

  // The graph wokrload mixed with Get, Put, Iterator
  void MixGraph(ThreadState* thread) {
    int64_t read = 0;  // including single gets and Next of iterators
    int64_t gets = 0;
    int64_t puts = 0;
    int64_t found = 0;
    int64_t seek = 0;
    int64_t seek_found = 0;
    int64_t bytes = 0;
    const int64_t default_value_max = 64 * 1024 * 1024;
    int64_t value_max = default_value_max;
    int64_t scan_len_max = FLAGS_mix_max_scan_len;
    double write_rate = 1000000.0;
    double read_rate = 1000000.0;
    std::vector<double> ratio{FLAGS_mix_get_ratio, FLAGS_mix_put_ratio,
                              FLAGS_mix_seek_ratio};
    char value_buffer[default_value_max];
    QueryDecider query;
    RandomGenerator gen;
    Status s;
    if (value_max > FLAGS_mix_max_value_size) {
      value_max = FLAGS_mix_max_value_size;
    }

    ReadOptions options(FLAGS_verify_checksum, true);
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;
    query.Initiate(ratio);

    // the limit of qps initiation
    if (FLAGS_sine_a != 0 || FLAGS_sine_d != 0) {
      thread->shared->read_rate_limiter.reset(NewGenericRateLimiter(
          read_rate, 100000 /* refill_period_us */, 10 /* fairness */,
          RateLimiter::Mode::kReadsOnly));
      thread->shared->write_rate_limiter.reset(
          NewGenericRateLimiter(write_rate));
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      int64_t rand_v, key_rand, key_seed;
      rand_v = GetRandomKey(&thread->rand) % FLAGS_num;
      double u = static_cast<double>(rand_v) / FLAGS_num;
      key_seed = PowerCdfInversion(u, FLAGS_key_dist_a, FLAGS_key_dist_b);
      Random64 rand(key_seed);
      key_rand = static_cast<int64_t>(rand.Next()) % FLAGS_num;
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      int query_type = query.GetType(rand_v);

      // change the qps
      uint64_t now = FLAGS_env->NowMicros();
      uint64_t usecs_since_last;
      if (now > thread->stats.GetSineInterval()) {
        usecs_since_last = now - thread->stats.GetSineInterval();
      } else {
        usecs_since_last = 0;
      }

      if (usecs_since_last >
          (FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000})) {
        double usecs_since_start =
            static_cast<double>(now - thread->stats.GetStart());
        thread->stats.ResetSineInterval();
        double mix_rate_with_noise = AddNoise(
            SineRate(usecs_since_start / 1000000.0), FLAGS_sine_mix_rate_noise);
        read_rate = mix_rate_with_noise * (query.ratio_[0] + query.ratio_[2]);
        write_rate =
            mix_rate_with_noise * query.ratio_[1] * FLAGS_mix_ave_kv_size;

        thread->shared->write_rate_limiter.reset(
            NewGenericRateLimiter(write_rate));
        thread->shared->read_rate_limiter.reset(NewGenericRateLimiter(
            read_rate,
            FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000}, 10,
            RateLimiter::Mode::kReadsOnly));
      }
      // Start the query
      if (query_type == 0) {
        // the Get query
        gets++;
        read++;
        if (FLAGS_num_column_families > 1) {
          s = db_with_cfh->db->Get(options, db_with_cfh->GetCfh(key_rand), key,
                                   &pinnable_val);
        } else {
          pinnable_val.Reset();
          s = db_with_cfh->db->Get(options,
                                   db_with_cfh->db->DefaultColumnFamily(), key,
                                   &pinnable_val);
        }

        if (s.ok()) {
          found++;
          bytes += key.size() + pinnable_val.size();
        } else if (!s.IsNotFound()) {
          fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
          abort();
        }

        if (thread->shared->read_rate_limiter.get() != nullptr &&
            read % 256 == 255) {
          thread->shared->read_rate_limiter->Request(
              256, Env::IO_HIGH, nullptr /* stats */,
              RateLimiter::OpType::kRead);
        }

      } else if (query_type == 1) {
        // the Put query
        puts++;
        int64_t value_size = ParetoCdfInversion(
            u, FLAGS_value_theta, FLAGS_value_k, FLAGS_value_sigma);
        if (value_size < 0) {
          value_size = 10;
        } else if (value_size > value_max) {
          value_size = value_size % value_max;
        }
        s = db_with_cfh->db->Put(
            write_options_, key,
            gen.Generate(static_cast<unsigned int>(value_size)));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }

        if (thread->shared->write_rate_limiter) {
          thread->shared->write_rate_limiter->Request(
              key.size() + value_size, Env::IO_HIGH, nullptr /*stats*/,
              RateLimiter::OpType::kWrite);
        }

      } else if (query_type == 2) {
        // Seek query
        if (db_with_cfh->db != nullptr) {
          Iterator* single_iter = nullptr;
          single_iter = db_with_cfh->db->NewIterator(options);
          if (single_iter != nullptr) {
            single_iter->Seek(key);
            seek++;
            read++;
            if (single_iter->Valid() && single_iter->key().compare(key) == 0) {
              seek_found++;
            }
            int64_t scan_length =
                ParetoCdfInversion(u, FLAGS_iter_theta, FLAGS_iter_k,
                                   FLAGS_iter_sigma) %
                scan_len_max;
            for (int64_t j = 0; j < scan_length && single_iter->Valid(); j++) {
              Slice value = single_iter->value();
              memcpy(value_buffer, value.data(),
                     std::min(value.size(), sizeof(value_buffer)));
              bytes += single_iter->key().size() + single_iter->value().size();
              single_iter->Next();
              assert(single_iter->status().ok());
            }
          }
          delete single_iter;
        }
      }
    }
    char msg[256];
    snprintf(msg, sizeof(msg),
             "( Gets:%" PRIu64 " Puts:%" PRIu64 " Seek:%" PRIu64 " of %" PRIu64
             " in %" PRIu64 " found)\n",
             gets, puts, seek, found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  void IteratorCreation(ThreadState* thread) {
    Duration duration(FLAGS_duration, reads_);
    ReadOptions options(FLAGS_verify_checksum, true);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      Iterator* iter = db->NewIterator(options);
      delete iter;
      thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }
  }

  void IteratorCreationWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      IteratorCreation(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void SeekRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    ReadOptions options(FLAGS_verify_checksum, true);
    options.tailing = FLAGS_use_tailing_iterator;

    Iterator* single_iter = nullptr;
    std::vector<Iterator*> multi_iters;
    if (db_.db != nullptr) {
      single_iter = db_.db->NewIterator(options);
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        multi_iters.push_back(db_with_cfh.db->NewIterator(options));
      }
    }

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    std::unique_ptr<const char[]> upper_bound_key_guard;
    Slice upper_bound = AllocateKey(&upper_bound_key_guard);
    std::unique_ptr<const char[]> lower_bound_key_guard;
    Slice lower_bound = AllocateKey(&lower_bound_key_guard);

    Duration duration(FLAGS_duration, reads_);
    char value_buffer[256];
    while (!duration.Done(1)) {
      int64_t seek_pos = thread->rand.Next() % FLAGS_num;
      GenerateKeyFromInt((uint64_t)seek_pos, FLAGS_num, &key);
      if (FLAGS_max_scan_distance != 0) {
        if (FLAGS_reverse_iterator) {
          GenerateKeyFromInt(
              static_cast<uint64_t>(std::max(
                  static_cast<int64_t>(0), seek_pos - FLAGS_max_scan_distance)),
              FLAGS_num, &lower_bound);
          options.iterate_lower_bound = &lower_bound;
        } else {
          GenerateKeyFromInt(
              (uint64_t)std::min(FLAGS_num, seek_pos + FLAGS_max_scan_distance),
              FLAGS_num, &upper_bound);
          options.iterate_upper_bound = &upper_bound;
        }
      }

      if (!FLAGS_use_tailing_iterator) {
        if (db_.db != nullptr) {
          delete single_iter;
          single_iter = db_.db->NewIterator(options);
        } else {
          for (auto iter : multi_iters) {
            delete iter;
          }
          multi_iters.clear();
          for (const auto& db_with_cfh : multi_dbs_) {
            multi_iters.push_back(db_with_cfh.db->NewIterator(options));
          }
        }
      }
      // Pick a Iterator to use
      Iterator* iter_to_use = single_iter;
      if (single_iter == nullptr) {
        iter_to_use = multi_iters[thread->rand.Next() % multi_iters.size()];
      }

      iter_to_use->Seek(key);
      read++;
      if (iter_to_use->Valid() && iter_to_use->key().compare(key) == 0) {
        found++;
      }

      for (int j = 0; j < FLAGS_seek_nexts && iter_to_use->Valid(); ++j) {
        // Copy out iterator's value to make sure we read them.
        Slice value = iter_to_use->value();
        memcpy(value_buffer, value.data(),
               std::min(value.size(), sizeof(value_buffer)));
        bytes += iter_to_use->key().size() + iter_to_use->value().size();

        if (!FLAGS_reverse_iterator) {
          iter_to_use->Next();
        } else {
          iter_to_use->Prev();
        }
        assert(iter_to_use->status().ok());
      }

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(&db_, db_.db, 1, kSeek);
    }
    delete single_iter;
    for (auto iter : multi_iters) {
      delete iter;
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n",
             found, read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  void SeekRandomWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      SeekRandom(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void SeekRandomWhileMerging(ThreadState* thread) {
    if (thread->tid > 0) {
      SeekRandom(thread);
    } else {
      BGWriter(thread, kMerge);
    }
  }

  void DoDelete(ThreadState* thread, bool seq) {
    WriteBatch batch;
    Duration duration(seq ? 0 : FLAGS_duration, deletes_);
    int64_t i = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    while (!duration.Done(entries_per_batch_)) {
      DB* db = SelectDB(thread);
      batch.Clear();
      for (int64_t j = 0; j < entries_per_batch_; ++j) {
        const int64_t k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
        GenerateKeyFromInt(k, FLAGS_num, &key);
        batch.Delete(key);
      }
      auto s = db->Write(write_options_, &batch);
      thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kDelete);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
      i += entries_per_batch_;
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void ReadWhileMerging(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      BGWriter(thread, kMerge);
    }
  }

  void BGWriter(ThreadState* thread, enum OperationType write_merge) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    std::unique_ptr<RateLimiter> write_rate_limiter;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    uint32_t written = 0;
    bool hint_printed = false;

    while (true) {
      DB* db = SelectDB(thread);
      {
        MutexLock l(&thread->shared->mu);
        if (FLAGS_finish_after_writes && written == writes_) {
          fprintf(stderr, "Exiting the writer after %u writes...\n", written);
          break;
        }
        if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
          // Other threads have finished
          if (FLAGS_finish_after_writes) {
            // Wait for the writes to be finished
            if (!hint_printed) {
              fprintf(stderr, "Reads are finished. Have %d more writes to do\n",
                      (int)writes_ - written);
              hint_printed = true;
            }
          } else {
            // Finish the write immediately
            break;
          }
        }
      }

      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      Status s;

      if (write_merge == kWrite) {
        s = db->Put(write_options_, key, gen.Generate(value_size_));
      } else {
        s = db->Merge(write_options_, key, gen.Generate(value_size_));
      }
      written++;

      if (!s.ok()) {
        fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value_size_;
      thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);

      if (FLAGS_benchmark_write_rate_limit > 0) {
        write_rate_limiter->Request(
            entries_per_batch_ * (value_size_ + key_size_), Env::IO_HIGH,
            nullptr /* stats */, RateLimiter::OpType::kWrite);
      }
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadWhileScanning(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      BGScan(thread);
    }
  }

  void BGScan(ThreadState* thread) {
    if (FLAGS_num_multi_db > 0) {
      fprintf(stderr, "Not supporting multiple DBs.\n");
      abort();
    }
    assert(db_.db != nullptr);
    ReadOptions read_options;
    Iterator* iter = db_.db->NewIterator(read_options);

    fprintf(stderr, "num reads to do %" PRIu64 "\n", reads_);
    Duration duration(FLAGS_duration, reads_);
    uint64_t num_seek_to_first = 0;
    uint64_t num_next = 0;
    while (!duration.Done(1)) {
      if (!iter->Valid()) {
        iter->SeekToFirst();
        num_seek_to_first++;
      } else if (!iter->status().ok()) {
        fprintf(stderr, "Iterator error: %s\n",
                iter->status().ToString().c_str());
        abort();
      } else {
        iter->Next();
        num_next++;
      }

      thread->stats.FinishedOps(&db_, db_.db, 1, kSeek);
    }
    delete iter;
  }

  // Given a key K and value V, this puts (K+"0", V), (K+"1", V), (K+"2", V)
  // in DB atomically i.e in a single batch. Also refer GetMany.
  Status PutMany(DB* db, const WriteOptions& writeoptions, const Slice& key,
                 const Slice& value) {
    std::string suffixes[3] = {"2", "1", "0"};
    std::string keys[3];

    WriteBatch batch;
    Status s;
    for (int i = 0; i < 3; i++) {
      keys[i] = key.ToString() + suffixes[i];
      batch.Put(keys[i], value);
    }

    s = db->Write(writeoptions, &batch);
    return s;
  }


  // Given a key K, this deletes (K+"0", V), (K+"1", V), (K+"2", V)
  // in DB atomically i.e in a single batch. Also refer GetMany.
  Status DeleteMany(DB* db, const WriteOptions& writeoptions,
                    const Slice& key) {
    std::string suffixes[3] = {"1", "2", "0"};
    std::string keys[3];

    WriteBatch batch;
    Status s;
    for (int i = 0; i < 3; i++) {
      keys[i] = key.ToString() + suffixes[i];
      batch.Delete(keys[i]);
    }

    s = db->Write(writeoptions, &batch);
    return s;
  }

  // Given a key K and value V, this gets values for K+"0", K+"1" and K+"2"
  // in the same snapshot, and verifies that all the values are identical.
  // ASSUMES that PutMany was used to put (K, V) into the DB.
  Status GetMany(DB* db, const ReadOptions& readoptions, const Slice& key,
                 std::string* value) {
    std::string suffixes[3] = {"0", "1", "2"};
    std::string keys[3];
    Slice key_slices[3];
    std::string values[3];
    ReadOptions readoptionscopy = readoptions;
    readoptionscopy.snapshot = db->GetSnapshot();
    Status s;
    for (int i = 0; i < 3; i++) {
      keys[i] = key.ToString() + suffixes[i];
      key_slices[i] = keys[i];
      s = db->Get(readoptionscopy, key_slices[i], value);
      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "get error: %s\n", s.ToString().c_str());
        values[i] = "";
        // we continue after error rather than exiting so that we can
        // find more errors if any
      } else if (s.IsNotFound()) {
        values[i] = "";
      } else {
        values[i] = *value;
      }
    }
    db->ReleaseSnapshot(readoptionscopy.snapshot);

    if ((values[0] != values[1]) || (values[1] != values[2])) {
      fprintf(stderr, "inconsistent values for key %s: %s, %s, %s\n",
              key.ToString().c_str(), values[0].c_str(), values[1].c_str(),
              values[2].c_str());
      // we continue after error rather than exiting so that we can
      // find more errors if any
    }

    return s;
  }

  // Differs from readrandomwriterandom in the following ways:
  // (a) Uses GetMany/PutMany to read/write key values. Refer to those funcs.
  // (b) Does deletes as well (per FLAGS_deletepercent)
  // (c) In order to achieve high % of 'found' during lookups, and to do
  //     multiple writes (including puts and deletes) it uses upto
  //     FLAGS_numdistinct distinct keys instead of FLAGS_num distinct keys.
  // (d) Does not have a MultiGet option.
  void RandomWithVerify(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int delete_weight = 0;
    int64_t gets_done = 0;
    int64_t puts_done = 0;
    int64_t deletes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    for (int64_t i = 0; i < readwrites_; i++) {
      DB* db = SelectDB(thread);
      if (get_weight == 0 && put_weight == 0 && delete_weight == 0) {
        // one batch completed, reinitialize for next batch
        get_weight = FLAGS_readwritepercent;
        delete_weight = FLAGS_deletepercent;
        put_weight = 100 - get_weight - delete_weight;
      }
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_numdistinct,
          FLAGS_numdistinct, &key);
      if (get_weight > 0) {
        // do all the gets first
        Status s = GetMany(db, options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "getmany error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
        }
        get_weight--;
        gets_done++;
        thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
      } else if (put_weight > 0) {
        // then do all the corresponding number of puts
        // for all the gets we have done earlier
        Status s = PutMany(db, write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "putmany error: %s\n", s.ToString().c_str());
          exit(1);
        }
        put_weight--;
        puts_done++;
        thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
      } else if (delete_weight > 0) {
        Status s = DeleteMany(db, write_options_, key);
        if (!s.ok()) {
          fprintf(stderr, "deletemany error: %s\n", s.ToString().c_str());
          exit(1);
        }
        delete_weight--;
        deletes_done++;
        thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
      }
    }
    char msg[128];
    snprintf(msg, sizeof(msg),
             "( get:%" PRIu64 " put:%" PRIu64 " del:%" PRIu64 " total:%" \
             PRIu64 " found:%" PRIu64 ")",
             gets_done, puts_done, deletes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  // This is different from ReadWhileWriting because it does not use
  // an extra thread.
  void ReadRandomWriteRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      if (get_weight == 0 && put_weight == 0) {
        // one batch completed, reinitialize for next batch
        get_weight = FLAGS_readwritepercent;
        put_weight = 100 - get_weight;
      }
      if (get_weight > 0) {
        // do all the gets first
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
        }
        get_weight--;
        reads_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else  if (put_weight > 0) {
        // then do all the corresponding number of puts
        // for all the gets we have done earlier
        Status s = db->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        put_weight--;
        writes_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kWrite);
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
             " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  //
  // Read-modify-write for random keys
  void UpdateRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t bytes = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);

      auto status = db->Get(options, key, &value);
      if (status.ok()) {
        ++found;
        bytes += key.size() + value.size();
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        abort();
      }

      if (thread->shared->write_rate_limiter) {
        thread->shared->write_rate_limiter->Request(
            key.size() + value_size_, Env::IO_HIGH, nullptr /*stats*/,
            RateLimiter::OpType::kWrite);
      }

      Status s = db->Put(write_options_, key, gen.Generate(value_size_));
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value_size_;
      thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
    }
    char msg[100];
    snprintf(msg, sizeof(msg),
             "( updates:%" PRIu64 " found:%" PRIu64 ")", readwrites_, found);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read-XOR-write for random keys. Xors the existing value with a randomly
  // generated value, and stores the result. Assuming A in the array of bytes
  // representing the existing value, we generate an array B of the same size,
  // then compute C = A^B as C[i]=A[i]^B[i], and store C
  void XORUpdateRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string existing_value;
    int64_t found = 0;
    Duration duration(FLAGS_duration, readwrites_);

    BytesXOROperator xor_operator;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);

      auto status = db->Get(options, key, &existing_value);
      if (status.ok()) {
        ++found;
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        exit(1);
      }

      Slice value = gen.Generate(value_size_);
      std::string new_value;

      if (status.ok()) {
        Slice existing_value_slice = Slice(existing_value);
        xor_operator.XOR(&existing_value_slice, value, &new_value);
      } else {
        xor_operator.XOR(nullptr, value, &new_value);
      }

      Status s = db->Put(write_options_, key, Slice(new_value));
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      thread->stats.FinishedOps(nullptr, db, 1);
    }
    char msg[100];
    snprintf(msg, sizeof(msg),
             "( updates:%" PRIu64 " found:%" PRIu64 ")", readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  // Read-modify-write for random keys.
  // Each operation causes the key grow by value_size (simulating an append).
  // Generally used for benchmarking against merges of similar type
  void AppendRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // The number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);

      auto status = db->Get(options, key, &value);
      if (status.ok()) {
        ++found;
        bytes += key.size() + value.size();
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        abort();
      } else {
        // If not existing, then just assume an empty string of data
        value.clear();
      }

      // Update the value (by appending data)
      Slice operand = gen.Generate(value_size_);
      if (value.size() > 0) {
        // Use a delimiter to match the semantics for StringAppendOperator
        value.append(1,',');
      }
      value.append(operand.data(), operand.size());

      // Write back to the database
      Status s = db->Put(write_options_, key, value);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value.size();
      thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 " found:%" PRIu64 ")",
            readwrites_, found);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read-modify-write for random keys (using MergeOperator)
  // The merge operator to use should be defined by FLAGS_merge_operator
  // Adjust FLAGS_value_size so that the keys are reasonable for this operator
  // Assumes that the merge operator is non-null (i.e.: is well-defined)
  //
  // For example, use FLAGS_merge_operator="uint64add" and FLAGS_value_size=8
  // to simulate random additions over 64-bit integers using merge.
  //
  // The number of merges on the same key can be controlled by adjusting
  // FLAGS_merge_keys.
  void MergeRandom(ThreadState* thread) {
    RandomGenerator gen;
    int64_t bytes = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // The number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      int64_t key_rand = thread->rand.Next() % merge_keys_;
      GenerateKeyFromInt(key_rand, merge_keys_, &key);

      Status s;
      if (FLAGS_num_column_families > 1) {
        s = db_with_cfh->db->Merge(write_options_,
                                   db_with_cfh->GetCfh(key_rand), key,
                                   gen.Generate(value_size_));
      } else {
        s = db_with_cfh->db->Merge(write_options_,
                                   db_with_cfh->db->DefaultColumnFamily(), key,
                                   gen.Generate(value_size_));
      }

      if (!s.ok()) {
        fprintf(stderr, "merge error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + value_size_;
      thread->stats.FinishedOps(nullptr, db_with_cfh->db, 1, kMerge);
    }

    // Print some statistics
    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 ")", readwrites_);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read and merge random keys. The amount of reads and merges are controlled
  // by adjusting FLAGS_num and FLAGS_mergereadpercent. The number of distinct
  // keys (and thus also the number of reads and merges on the same key) can be
  // adjusted with FLAGS_merge_keys.
  //
  // As with MergeRandom, the merge operator to use should be defined by
  // FLAGS_merge_operator.
  void ReadRandomMergeRandom(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    RandomGenerator gen;
    std::string value;
    int64_t num_hits = 0;
    int64_t num_gets = 0;
    int64_t num_merges = 0;
    size_t max_length = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % merge_keys_, merge_keys_, &key);

      bool do_merge = int(thread->rand.Next() % 100) < FLAGS_mergereadpercent;

      if (do_merge) {
        Status s = db->Merge(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "merge error: %s\n", s.ToString().c_str());
          exit(1);
        }
        num_merges++;
        thread->stats.FinishedOps(nullptr, db, 1, kMerge);
      } else {
        Status s = db->Get(options, key, &value);
        if (value.length() > max_length)
          max_length = value.length();

        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          num_hits++;
        }
        num_gets++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      }
    }

    char msg[100];
    snprintf(msg, sizeof(msg),
             "(reads:%" PRIu64 " merges:%" PRIu64 " total:%" PRIu64
             " hits:%" PRIu64 " maxlength:%" ROCKSDB_PRIszt ")",
             num_gets, num_merges, readwrites_, num_hits, max_length);
    thread->stats.AddMessage(msg);
  }

  void WriteSeqSeekSeq(ThreadState* thread) {
    writes_ = FLAGS_num;
    DoWrite(thread, SEQUENTIAL);
    // exclude writes from the ops/sec calculation
    thread->stats.Start(thread->tid);

    DB* db = SelectDB(thread);
    std::unique_ptr<Iterator> iter(
      db->NewIterator(ReadOptions(FLAGS_verify_checksum, true)));

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    for (int64_t i = 0; i < FLAGS_num; ++i) {
      GenerateKeyFromInt(i, FLAGS_num, &key);
      iter->Seek(key);
      assert(iter->Valid() && iter->key() == key);
      thread->stats.FinishedOps(nullptr, db, 1, kSeek);

      for (int j = 0; j < FLAGS_seek_nexts && i + 1 < FLAGS_num; ++j) {
        if (!FLAGS_reverse_iterator) {
          iter->Next();
        } else {
          iter->Prev();
        }
        GenerateKeyFromInt(++i, FLAGS_num, &key);
        assert(iter->Valid() && iter->key() == key);
        thread->stats.FinishedOps(nullptr, db, 1, kSeek);
      }

      iter->Seek(key);
      assert(iter->Valid() && iter->key() == key);
      thread->stats.FinishedOps(nullptr, db, 1, kSeek);
    }
  }

#ifndef ROCKSDB_LITE
  // This benchmark stress tests Transactions.  For a given --duration (or
  // total number of --writes, a Transaction will perform a read-modify-write
  // to increment the value of a key in each of N(--transaction-sets) sets of
  // keys (where each set has --num keys).  If --threads is set, this will be
  // done in parallel.
  //
  // To test transactions, use --transaction_db=true.  Not setting this
  // parameter
  // will run the same benchmark without transactions.
  //
  // RandomTransactionVerify() will then validate the correctness of the results
  // by checking if the sum of all keys in each set is the same.
  void RandomTransaction(ThreadState* thread) {
    ReadOptions options(FLAGS_verify_checksum, true);
    Duration duration(FLAGS_duration, readwrites_);
    ReadOptions read_options(FLAGS_verify_checksum, true);
    uint16_t num_prefix_ranges = static_cast<uint16_t>(FLAGS_transaction_sets);
    uint64_t transactions_done = 0;

    if (num_prefix_ranges == 0 || num_prefix_ranges > 9999) {
      fprintf(stderr, "invalid value for transaction_sets\n");
      abort();
    }

    TransactionOptions txn_options;
    txn_options.lock_timeout = FLAGS_transaction_lock_timeout;
    txn_options.set_snapshot = FLAGS_transaction_set_snapshot;

    RandomTransactionInserter inserter(&thread->rand, write_options_,
                                       read_options, FLAGS_num,
                                       num_prefix_ranges);

    if (FLAGS_num_multi_db > 1) {
      fprintf(stderr,
              "Cannot run RandomTransaction benchmark with "
              "FLAGS_multi_db > 1.");
      abort();
    }

    while (!duration.Done(1)) {
      bool success;

      // RandomTransactionInserter will attempt to insert a key for each
      // # of FLAGS_transaction_sets
      if (FLAGS_optimistic_transaction_db) {
        success = inserter.OptimisticTransactionDBInsert(db_.opt_txn_db);
      } else if (FLAGS_transaction_db) {
        TransactionDB* txn_db = reinterpret_cast<TransactionDB*>(db_.db);
        success = inserter.TransactionDBInsert(txn_db, txn_options);
      } else {
        success = inserter.DBInsert(db_.db);
      }

      if (!success) {
        fprintf(stderr, "Unexpected error: %s\n",
                inserter.GetLastStatus().ToString().c_str());
        abort();
      }

      thread->stats.FinishedOps(nullptr, db_.db, 1, kOthers);
      transactions_done++;
    }

    char msg[100];
    if (FLAGS_optimistic_transaction_db || FLAGS_transaction_db) {
      snprintf(msg, sizeof(msg),
               "( transactions:%" PRIu64 " aborts:%" PRIu64 ")",
               transactions_done, inserter.GetFailureCount());
    } else {
      snprintf(msg, sizeof(msg), "( batches:%" PRIu64 " )", transactions_done);
    }
    thread->stats.AddMessage(msg);

    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
    thread->stats.AddBytes(static_cast<int64_t>(inserter.GetBytesInserted()));
  }

  // Verifies consistency of data after RandomTransaction() has been run.
  // Since each iteration of RandomTransaction() incremented a key in each set
  // by the same value, the sum of the keys in each set should be the same.
  void RandomTransactionVerify() {
    if (!FLAGS_transaction_db && !FLAGS_optimistic_transaction_db) {
      // transactions not used, nothing to verify.
      return;
    }

    Status s =
        RandomTransactionInserter::Verify(db_.db,
                            static_cast<uint16_t>(FLAGS_transaction_sets));

    if (s.ok()) {
      fprintf(stdout, "RandomTransactionVerify Success.\n");
    } else {
      fprintf(stdout, "RandomTransactionVerify FAILED!!\n");
    }
  }
#endif  // ROCKSDB_LITE

  // Writes and deletes random keys without overwriting keys.
  //
  // This benchmark is intended to partially replicate the behavior of MyRocks
  // secondary indices: All data is stored in keys and updates happen by
  // deleting the old version of the key and inserting the new version.
  void RandomReplaceKeys(ThreadState* thread) {
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::vector<uint32_t> counters(FLAGS_numdistinct, 0);
    size_t max_counter = 50;
    RandomGenerator gen;

    Status s;
    DB* db = SelectDB(thread);
    for (int64_t i = 0; i < FLAGS_numdistinct; i++) {
      GenerateKeyFromInt(i * max_counter, FLAGS_num, &key);
      s = db->Put(write_options_, key, gen.Generate(value_size_));
      if (!s.ok()) {
        fprintf(stderr, "Operation failed: %s\n", s.ToString().c_str());
        exit(1);
      }
    }

    db->GetSnapshot();

    std::default_random_engine generator;
    std::normal_distribution<double> distribution(FLAGS_numdistinct / 2.0,
                                                  FLAGS_stddev);
    Duration duration(FLAGS_duration, FLAGS_num);
    while (!duration.Done(1)) {
      int64_t rnd_id = static_cast<int64_t>(distribution(generator));
      int64_t key_id = std::max(std::min(FLAGS_numdistinct - 1, rnd_id),
                                static_cast<int64_t>(0));
      GenerateKeyFromInt(key_id * max_counter + counters[key_id], FLAGS_num,
                         &key);
      s = FLAGS_use_single_deletes ? db->SingleDelete(write_options_, key)
                                   : db->Delete(write_options_, key);
      if (s.ok()) {
        counters[key_id] = (counters[key_id] + 1) % max_counter;
        GenerateKeyFromInt(key_id * max_counter + counters[key_id], FLAGS_num,
                           &key);
        s = db->Put(write_options_, key, Slice());
      }

      if (!s.ok()) {
        fprintf(stderr, "Operation failed: %s\n", s.ToString().c_str());
        exit(1);
      }

      thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }

    char msg[200];
    snprintf(msg, sizeof(msg),
             "use single deletes: %d, "
             "standard deviation: %lf\n",
             FLAGS_use_single_deletes, FLAGS_stddev);
    thread->stats.AddMessage(msg);
  }

  void TimeSeriesReadOrDelete(ThreadState* thread, bool do_deletion) {
    ReadOptions options(FLAGS_verify_checksum, true);
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;

    Iterator* iter = nullptr;
    // Only work on single database
    assert(db_.db != nullptr);
    iter = db_.db->NewIterator(options);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    char value_buffer[256];
    while (true) {
      {
        MutexLock l(&thread->shared->mu);
        if (thread->shared->num_done >= 1) {
          // Write thread have finished
          break;
        }
      }
      if (!FLAGS_use_tailing_iterator) {
        delete iter;
        iter = db_.db->NewIterator(options);
      }
      // Pick a Iterator to use

      int64_t key_id = thread->rand.Next() % FLAGS_key_id_range;
      GenerateKeyFromInt(key_id, FLAGS_num, &key);
      // Reset last 8 bytes to 0
      char* start = const_cast<char*>(key.data());
      start += key.size() - 8;
      memset(start, 0, 8);
      ++read;

      bool key_found = false;
      // Seek the prefix
      for (iter->Seek(key); iter->Valid() && iter->key().starts_with(key);
           iter->Next()) {
        key_found = true;
        // Copy out iterator's value to make sure we read them.
        if (do_deletion) {
          bytes += iter->key().size();
          if (KeyExpired(timestamp_emulator_.get(), iter->key())) {
            thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
            db_.db->Delete(write_options_, iter->key());
          } else {
            break;
          }
        } else {
          bytes += iter->key().size() + iter->value().size();
          thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
          Slice value = iter->value();
          memcpy(value_buffer, value.data(),
                 std::min(value.size(), sizeof(value_buffer)));

          assert(iter->status().ok());
        }
      }
      found += key_found;

      if (thread->shared->read_rate_limiter.get() != nullptr) {
        thread->shared->read_rate_limiter->Request(
            1, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }
    }
    delete iter;

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found,
             read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
    if (FLAGS_perf_level > rocksdb::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
  }

  void TimeSeriesWrite(ThreadState* thread) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<RateLimiter> write_rate_limiter;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    Duration duration(FLAGS_duration, writes_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      uint64_t key_id = thread->rand.Next() % FLAGS_key_id_range;
      // Write key id
      GenerateKeyFromInt(key_id, FLAGS_num, &key);
      // Write timestamp

      char* start = const_cast<char*>(key.data());
      char* pos = start + 8;
      int bytes_to_fill =
          std::min(key_size_ - static_cast<int>(pos - start), 8);
      uint64_t timestamp_value = timestamp_emulator_->Get();
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (timestamp_value >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&timestamp_value), bytes_to_fill);
      }

      timestamp_emulator_->Inc();

      Status s;

      s = db->Put(write_options_, key, gen.Generate(value_size_));

      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes = key.size() + value_size_;
      thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
      thread->stats.AddBytes(bytes);

      if (FLAGS_benchmark_write_rate_limit > 0) {
        write_rate_limiter->Request(
            entries_per_batch_ * (value_size_ + key_size_), Env::IO_HIGH,
            nullptr /* stats */, RateLimiter::OpType::kWrite);
      }
    }
  }

  void TimeSeries(ThreadState* thread) {
    if (thread->tid > 0) {
      bool do_deletion = FLAGS_expire_style == "delete" &&
                         thread->tid <= FLAGS_num_deletion_threads;
      TimeSeriesReadOrDelete(thread, do_deletion);
    } else {
      TimeSeriesWrite(thread);
      thread->stats.Stop();
      thread->stats.Report("timeseries write");
    }
  }

  void Compact(ThreadState* thread) {
    DB* db = SelectDB(thread);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    db->CompactRange(cro, nullptr, nullptr);
  }

  void CompactAll() {
    if (db_.db != nullptr) {
      db_.db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    }
  }

  void ResetStats() {
    if (db_.db != nullptr) {
      db_.db->ResetStats();
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->ResetStats();
    }
  }

  void PrintStats(const char* key) {
    if (db_.db != nullptr) {
      PrintStats(db_.db, key, false);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStats(db_with_cfh.db, key, true);
    }
  }

  void PrintStats(DB* db, const char* key, bool print_header = false) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }
    std::string stats;
    if (!db->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
  }

  void Replay(ThreadState* thread) {
    if (db_.db != nullptr) {
      Replay(thread, &db_);
    }
  }

  void Replay(ThreadState* /*thread*/, DBWithColumnFamilies* db_with_cfh) {
    Status s;
    std::unique_ptr<TraceReader> trace_reader;
    s = NewFileTraceReader(FLAGS_env, EnvOptions(), FLAGS_trace_file,
                           &trace_reader);
    if (!s.ok()) {
      fprintf(
          stderr,
          "Encountered an error creating a TraceReader from the trace file. "
          "Error: %s\n",
          s.ToString().c_str());
      exit(1);
    }
    Replayer replayer(db_with_cfh->db, db_with_cfh->cfh,
                      std::move(trace_reader));
    s = replayer.Replay();
    if (s.ok()) {
      fprintf(stdout, "Replay started from trace_file: %s\n",
              FLAGS_trace_file.c_str());
    } else {
      fprintf(stderr, "Starting replay failed. Error: %s\n",
              s.ToString().c_str());
    }
  }
};

int db_bench_tool(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  static bool initialized = false;
  if (!initialized) {
    SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                    " [OPTIONS]...");
    initialized = true;
  }
  ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_compaction_style_e = (rocksdb::CompactionStyle) FLAGS_compaction_style;
#ifndef ROCKSDB_LITE
  if (FLAGS_statistics && !FLAGS_statistics_string.empty()) {
    fprintf(stderr,
            "Cannot provide both --statistics and --statistics_string.\n");
    exit(1);
  }
  if (!FLAGS_statistics_string.empty()) {
    std::unique_ptr<Statistics> custom_stats_guard;
    dbstats.reset(NewCustomObject<Statistics>(FLAGS_statistics_string,
                                              &custom_stats_guard));
    custom_stats_guard.release();
    if (dbstats == nullptr) {
      fprintf(stderr, "No Statistics registered matching string: %s\n",
              FLAGS_statistics_string.c_str());
      exit(1);
    }
  }
#endif  // ROCKSDB_LITE
  if (FLAGS_statistics) {
    dbstats = rocksdb::CreateDBStatistics();
  }
  FLAGS_compaction_pri_e = (rocksdb::CompactionPri)FLAGS_compaction_pri;

  std::vector<std::string> fanout = rocksdb::StringSplit(
      FLAGS_max_bytes_for_level_multiplier_additional, ',');
  for (size_t j = 0; j < fanout.size(); j++) {
    FLAGS_max_bytes_for_level_multiplier_additional_v.push_back(
#ifndef CYGWIN
        std::stoi(fanout[j]));
#else
        stoi(fanout[j]));
#endif
  }

  FLAGS_compression_type_e =
    StringToCompressionType(FLAGS_compression_type.c_str());

#ifndef ROCKSDB_LITE
  std::unique_ptr<Env> custom_env_guard;
  if (!FLAGS_hdfs.empty() && !FLAGS_env_uri.empty()) {
    fprintf(stderr, "Cannot provide both --hdfs and --env_uri.\n");
    exit(1);
  } else if (!FLAGS_env_uri.empty()) {
    FLAGS_env = NewCustomObject<Env>(FLAGS_env_uri, &custom_env_guard);
    if (FLAGS_env == nullptr) {
      fprintf(stderr, "No Env registered for URI: %s\n", FLAGS_env_uri.c_str());
      exit(1);
    }
  }
#endif  // ROCKSDB_LITE
  if (!FLAGS_hdfs.empty()) {
    FLAGS_env  = new rocksdb::HdfsEnv(FLAGS_hdfs);
  }

  if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "NONE"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::NONE;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "NORMAL"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::NORMAL;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "SEQUENTIAL"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::SEQUENTIAL;
  else if (!strcasecmp(FLAGS_compaction_fadvice.c_str(), "WILLNEED"))
    FLAGS_compaction_fadvice_e = rocksdb::Options::WILLNEED;
  else {
    fprintf(stdout, "Unknown compaction fadvice:%s\n",
            FLAGS_compaction_fadvice.c_str());
  }

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // Note options sanitization may increase thread pool sizes according to
  // max_background_flushes/max_background_compactions/max_background_jobs
  FLAGS_env->SetBackgroundThreads(FLAGS_num_high_pri_threads,
                                  rocksdb::Env::Priority::HIGH);
  FLAGS_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
                                  rocksdb::Env::Priority::BOTTOM);
  FLAGS_env->SetBackgroundThreads(FLAGS_num_low_pri_threads,
                                  rocksdb::Env::Priority::LOW);

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    rocksdb::Env::Default()->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path;
  }

  if (FLAGS_stats_interval_seconds > 0) {
    // When both are set then FLAGS_stats_interval determines the frequency
    // at which the timer is checked for FLAGS_stats_interval_seconds
    FLAGS_stats_interval = 1000;
  }

  rocksdb::Benchmark benchmark;
  benchmark.Run();

#ifndef ROCKSDB_LITE
  if (FLAGS_print_malloc_stats) {
    std::string stats_string;
    rocksdb::DumpMallocStats(&stats_string);
    fprintf(stdout, "Malloc stats:\n%s\n", stats_string.c_str());
  }
#endif  // ROCKSDB_LITE

  return 0;
}
}  // namespace rocksdb
#endif
