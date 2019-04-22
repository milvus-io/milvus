// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
// The following set of functions provide a way to construct RocksDB Options
// from a string or a string-to-string map.  Here're the general rule of
// setting option values from strings by type.  Some RocksDB types are also
// supported in these APIs.  Please refer to the comment of the function itself
// to find more information about how to config those RocksDB types.
//
// * Strings:
//   Strings will be used as values directly without any truncating or
//   trimming.
//
// * Booleans:
//   - "true" or "1" => true
//   - "false" or "0" => false.
//   [Example]:
//   - {"optimize_filters_for_hits", "1"} in GetColumnFamilyOptionsFromMap, or
//   - "optimize_filters_for_hits=true" in GetColumnFamilyOptionsFromString.
//
// * Integers:
//   Integers are converted directly from string, in addition to the following
//   units that we support:
//   - 'k' or 'K' => 2^10
//   - 'm' or 'M' => 2^20
//   - 'g' or 'G' => 2^30
//   - 't' or 'T' => 2^40  // only for unsigned int with sufficient bits.
//   [Example]:
//   - {"arena_block_size", "19G"} in GetColumnFamilyOptionsFromMap, or
//   - "arena_block_size=19G" in GetColumnFamilyOptionsFromString.
//
// * Doubles / Floating Points:
//   Doubles / Floating Points are converted directly from string.  Note that
//   currently we do not support units.
//   [Example]:
//   - {"hard_rate_limit", "2.1"} in GetColumnFamilyOptionsFromMap, or
//   - "hard_rate_limit=2.1" in GetColumnFamilyOptionsFromString.
// * Array / Vectors:
//   An array is specified by a list of values, where ':' is used as
//   the delimiter to separate each value.
//   [Example]:
//   - {"compression_per_level", "kNoCompression:kSnappyCompression"}
//     in GetColumnFamilyOptionsFromMap, or
//   - "compression_per_level=kNoCompression:kSnappyCompression" in
//     GetColumnFamilyOptionsFromMapString
// * Enums:
//   The valid values of each enum are identical to the names of its constants.
//   [Example]:
//   - CompressionType: valid values are "kNoCompression",
//     "kSnappyCompression", "kZlibCompression", "kBZip2Compression", ...
//   - CompactionStyle: valid values are "kCompactionStyleLevel",
//     "kCompactionStyleUniversal", "kCompactionStyleFIFO", and
//     "kCompactionStyleNone".
//

// Take a default ColumnFamilyOptions "base_options" in addition to a
// map "opts_map" of option name to option value to construct the new
// ColumnFamilyOptions "new_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in ColumnFOptions:
//
// * table_factory:
//   table_factory can be configured using our custom nested-option syntax.
//
//   {option_a=value_a; option_b=value_b; option_c=value_c; ... }
//
//   A nested option is enclosed by two curly braces, within which there are
//   multiple option assignments.  Each assignment is of the form
//   "variable_name=value;".
//
//   Currently we support the following types of TableFactory:
//   - BlockBasedTableFactory:
//     Use name "block_based_table_factory" to initialize table_factory with
//     BlockBasedTableFactory.  Its BlockBasedTableFactoryOptions can be
//     configured using the nested-option syntax.
//     [Example]:
//     * {"block_based_table_factory", "{block_cache=1M;block_size=4k;}"}
//       is equivalent to assigning table_factory with a BlockBasedTableFactory
//       that has 1M LRU block-cache with block size equals to 4k:
//         ColumnFamilyOptions cf_opt;
//         BlockBasedTableOptions blk_opt;
//         blk_opt.block_cache = NewLRUCache(1 * 1024 * 1024);
//         blk_opt.block_size = 4 * 1024;
//         cf_opt.table_factory.reset(NewBlockBasedTableFactory(blk_opt));
//   - PlainTableFactory:
//     Use name "plain_table_factory" to initialize table_factory with
//     PlainTableFactory.  Its PlainTableFactoryOptions can be configured using
//     the nested-option syntax.
//     [Example]:
//     * {"plain_table_factory", "{user_key_len=66;bloom_bits_per_key=20;}"}
//
// * memtable_factory:
//   Use "memtable" to config memtable_factory.  Here are the supported
//   memtable factories:
//   - SkipList:
//     Pass "skip_list:<lookahead>" to config memtable to use SkipList,
//     or simply "skip_list" to use the default SkipList.
//     [Example]:
//     * {"memtable", "skip_list:5"} is equivalent to setting
//       memtable to SkipListFactory(5).
//   - PrefixHash:
//     Pass "prfix_hash:<hash_bucket_count>" to config memtable
//     to use PrefixHash, or simply "prefix_hash" to use the default
//     PrefixHash.
//     [Example]:
//     * {"memtable", "prefix_hash:1000"} is equivalent to setting
//       memtable to NewHashSkipListRepFactory(hash_bucket_count).
//   - HashLinkedList:
//     Pass "hash_linkedlist:<hash_bucket_count>" to config memtable
//     to use HashLinkedList, or simply "hash_linkedlist" to use the default
//     HashLinkedList.
//     [Example]:
//     * {"memtable", "hash_linkedlist:1000"} is equivalent to
//       setting memtable to NewHashLinkListRepFactory(1000).
//   - VectorRepFactory:
//     Pass "vector:<count>" to config memtable to use VectorRepFactory,
//     or simply "vector" to use the default Vector memtable.
//     [Example]:
//     * {"memtable", "vector:1024"} is equivalent to setting memtable
//       to VectorRepFactory(1024).
//   - HashCuckooRepFactory:
//     Pass "cuckoo:<write_buffer_size>" to use HashCuckooRepFactory with the
//     specified write buffer size, or simply "cuckoo" to use the default
//     HashCuckooRepFactory.
//     [Example]:
//     * {"memtable", "cuckoo:1024"} is equivalent to setting memtable
//       to NewHashCuckooRepFactory(1024).
//
//  * compression_opts:
//    Use "compression_opts" to config compression_opts.  The value format
//    is of the form "<window_bits>:<level>:<strategy>:<max_dict_bytes>".
//    [Example]:
//    * {"compression_opts", "4:5:6:7"} is equivalent to setting:
//        ColumnFamilyOptions cf_opt;
//        cf_opt.compression_opts.window_bits = 4;
//        cf_opt.compression_opts.level = 5;
//        cf_opt.compression_opts.strategy = 6;
//        cf_opt.compression_opts.max_dict_bytes = 7;
//
// @param base_options the default options of the output "new_options".
// @param opts_map an option name to value map for specifying how "new_options"
//     should be set.
// @param new_options the resulting options based on "base_options" with the
//     change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_options" will be set to "base_options".
Status GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped = false,
    bool ignore_unknown_options = false);

// Take a default DBOptions "base_options" in addition to a
// map "opts_map" of option name to option value to construct the new
// DBOptions "new_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in DBOptions:
//
// * rate_limiter_bytes_per_sec:
//   RateLimiter can be configured directly by specifying its bytes_per_sec.
//   [Example]:
//   - Passing {"rate_limiter_bytes_per_sec", "1024"} is equivalent to
//     passing NewGenericRateLimiter(1024) to rate_limiter_bytes_per_sec.
//
// @param base_options the default options of the output "new_options".
// @param opts_map an option name to value map for specifying how "new_options"
//     should be set.
// @param new_options the resulting options based on "base_options" with the
//     change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_options" will be set to "base_options".
Status GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped = false,
    bool ignore_unknown_options = false);

// Take a default BlockBasedTableOptions "table_options" in addition to a
// map "opts_map" of option name to option value to construct the new
// BlockBasedTableOptions "new_table_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in BlockBasedTableOptions:
//
// * filter_policy:
//   We currently only support the following FilterPolicy in the convenience
//   functions:
//   - BloomFilter: use "bloomfilter:[bits_per_key]:[use_block_based_builder]"
//     to specify BloomFilter.  The above string is equivalent to calling
//     NewBloomFilterPolicy(bits_per_key, use_block_based_builder).
//     [Example]:
//     - Pass {"filter_policy", "bloomfilter:4:true"} in
//       GetBlockBasedTableOptionsFromMap to use a BloomFilter with 4-bits
//       per key and use_block_based_builder enabled.
//
// * block_cache / block_cache_compressed:
//   We currently only support LRU cache in the GetOptions API.  The LRU
//   cache can be set by directly specifying its size.
//   [Example]:
//   - Passing {"block_cache", "1M"} in GetBlockBasedTableOptionsFromMap is
//     equivalent to setting block_cache using NewLRUCache(1024 * 1024).
//
// @param table_options the default options of the output "new_table_options".
// @param opts_map an option name to value map for specifying how
//     "new_table_options" should be set.
// @param new_table_options the resulting options based on "table_options"
//     with the change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_table_options" will be set to
//     "table_options".
Status GetBlockBasedTableOptionsFromMap(
    const BlockBasedTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    BlockBasedTableOptions* new_table_options,
    bool input_strings_escaped = false, bool ignore_unknown_options = false);

// Take a default PlainTableOptions "table_options" in addition to a
// map "opts_map" of option name to option value to construct the new
// PlainTableOptions "new_table_options".
//
// @param table_options the default options of the output "new_table_options".
// @param opts_map an option name to value map for specifying how
//     "new_table_options" should be set.
// @param new_table_options the resulting options based on "table_options"
//     with the change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_table_options" will be set to
//     "table_options".
Status GetPlainTableOptionsFromMap(
    const PlainTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    PlainTableOptions* new_table_options, bool input_strings_escaped = false,
    bool ignore_unknown_options = false);

// Take a string representation of option names and  values, apply them into the
// base_options, and return the new options as a result. The string has the
// following format:
//   "write_buffer_size=1024;max_write_buffer_number=2"
// Nested options config is also possible. For example, you can define
// BlockBasedTableOptions as part of the string for block-based table factory:
//   "write_buffer_size=1024;block_based_table_factory={block_size=4k};"
//   "max_write_buffer_num=2"
Status GetColumnFamilyOptionsFromString(
    const ColumnFamilyOptions& base_options,
    const std::string& opts_str,
    ColumnFamilyOptions* new_options);

Status GetDBOptionsFromString(
    const DBOptions& base_options,
    const std::string& opts_str,
    DBOptions* new_options);

Status GetStringFromDBOptions(std::string* opts_str,
                              const DBOptions& db_options,
                              const std::string& delimiter = ";  ");

Status GetStringFromColumnFamilyOptions(std::string* opts_str,
                                        const ColumnFamilyOptions& cf_options,
                                        const std::string& delimiter = ";  ");

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type);

std::vector<CompressionType> GetSupportedCompressions();

Status GetBlockBasedTableOptionsFromString(
    const BlockBasedTableOptions& table_options,
    const std::string& opts_str,
    BlockBasedTableOptions* new_table_options);

Status GetPlainTableOptionsFromString(
    const PlainTableOptions& table_options,
    const std::string& opts_str,
    PlainTableOptions* new_table_options);

Status GetMemTableRepFactoryFromString(
    const std::string& opts_str,
    std::unique_ptr<MemTableRepFactory>* new_mem_factory);

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options);

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map);

// Request stopping background work, if wait is true wait until it's done
void CancelAllBackgroundWork(DB* db, bool wait = false);

// Delete files which are entirely in the given range
// Could leave some keys in the range which are in files which are not
// entirely in the range. Also leaves L0 files regardless of whether they're
// in the range.
// Snapshots before the delete might not see the data in the given range.
Status DeleteFilesInRange(DB* db, ColumnFamilyHandle* column_family,
                          const Slice* begin, const Slice* end,
                          bool include_end = true);

// Delete files in multiple ranges at once
// Delete files in a lot of ranges one at a time can be slow, use this API for
// better performance in that case.
Status DeleteFilesInRanges(DB* db, ColumnFamilyHandle* column_family,
                           const RangePtr* ranges, size_t n,
                           bool include_end = true);

// Verify the checksum of file
Status VerifySstFileChecksum(const Options& options,
                             const EnvOptions& env_options,
                             const std::string& file_path);
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
