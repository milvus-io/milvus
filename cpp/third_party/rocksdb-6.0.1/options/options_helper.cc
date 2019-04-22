//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "options/options_helper.h"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <unordered_set>
#include <vector>
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based_table_factory.h"
#include "table/plain_table_factory.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace rocksdb {

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options) {
  DBOptions options;

  options.create_if_missing = immutable_db_options.create_if_missing;
  options.create_missing_column_families =
      immutable_db_options.create_missing_column_families;
  options.error_if_exists = immutable_db_options.error_if_exists;
  options.paranoid_checks = immutable_db_options.paranoid_checks;
  options.env = immutable_db_options.env;
  options.rate_limiter = immutable_db_options.rate_limiter;
  options.sst_file_manager = immutable_db_options.sst_file_manager;
  options.info_log = immutable_db_options.info_log;
  options.info_log_level = immutable_db_options.info_log_level;
  options.max_open_files = mutable_db_options.max_open_files;
  options.max_file_opening_threads =
      immutable_db_options.max_file_opening_threads;
  options.max_total_wal_size = mutable_db_options.max_total_wal_size;
  options.statistics = immutable_db_options.statistics;
  options.use_fsync = immutable_db_options.use_fsync;
  options.db_paths = immutable_db_options.db_paths;
  options.db_log_dir = immutable_db_options.db_log_dir;
  options.wal_dir = immutable_db_options.wal_dir;
  options.delete_obsolete_files_period_micros =
      mutable_db_options.delete_obsolete_files_period_micros;
  options.max_background_jobs = mutable_db_options.max_background_jobs;
  options.base_background_compactions =
      mutable_db_options.base_background_compactions;
  options.max_background_compactions =
      mutable_db_options.max_background_compactions;
  options.bytes_per_sync = mutable_db_options.bytes_per_sync;
  options.wal_bytes_per_sync = mutable_db_options.wal_bytes_per_sync;
  options.max_subcompactions = immutable_db_options.max_subcompactions;
  options.max_background_flushes = immutable_db_options.max_background_flushes;
  options.max_log_file_size = immutable_db_options.max_log_file_size;
  options.log_file_time_to_roll = immutable_db_options.log_file_time_to_roll;
  options.keep_log_file_num = immutable_db_options.keep_log_file_num;
  options.recycle_log_file_num = immutable_db_options.recycle_log_file_num;
  options.max_manifest_file_size = immutable_db_options.max_manifest_file_size;
  options.table_cache_numshardbits =
      immutable_db_options.table_cache_numshardbits;
  options.WAL_ttl_seconds = immutable_db_options.wal_ttl_seconds;
  options.WAL_size_limit_MB = immutable_db_options.wal_size_limit_mb;
  options.manifest_preallocation_size =
      immutable_db_options.manifest_preallocation_size;
  options.allow_mmap_reads = immutable_db_options.allow_mmap_reads;
  options.allow_mmap_writes = immutable_db_options.allow_mmap_writes;
  options.use_direct_reads = immutable_db_options.use_direct_reads;
  options.use_direct_io_for_flush_and_compaction =
      immutable_db_options.use_direct_io_for_flush_and_compaction;
  options.allow_fallocate = immutable_db_options.allow_fallocate;
  options.is_fd_close_on_exec = immutable_db_options.is_fd_close_on_exec;
  options.stats_dump_period_sec = mutable_db_options.stats_dump_period_sec;
  options.stats_persist_period_sec =
      mutable_db_options.stats_persist_period_sec;
  options.stats_history_buffer_size =
      mutable_db_options.stats_history_buffer_size;
  options.advise_random_on_open = immutable_db_options.advise_random_on_open;
  options.db_write_buffer_size = immutable_db_options.db_write_buffer_size;
  options.write_buffer_manager = immutable_db_options.write_buffer_manager;
  options.access_hint_on_compaction_start =
      immutable_db_options.access_hint_on_compaction_start;
  options.new_table_reader_for_compaction_inputs =
      immutable_db_options.new_table_reader_for_compaction_inputs;
  options.compaction_readahead_size =
      mutable_db_options.compaction_readahead_size;
  options.random_access_max_buffer_size =
      immutable_db_options.random_access_max_buffer_size;
  options.writable_file_max_buffer_size =
      mutable_db_options.writable_file_max_buffer_size;
  options.use_adaptive_mutex = immutable_db_options.use_adaptive_mutex;
  options.listeners = immutable_db_options.listeners;
  options.enable_thread_tracking = immutable_db_options.enable_thread_tracking;
  options.delayed_write_rate = mutable_db_options.delayed_write_rate;
  options.enable_pipelined_write = immutable_db_options.enable_pipelined_write;
  options.allow_concurrent_memtable_write =
      immutable_db_options.allow_concurrent_memtable_write;
  options.enable_write_thread_adaptive_yield =
      immutable_db_options.enable_write_thread_adaptive_yield;
  options.write_thread_max_yield_usec =
      immutable_db_options.write_thread_max_yield_usec;
  options.write_thread_slow_yield_usec =
      immutable_db_options.write_thread_slow_yield_usec;
  options.skip_stats_update_on_db_open =
      immutable_db_options.skip_stats_update_on_db_open;
  options.wal_recovery_mode = immutable_db_options.wal_recovery_mode;
  options.allow_2pc = immutable_db_options.allow_2pc;
  options.row_cache = immutable_db_options.row_cache;
#ifndef ROCKSDB_LITE
  options.wal_filter = immutable_db_options.wal_filter;
#endif  // ROCKSDB_LITE
  options.fail_if_options_file_error =
      immutable_db_options.fail_if_options_file_error;
  options.dump_malloc_stats = immutable_db_options.dump_malloc_stats;
  options.avoid_flush_during_recovery =
      immutable_db_options.avoid_flush_during_recovery;
  options.avoid_flush_during_shutdown =
      mutable_db_options.avoid_flush_during_shutdown;
  options.allow_ingest_behind =
      immutable_db_options.allow_ingest_behind;
  options.preserve_deletes =
      immutable_db_options.preserve_deletes;
  options.two_write_queues = immutable_db_options.two_write_queues;
  options.manual_wal_flush = immutable_db_options.manual_wal_flush;
  options.atomic_flush = immutable_db_options.atomic_flush;

  return options;
}

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& options,
    const MutableCFOptions& mutable_cf_options) {
  ColumnFamilyOptions cf_opts(options);

  // Memtable related options
  cf_opts.write_buffer_size = mutable_cf_options.write_buffer_size;
  cf_opts.max_write_buffer_number = mutable_cf_options.max_write_buffer_number;
  cf_opts.arena_block_size = mutable_cf_options.arena_block_size;
  cf_opts.memtable_prefix_bloom_size_ratio =
      mutable_cf_options.memtable_prefix_bloom_size_ratio;
  cf_opts.memtable_whole_key_filtering =
      mutable_cf_options.memtable_whole_key_filtering;
  cf_opts.memtable_huge_page_size = mutable_cf_options.memtable_huge_page_size;
  cf_opts.max_successive_merges = mutable_cf_options.max_successive_merges;
  cf_opts.inplace_update_num_locks =
      mutable_cf_options.inplace_update_num_locks;
  cf_opts.prefix_extractor = mutable_cf_options.prefix_extractor;

  // Compaction related options
  cf_opts.disable_auto_compactions =
      mutable_cf_options.disable_auto_compactions;
  cf_opts.soft_pending_compaction_bytes_limit =
      mutable_cf_options.soft_pending_compaction_bytes_limit;
  cf_opts.hard_pending_compaction_bytes_limit =
      mutable_cf_options.hard_pending_compaction_bytes_limit;
  cf_opts.level0_file_num_compaction_trigger =
      mutable_cf_options.level0_file_num_compaction_trigger;
  cf_opts.level0_slowdown_writes_trigger =
      mutable_cf_options.level0_slowdown_writes_trigger;
  cf_opts.level0_stop_writes_trigger =
      mutable_cf_options.level0_stop_writes_trigger;
  cf_opts.max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  cf_opts.target_file_size_base = mutable_cf_options.target_file_size_base;
  cf_opts.target_file_size_multiplier =
      mutable_cf_options.target_file_size_multiplier;
  cf_opts.max_bytes_for_level_base =
      mutable_cf_options.max_bytes_for_level_base;
  cf_opts.max_bytes_for_level_multiplier =
      mutable_cf_options.max_bytes_for_level_multiplier;
  cf_opts.ttl = mutable_cf_options.ttl;

  cf_opts.max_bytes_for_level_multiplier_additional.clear();
  for (auto value :
       mutable_cf_options.max_bytes_for_level_multiplier_additional) {
    cf_opts.max_bytes_for_level_multiplier_additional.emplace_back(value);
  }

  cf_opts.compaction_options_fifo = mutable_cf_options.compaction_options_fifo;
  cf_opts.compaction_options_universal =
      mutable_cf_options.compaction_options_universal;

  // Misc options
  cf_opts.max_sequential_skip_in_iterations =
      mutable_cf_options.max_sequential_skip_in_iterations;
  cf_opts.paranoid_file_checks = mutable_cf_options.paranoid_file_checks;
  cf_opts.report_bg_io_stats = mutable_cf_options.report_bg_io_stats;
  cf_opts.compression = mutable_cf_options.compression;

  cf_opts.table_factory = options.table_factory;
  // TODO(yhchiang): find some way to handle the following derived options
  // * max_file_size

  return cf_opts;
}

std::map<CompactionStyle, std::string>
    OptionsHelper::compaction_style_to_string = {
        {kCompactionStyleLevel, "kCompactionStyleLevel"},
        {kCompactionStyleUniversal, "kCompactionStyleUniversal"},
        {kCompactionStyleFIFO, "kCompactionStyleFIFO"},
        {kCompactionStyleNone, "kCompactionStyleNone"}};

std::map<CompactionPri, std::string> OptionsHelper::compaction_pri_to_string = {
    {kByCompensatedSize, "kByCompensatedSize"},
    {kOldestLargestSeqFirst, "kOldestLargestSeqFirst"},
    {kOldestSmallestSeqFirst, "kOldestSmallestSeqFirst"},
    {kMinOverlappingRatio, "kMinOverlappingRatio"}};

std::map<CompactionStopStyle, std::string>
    OptionsHelper::compaction_stop_style_to_string = {
        {kCompactionStopStyleSimilarSize, "kCompactionStopStyleSimilarSize"},
        {kCompactionStopStyleTotalSize, "kCompactionStopStyleTotalSize"}};

std::unordered_map<std::string, ChecksumType>
    OptionsHelper::checksum_type_string_map = {{"kNoChecksum", kNoChecksum},
                                               {"kCRC32c", kCRC32c},
                                               {"kxxHash", kxxHash},
                                               {"kxxHash64", kxxHash64}};

std::unordered_map<std::string, CompressionType>
    OptionsHelper::compression_type_string_map = {
        {"kNoCompression", kNoCompression},
        {"kSnappyCompression", kSnappyCompression},
        {"kZlibCompression", kZlibCompression},
        {"kBZip2Compression", kBZip2Compression},
        {"kLZ4Compression", kLZ4Compression},
        {"kLZ4HCCompression", kLZ4HCCompression},
        {"kXpressCompression", kXpressCompression},
        {"kZSTD", kZSTD},
        {"kZSTDNotFinalCompression", kZSTDNotFinalCompression},
        {"kDisableCompressionOption", kDisableCompressionOption}};
#ifndef ROCKSDB_LITE

template <typename T>
Status GetStringFromStruct(
    std::string* opt_string, const T& options,
    const std::unordered_map<std::string, OptionTypeInfo> type_info,
    const std::string& delimiter);

namespace {
template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

bool SerializeVectorCompressionType(const std::vector<CompressionType>& types,
                                    std::string* value) {
  std::stringstream ss;
  bool result;
  for (size_t i = 0; i < types.size(); ++i) {
    if (i > 0) {
      ss << ':';
    }
    std::string string_type;
    result = SerializeEnum<CompressionType>(compression_type_string_map,
                                            types[i], &string_type);
    if (result == false) {
      return result;
    }
    ss << string_type;
  }
  *value = ss.str();
  return true;
}

bool ParseVectorCompressionType(
    const std::string& value,
    std::vector<CompressionType>* compression_per_level) {
  compression_per_level->clear();
  size_t start = 0;
  while (start < value.size()) {
    size_t end = value.find(':', start);
    bool is_ok;
    CompressionType type;
    if (end == std::string::npos) {
      is_ok = ParseEnum<CompressionType>(compression_type_string_map,
                                         value.substr(start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      break;
    } else {
      is_ok = ParseEnum<CompressionType>(
          compression_type_string_map, value.substr(start, end - start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      start = end + 1;
    }
  }
  return true;
}

// This is to handle backward compatibility, where compaction_options_fifo
// could be assigned a single scalar value, say, like "23", which would be
// assigned to max_table_files_size.
bool FIFOCompactionOptionsSpecialCase(const std::string& opt_str,
                                      CompactionOptionsFIFO* options) {
  if (opt_str.find("=") != std::string::npos) {
    // New format. Go do your new parsing using ParseStructOptions.
    return false;
  }

  // Old format. Parse just a single uint64_t value.
  options->max_table_files_size = ParseUint64(opt_str);
  return true;
}

template <typename T>
bool SerializeStruct(
    const T& options, std::string* value,
    std::unordered_map<std::string, OptionTypeInfo> type_info_map) {
  std::string opt_str;
  Status s = GetStringFromStruct(&opt_str, options, type_info_map, ";");
  if (!s.ok()) {
    return false;
  }
  *value = "{" + opt_str + "}";
  return true;
}

template <typename T>
bool ParseSingleStructOption(
    const std::string& opt_val_str, T* options,
    std::unordered_map<std::string, OptionTypeInfo> type_info_map) {
  size_t end = opt_val_str.find('=');
  std::string key = opt_val_str.substr(0, end);
  std::string value = opt_val_str.substr(end + 1);
  auto iter = type_info_map.find(key);
  if (iter == type_info_map.end()) {
    return false;
  }
  const auto& opt_info = iter->second;
  return ParseOptionHelper(
      reinterpret_cast<char*>(options) + opt_info.mutable_offset, opt_info.type,
      value);
}

template <typename T>
bool ParseStructOptions(
    const std::string& opt_str, T* options,
    std::unordered_map<std::string, OptionTypeInfo> type_info_map) {
  assert(!opt_str.empty());

  size_t start = 0;
  if (opt_str[0] == '{') {
    start++;
  }
  while ((start != std::string::npos) && (start < opt_str.size())) {
    if (opt_str[start] == '}') {
      break;
    }
    size_t end = opt_str.find(';', start);
    size_t len = (end == std::string::npos) ? end : end - start;
    if (!ParseSingleStructOption(opt_str.substr(start, len), options,
                                 type_info_map)) {
      return false;
    }
    start = (end == std::string::npos) ? end : end + 1;
  }
  return true;
}
}  // anonymouse namespace

bool ParseSliceTransformHelper(
    const std::string& kFixedPrefixName, const std::string& kCappedPrefixName,
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  const char* no_op_name = "rocksdb.Noop";
  size_t no_op_length = strlen(no_op_name);
  auto& pe_value = value;
  if (pe_value.size() > kFixedPrefixName.size() &&
      pe_value.compare(0, kFixedPrefixName.size(), kFixedPrefixName) == 0) {
    int prefix_length = ParseInt(trim(value.substr(kFixedPrefixName.size())));
    slice_transform->reset(NewFixedPrefixTransform(prefix_length));
  } else if (pe_value.size() > kCappedPrefixName.size() &&
             pe_value.compare(0, kCappedPrefixName.size(), kCappedPrefixName) ==
                 0) {
    int prefix_length =
        ParseInt(trim(pe_value.substr(kCappedPrefixName.size())));
    slice_transform->reset(NewCappedPrefixTransform(prefix_length));
  } else if (pe_value.size() == no_op_length &&
             pe_value.compare(0, no_op_length, no_op_name) == 0) {
    const SliceTransform* no_op_transform = NewNoopTransform();
    slice_transform->reset(no_op_transform);
  } else if (value == kNullptrString) {
    slice_transform->reset();
  } else {
    return false;
  }

  return true;
}

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  // While we normally don't convert the string representation of a
  // pointer-typed option into its instance, here we do so for backward
  // compatibility as we allow this action in SetOption().

  // TODO(yhchiang): A possible better place for these serialization /
  // deserialization is inside the class definition of pointer-typed
  // option itself, but this requires a bigger change of public API.
  bool result =
      ParseSliceTransformHelper("fixed:", "capped:", value, slice_transform);
  if (result) {
    return result;
  }
  result = ParseSliceTransformHelper(
      "rocksdb.FixedPrefix.", "rocksdb.CappedPrefix.", value, slice_transform);
  if (result) {
    return result;
  }
  // TODO(yhchiang): we can further support other default
  //                 SliceTransforms here.
  return false;
}

bool ParseOptionHelper(char* opt_address, const OptionType& opt_type,
                       const std::string& value) {
  switch (opt_type) {
    case OptionType::kBoolean:
      *reinterpret_cast<bool*>(opt_address) = ParseBoolean("", value);
      break;
    case OptionType::kInt:
      *reinterpret_cast<int*>(opt_address) = ParseInt(value);
      break;
    case OptionType::kVectorInt:
      *reinterpret_cast<std::vector<int>*>(opt_address) = ParseVectorInt(value);
      break;
    case OptionType::kUInt:
      *reinterpret_cast<unsigned int*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt32T:
      *reinterpret_cast<uint32_t*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt64T:
      PutUnaligned(reinterpret_cast<uint64_t*>(opt_address), ParseUint64(value));
      break;
    case OptionType::kSizeT:
      PutUnaligned(reinterpret_cast<size_t*>(opt_address), ParseSizeT(value));
      break;
    case OptionType::kString:
      *reinterpret_cast<std::string*>(opt_address) = value;
      break;
    case OptionType::kDouble:
      *reinterpret_cast<double*>(opt_address) = ParseDouble(value);
      break;
    case OptionType::kCompactionStyle:
      return ParseEnum<CompactionStyle>(
          compaction_style_string_map, value,
          reinterpret_cast<CompactionStyle*>(opt_address));
    case OptionType::kCompactionPri:
      return ParseEnum<CompactionPri>(
          compaction_pri_string_map, value,
          reinterpret_cast<CompactionPri*>(opt_address));
    case OptionType::kCompressionType:
      return ParseEnum<CompressionType>(
          compression_type_string_map, value,
          reinterpret_cast<CompressionType*>(opt_address));
    case OptionType::kVectorCompressionType:
      return ParseVectorCompressionType(
          value, reinterpret_cast<std::vector<CompressionType>*>(opt_address));
    case OptionType::kSliceTransform:
      return ParseSliceTransform(
          value, reinterpret_cast<std::shared_ptr<const SliceTransform>*>(
                     opt_address));
    case OptionType::kChecksumType:
      return ParseEnum<ChecksumType>(
          checksum_type_string_map, value,
          reinterpret_cast<ChecksumType*>(opt_address));
    case OptionType::kBlockBasedTableIndexType:
      return ParseEnum<BlockBasedTableOptions::IndexType>(
          block_base_table_index_type_string_map, value,
          reinterpret_cast<BlockBasedTableOptions::IndexType*>(opt_address));
    case OptionType::kBlockBasedTableDataBlockIndexType:
      return ParseEnum<BlockBasedTableOptions::DataBlockIndexType>(
          block_base_table_data_block_index_type_string_map, value,
          reinterpret_cast<BlockBasedTableOptions::DataBlockIndexType*>(
              opt_address));
    case OptionType::kEncodingType:
      return ParseEnum<EncodingType>(
          encoding_type_string_map, value,
          reinterpret_cast<EncodingType*>(opt_address));
    case OptionType::kWALRecoveryMode:
      return ParseEnum<WALRecoveryMode>(
          wal_recovery_mode_string_map, value,
          reinterpret_cast<WALRecoveryMode*>(opt_address));
    case OptionType::kAccessHint:
      return ParseEnum<DBOptions::AccessHint>(
          access_hint_string_map, value,
          reinterpret_cast<DBOptions::AccessHint*>(opt_address));
    case OptionType::kInfoLogLevel:
      return ParseEnum<InfoLogLevel>(
          info_log_level_string_map, value,
          reinterpret_cast<InfoLogLevel*>(opt_address));
    case OptionType::kCompactionOptionsFIFO: {
      if (!FIFOCompactionOptionsSpecialCase(
              value, reinterpret_cast<CompactionOptionsFIFO*>(opt_address))) {
        return ParseStructOptions<CompactionOptionsFIFO>(
            value, reinterpret_cast<CompactionOptionsFIFO*>(opt_address),
            fifo_compaction_options_type_info);
      }
      return true;
    }
    case OptionType::kLRUCacheOptions: {
      return ParseStructOptions<LRUCacheOptions>(value,
          reinterpret_cast<LRUCacheOptions*>(opt_address),
          lru_cache_options_type_info);
    }
    case OptionType::kCompactionOptionsUniversal:
      return ParseStructOptions<CompactionOptionsUniversal>(
          value, reinterpret_cast<CompactionOptionsUniversal*>(opt_address),
          universal_compaction_options_type_info);
    case OptionType::kCompactionStopStyle:
      return ParseEnum<CompactionStopStyle>(
          compaction_stop_style_string_map, value,
          reinterpret_cast<CompactionStopStyle*>(opt_address));
    default:
      return false;
  }
  return true;
}

bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionType opt_type,
                                 std::string* value) {

  assert(value);
  switch (opt_type) {
    case OptionType::kBoolean:
      *value = *(reinterpret_cast<const bool*>(opt_address)) ? "true" : "false";
      break;
    case OptionType::kInt:
      *value = ToString(*(reinterpret_cast<const int*>(opt_address)));
      break;
    case OptionType::kVectorInt:
      return SerializeIntVector(
          *reinterpret_cast<const std::vector<int>*>(opt_address), value);
    case OptionType::kUInt:
      *value = ToString(*(reinterpret_cast<const unsigned int*>(opt_address)));
      break;
    case OptionType::kUInt32T:
      *value = ToString(*(reinterpret_cast<const uint32_t*>(opt_address)));
      break;
    case OptionType::kUInt64T:
      {
        uint64_t v;
        GetUnaligned(reinterpret_cast<const uint64_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kSizeT:
      {
        size_t v;
        GetUnaligned(reinterpret_cast<const size_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kDouble:
      *value = ToString(*(reinterpret_cast<const double*>(opt_address)));
      break;
    case OptionType::kString:
      *value = EscapeOptionString(
          *(reinterpret_cast<const std::string*>(opt_address)));
      break;
    case OptionType::kCompactionStyle:
      return SerializeEnum<CompactionStyle>(
          compaction_style_string_map,
          *(reinterpret_cast<const CompactionStyle*>(opt_address)), value);
    case OptionType::kCompactionPri:
      return SerializeEnum<CompactionPri>(
          compaction_pri_string_map,
          *(reinterpret_cast<const CompactionPri*>(opt_address)), value);
    case OptionType::kCompressionType:
      return SerializeEnum<CompressionType>(
          compression_type_string_map,
          *(reinterpret_cast<const CompressionType*>(opt_address)), value);
    case OptionType::kVectorCompressionType:
      return SerializeVectorCompressionType(
          *(reinterpret_cast<const std::vector<CompressionType>*>(opt_address)),
          value);
      break;
    case OptionType::kSliceTransform: {
      const auto* slice_transform_ptr =
          reinterpret_cast<const std::shared_ptr<const SliceTransform>*>(
              opt_address);
      *value = slice_transform_ptr->get() ? slice_transform_ptr->get()->Name()
                                          : kNullptrString;
      break;
    }
    case OptionType::kTableFactory: {
      const auto* table_factory_ptr =
          reinterpret_cast<const std::shared_ptr<const TableFactory>*>(
              opt_address);
      *value = table_factory_ptr->get() ? table_factory_ptr->get()->Name()
                                        : kNullptrString;
      break;
    }
    case OptionType::kComparator: {
      // it's a const pointer of const Comparator*
      const auto* ptr = reinterpret_cast<const Comparator* const*>(opt_address);
      // Since the user-specified comparator will be wrapped by
      // InternalKeyComparator, we should persist the user-specified one
      // instead of InternalKeyComparator.
      if (*ptr == nullptr) {
        *value = kNullptrString;
      } else {
        const Comparator* root_comp = (*ptr)->GetRootComparator();
        if (root_comp == nullptr) {
          root_comp = (*ptr);
        }
        *value = root_comp->Name();
      }
      break;
    }
    case OptionType::kCompactionFilter: {
      // it's a const pointer of const CompactionFilter*
      const auto* ptr =
          reinterpret_cast<const CompactionFilter* const*>(opt_address);
      *value = *ptr ? (*ptr)->Name() : kNullptrString;
      break;
    }
    case OptionType::kCompactionFilterFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<CompactionFilterFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kMemTableRepFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<MemTableRepFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kMergeOperator: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<MergeOperator>*>(opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kFilterPolicy: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<FilterPolicy>*>(opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kChecksumType:
      return SerializeEnum<ChecksumType>(
          checksum_type_string_map,
          *reinterpret_cast<const ChecksumType*>(opt_address), value);
    case OptionType::kBlockBasedTableIndexType:
      return SerializeEnum<BlockBasedTableOptions::IndexType>(
          block_base_table_index_type_string_map,
          *reinterpret_cast<const BlockBasedTableOptions::IndexType*>(
              opt_address),
          value);
    case OptionType::kBlockBasedTableDataBlockIndexType:
      return SerializeEnum<BlockBasedTableOptions::DataBlockIndexType>(
          block_base_table_data_block_index_type_string_map,
          *reinterpret_cast<const BlockBasedTableOptions::DataBlockIndexType*>(
              opt_address),
          value);
    case OptionType::kFlushBlockPolicyFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<FlushBlockPolicyFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kEncodingType:
      return SerializeEnum<EncodingType>(
          encoding_type_string_map,
          *reinterpret_cast<const EncodingType*>(opt_address), value);
    case OptionType::kWALRecoveryMode:
      return SerializeEnum<WALRecoveryMode>(
          wal_recovery_mode_string_map,
          *reinterpret_cast<const WALRecoveryMode*>(opt_address), value);
    case OptionType::kAccessHint:
      return SerializeEnum<DBOptions::AccessHint>(
          access_hint_string_map,
          *reinterpret_cast<const DBOptions::AccessHint*>(opt_address), value);
    case OptionType::kInfoLogLevel:
      return SerializeEnum<InfoLogLevel>(
          info_log_level_string_map,
          *reinterpret_cast<const InfoLogLevel*>(opt_address), value);
    case OptionType::kCompactionOptionsFIFO:
      return SerializeStruct<CompactionOptionsFIFO>(
          *reinterpret_cast<const CompactionOptionsFIFO*>(opt_address), value,
          fifo_compaction_options_type_info);
    case OptionType::kCompactionOptionsUniversal:
      return SerializeStruct<CompactionOptionsUniversal>(
          *reinterpret_cast<const CompactionOptionsUniversal*>(opt_address),
          value, universal_compaction_options_type_info);
    case OptionType::kCompactionStopStyle:
      return SerializeEnum<CompactionStopStyle>(
          compaction_stop_style_string_map,
          *reinterpret_cast<const CompactionStopStyle*>(opt_address), value);
    default:
      return false;
  }
  return true;
}

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : options_map) {
    try {
      auto iter = cf_options_type_info.find(o.first);
      if (iter == cf_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option: " + o.first);
      }
      const auto& opt_info = iter->second;
      if (!opt_info.is_mutable) {
        return Status::InvalidArgument("Option not changeable: " + o.first);
      }
      if (opt_info.verification == OptionVerificationType::kDeprecated) {
        // log warning when user tries to set a deprecated option but don't fail
        // the call for compatibility.
        ROCKS_LOG_WARN(info_log, "%s is a deprecated option and cannot be set",
                       o.first.c_str());
        continue;
      }
      bool is_ok = ParseOptionHelper(
          reinterpret_cast<char*>(new_options) + opt_info.mutable_offset,
          opt_info.type, o.second);
      if (!is_ok) {
        return Status::InvalidArgument("Error parsing " + o.first);
      }
    } catch (std::exception& e) {
      return Status::InvalidArgument("Error parsing " + o.first + ":" +
                                     std::string(e.what()));
    }
  }
  return Status::OK();
}

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : options_map) {
    try {
      auto iter = db_options_type_info.find(o.first);
      if (iter == db_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option: " + o.first);
      }
      const auto& opt_info = iter->second;
      if (!opt_info.is_mutable) {
        return Status::InvalidArgument("Option not changeable: " + o.first);
      }
      bool is_ok = ParseOptionHelper(
          reinterpret_cast<char*>(new_options) + opt_info.mutable_offset,
          opt_info.type, o.second);
      if (!is_ok) {
        return Status::InvalidArgument("Error parsing " + o.first);
      }
    } catch (std::exception& e) {
      return Status::InvalidArgument("Error parsing " + o.first + ":" +
                                     std::string(e.what()));
    }
  }
  return Status::OK();
}

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map) {
  assert(opts_map);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
  //              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
  size_t pos = 0;
  std::string opts = trim(opts_str);
  while (pos < opts.size()) {
    size_t eq_pos = opts.find('=', pos);
    if (eq_pos == std::string::npos) {
      return Status::InvalidArgument("Mismatched key value pair, '=' expected");
    }
    std::string key = trim(opts.substr(pos, eq_pos - pos));
    if (key.empty()) {
      return Status::InvalidArgument("Empty key found");
    }

    // skip space after '=' and look for '{' for possible nested options
    pos = eq_pos + 1;
    while (pos < opts.size() && isspace(opts[pos])) {
      ++pos;
    }
    // Empty value at the end
    if (pos >= opts.size()) {
      (*opts_map)[key] = "";
      break;
    }
    if (opts[pos] == '{') {
      int count = 1;
      size_t brace_pos = pos + 1;
      while (brace_pos < opts.size()) {
        if (opts[brace_pos] == '{') {
          ++count;
        } else if (opts[brace_pos] == '}') {
          --count;
          if (count == 0) {
            break;
          }
        }
        ++brace_pos;
      }
      // found the matching closing brace
      if (count == 0) {
        (*opts_map)[key] = trim(opts.substr(pos + 1, brace_pos - pos - 1));
        // skip all whitespace and move to the next ';'
        // brace_pos points to the next position after the matching '}'
        pos = brace_pos + 1;
        while (pos < opts.size() && isspace(opts[pos])) {
          ++pos;
        }
        if (pos < opts.size() && opts[pos] != ';') {
          return Status::InvalidArgument(
              "Unexpected chars after nested options");
        }
        ++pos;
      } else {
        return Status::InvalidArgument(
            "Mismatched curly braces for nested options");
      }
    } else {
      size_t sc_pos = opts.find(';', pos);
      if (sc_pos == std::string::npos) {
        (*opts_map)[key] = trim(opts.substr(pos));
        // It either ends with a trailing semi-colon or the last key-value pair
        break;
      } else {
        (*opts_map)[key] = trim(opts.substr(pos, sc_pos - pos));
      }
      pos = sc_pos + 1;
    }
  }

  return Status::OK();
}

Status ParseCompressionOptions(const std::string& value, const std::string& name,
                              CompressionOptions& compression_opts) {
  size_t start = 0;
  size_t end = value.find(':');
  if (end == std::string::npos) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.window_bits = ParseInt(value.substr(start, end - start));
  start = end + 1;
  end = value.find(':', start);
  if (end == std::string::npos) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.level = ParseInt(value.substr(start, end - start));
  start = end + 1;
  if (start >= value.size()) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  end = value.find(':', start);
  compression_opts.strategy =
      ParseInt(value.substr(start, value.size() - start));
  // max_dict_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_bytes =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // zstd_max_train_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.zstd_max_train_bytes =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // enabled is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.enabled =
        ParseBoolean("", value.substr(start, value.size() - start));
  }
  return Status::OK();
}

Status ParseColumnFamilyOption(const std::string& name,
                               const std::string& org_value,
                               ColumnFamilyOptions* new_options,
                               bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  try {
    if (name == "block_based_table_factory") {
      // Nested options
      BlockBasedTableOptions table_opt, base_table_options;
      BlockBasedTableFactory* block_based_table_factory =
          static_cast_with_check<BlockBasedTableFactory, TableFactory>(
              new_options->table_factory.get());
      if (block_based_table_factory != nullptr) {
        base_table_options = block_based_table_factory->table_options();
      }
      Status table_opt_s = GetBlockBasedTableOptionsFromString(
          base_table_options, value, &table_opt);
      if (!table_opt_s.ok()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->table_factory.reset(NewBlockBasedTableFactory(table_opt));
    } else if (name == "plain_table_factory") {
      // Nested options
      PlainTableOptions table_opt, base_table_options;
      PlainTableFactory* plain_table_factory =
          static_cast_with_check<PlainTableFactory, TableFactory>(
              new_options->table_factory.get());
      if (plain_table_factory != nullptr) {
        base_table_options = plain_table_factory->table_options();
      }
      Status table_opt_s = GetPlainTableOptionsFromString(
          base_table_options, value, &table_opt);
      if (!table_opt_s.ok()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->table_factory.reset(NewPlainTableFactory(table_opt));
    } else if (name == "memtable") {
      std::unique_ptr<MemTableRepFactory> new_mem_factory;
      Status mem_factory_s =
          GetMemTableRepFactoryFromString(value, &new_mem_factory);
      if (!mem_factory_s.ok()) {
        return Status::InvalidArgument(
            "unable to parse the specified CF option " + name);
      }
      new_options->memtable_factory.reset(new_mem_factory.release());
    } else if (name == "bottommost_compression_opts") {
      Status s = ParseCompressionOptions(
          value, name, new_options->bottommost_compression_opts);
      if (!s.ok()) {
        return s;
      }
    } else if (name == "compression_opts") {
      Status s =
          ParseCompressionOptions(value, name, new_options->compression_opts);
      if (!s.ok()) {
        return s;
      }
    } else {
      auto iter = cf_options_type_info.find(name);
      if (iter == cf_options_type_info.end()) {
        return Status::InvalidArgument(
            "Unable to parse the specified CF option " + name);
      }
      const auto& opt_info = iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          ParseOptionHelper(
              reinterpret_cast<char*>(new_options) + opt_info.offset,
              opt_info.type, value)) {
        return Status::OK();
      }
      switch (opt_info.verification) {
        case OptionVerificationType::kByName:
        case OptionVerificationType::kByNameAllowNull:
        case OptionVerificationType::kByNameAllowFromNull:
          return Status::NotSupported(
              "Deserializing the specified CF option " + name +
                  " is not supported");
        case OptionVerificationType::kDeprecated:
          return Status::OK();
        default:
          return Status::InvalidArgument(
              "Unable to parse the specified CF option " + name);
      }
    }
  } catch (const std::exception&) {
    return Status::InvalidArgument(
        "unable to parse the specified option " + name);
  }
  return Status::OK();
}

template <typename T>
bool SerializeSingleStructOption(
    std::string* opt_string, const T& options,
    const std::unordered_map<std::string, OptionTypeInfo> type_info,
    const std::string& name, const std::string& delimiter) {
  auto iter = type_info.find(name);
  if (iter == type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

template <typename T>
Status GetStringFromStruct(
    std::string* opt_string, const T& options,
    const std::unordered_map<std::string, OptionTypeInfo> type_info,
    const std::string& delimiter) {
  assert(opt_string);
  opt_string->clear();
  for (auto iter = type_info.begin(); iter != type_info.end(); ++iter) {
    if (iter->second.verification == OptionVerificationType::kDeprecated) {
      // If the option is no longer used in rocksdb and marked as deprecated,
      // we skip it in the serialization.
      continue;
    }
    std::string single_output;
    bool result = SerializeSingleStructOption<T>(
        &single_output, options, type_info, iter->first, delimiter);
    if (result) {
      opt_string->append(single_output);
    } else {
      return Status::InvalidArgument("failed to serialize %s\n",
                                     iter->first.c_str());
    }
    assert(result);
  }
  return Status::OK();
}

Status GetStringFromDBOptions(std::string* opt_string,
                              const DBOptions& db_options,
                              const std::string& delimiter) {
  return GetStringFromStruct<DBOptions>(opt_string, db_options,
                                        db_options_type_info, delimiter);
}

Status GetStringFromColumnFamilyOptions(std::string* opt_string,
                                        const ColumnFamilyOptions& cf_options,
                                        const std::string& delimiter) {
  return GetStringFromStruct<ColumnFamilyOptions>(
      opt_string, cf_options, cf_options_type_info, delimiter);
}

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type) {
  bool ok = SerializeEnum<CompressionType>(compression_type_string_map,
                                           compression_type, compression_str);
  if (ok) {
    return Status::OK();
  } else {
    return Status::InvalidArgument("Invalid compression types");
  }
}

std::vector<CompressionType> GetSupportedCompressions() {
  std::vector<CompressionType> supported_compressions;
  for (const auto& comp_to_name : compression_type_string_map) {
    CompressionType t = comp_to_name.second;
    if (t != kDisableCompressionOption && CompressionTypeSupported(t)) {
      supported_compressions.push_back(t);
    }
  }
  return supported_compressions;
}

Status ParseDBOption(const std::string& name,
                     const std::string& org_value,
                     DBOptions* new_options,
                     bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  try {
    if (name == "rate_limiter_bytes_per_sec") {
      new_options->rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(ParseUint64(value))));
    } else {
      auto iter = db_options_type_info.find(name);
      if (iter == db_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option DBOptions:", name);
      }
      const auto& opt_info = iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          ParseOptionHelper(
              reinterpret_cast<char*>(new_options) + opt_info.offset,
              opt_info.type, value)) {
        return Status::OK();
      }
      switch (opt_info.verification) {
        case OptionVerificationType::kByName:
        case OptionVerificationType::kByNameAllowNull:
          return Status::NotSupported(
              "Deserializing the specified DB option " + name +
                  " is not supported");
        case OptionVerificationType::kDeprecated:
          return Status::OK();
        default:
          return Status::InvalidArgument(
              "Unable to parse the specified DB option " + name);
      }
    }
  } catch (const std::exception&) {
    return Status::InvalidArgument("Unable to parse DBOptions:", name);
  }
  return Status::OK();
}

Status GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    bool ignore_unknown_options) {
  return GetColumnFamilyOptionsFromMapInternal(
      base_options, opts_map, new_options, input_strings_escaped, nullptr,
      ignore_unknown_options);
}

Status GetColumnFamilyOptionsFromMapInternal(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names,
    bool ignore_unknown_options) {
  assert(new_options);
  *new_options = base_options;
  if (unsupported_options_names) {
    unsupported_options_names->clear();
  }
  for (const auto& o : opts_map) {
    auto s = ParseColumnFamilyOption(o.first, o.second, new_options,
                                 input_strings_escaped);
    if (!s.ok()) {
      if (s.IsNotSupported()) {
        // If the deserialization of the specified option is not supported
        // and an output vector of unsupported_options is provided, then
        // we log the name of the unsupported option and proceed.
        if (unsupported_options_names != nullptr) {
          unsupported_options_names->push_back(o.first);
        }
        // Note that we still return Status::OK in such case to maintain
        // the backward compatibility in the old public API defined in
        // rocksdb/convenience.h
      } else if (s.IsInvalidArgument() && ignore_unknown_options) {
        continue;
      } else {
        // Restore "new_options" to the default "base_options".
        *new_options = base_options;
        return s;
      }
    }
  }
  return Status::OK();
}

Status GetColumnFamilyOptionsFromString(
    const ColumnFamilyOptions& base_options,
    const std::string& opts_str,
    ColumnFamilyOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetColumnFamilyOptionsFromMap(base_options, opts_map, new_options);
}

Status GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped,
    bool ignore_unknown_options) {
  return GetDBOptionsFromMapInternal(base_options, opts_map, new_options,
                                     input_strings_escaped, nullptr,
                                     ignore_unknown_options);
}

Status GetDBOptionsFromMapInternal(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names,
    bool ignore_unknown_options) {
  assert(new_options);
  *new_options = base_options;
  if (unsupported_options_names) {
    unsupported_options_names->clear();
  }
  for (const auto& o : opts_map) {
    auto s = ParseDBOption(o.first, o.second,
                           new_options, input_strings_escaped);
    if (!s.ok()) {
      if (s.IsNotSupported()) {
        // If the deserialization of the specified option is not supported
        // and an output vector of unsupported_options is provided, then
        // we log the name of the unsupported option and proceed.
        if (unsupported_options_names != nullptr) {
          unsupported_options_names->push_back(o.first);
        }
        // Note that we still return Status::OK in such case to maintain
        // the backward compatibility in the old public API defined in
        // rocksdb/convenience.h
      } else if (s.IsInvalidArgument() && ignore_unknown_options) {
        continue;
      } else {
        // Restore "new_options" to the default "base_options".
        *new_options = base_options;
        return s;
      }
    }
  }
  return Status::OK();
}

Status GetDBOptionsFromString(
    const DBOptions& base_options,
    const std::string& opts_str,
    DBOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetDBOptionsFromMap(base_options, opts_map, new_options);
}

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  DBOptions new_db_options(base_options);
  ColumnFamilyOptions new_cf_options(base_options);
  for (const auto& o : opts_map) {
    if (ParseDBOption(o.first, o.second, &new_db_options).ok()) {
    } else if (ParseColumnFamilyOption(
        o.first, o.second, &new_cf_options).ok()) {
    } else {
      return Status::InvalidArgument("Can't parse option " + o.first);
    }
  }
  *new_options = Options(new_db_options, new_cf_options);
  return Status::OK();
}

Status GetTableFactoryFromMap(
    const std::string& factory_name,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<TableFactory>* table_factory, bool ignore_unknown_options) {
  Status s;
  if (factory_name == BlockBasedTableFactory().Name()) {
    BlockBasedTableOptions bbt_opt;
    s = GetBlockBasedTableOptionsFromMap(BlockBasedTableOptions(), opt_map,
                                         &bbt_opt,
                                         true, /* input_strings_escaped */
                                         ignore_unknown_options);
    if (!s.ok()) {
      return s;
    }
    table_factory->reset(new BlockBasedTableFactory(bbt_opt));
    return Status::OK();
  } else if (factory_name == PlainTableFactory().Name()) {
    PlainTableOptions pt_opt;
    s = GetPlainTableOptionsFromMap(PlainTableOptions(), opt_map, &pt_opt,
                                    true, /* input_strings_escaped */
                                    ignore_unknown_options);
    if (!s.ok()) {
      return s;
    }
    table_factory->reset(new PlainTableFactory(pt_opt));
    return Status::OK();
  }
  // Return OK for not supported table factories as TableFactory
  // Deserialization is optional.
  table_factory->reset();
  return Status::OK();
}

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::db_options_type_info = {
        /*
         // not yet supported
          Env* env;
          std::shared_ptr<Cache> row_cache;
          std::shared_ptr<DeleteScheduler> delete_scheduler;
          std::shared_ptr<Logger> info_log;
          std::shared_ptr<RateLimiter> rate_limiter;
          std::shared_ptr<Statistics> statistics;
          std::vector<DbPath> db_paths;
          std::vector<std::shared_ptr<EventListener>> listeners;
         */
        {"advise_random_on_open",
         {offsetof(struct DBOptions, advise_random_on_open),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"allow_mmap_reads",
         {offsetof(struct DBOptions, allow_mmap_reads), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"allow_fallocate",
         {offsetof(struct DBOptions, allow_fallocate), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"allow_mmap_writes",
         {offsetof(struct DBOptions, allow_mmap_writes), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"use_direct_reads",
         {offsetof(struct DBOptions, use_direct_reads), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"use_direct_writes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"use_direct_io_for_flush_and_compaction",
         {offsetof(struct DBOptions, use_direct_io_for_flush_and_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"allow_2pc",
         {offsetof(struct DBOptions, allow_2pc), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"allow_os_buffer",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, true,
          0}},
        {"create_if_missing",
         {offsetof(struct DBOptions, create_if_missing), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"create_missing_column_families",
         {offsetof(struct DBOptions, create_missing_column_families),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"disableDataSync",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"disable_data_sync",  // for compatibility
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"enable_thread_tracking",
         {offsetof(struct DBOptions, enable_thread_tracking),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"error_if_exists",
         {offsetof(struct DBOptions, error_if_exists), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"is_fd_close_on_exec",
         {offsetof(struct DBOptions, is_fd_close_on_exec), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"paranoid_checks",
         {offsetof(struct DBOptions, paranoid_checks), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"skip_log_error_on_recovery",
         {offsetof(struct DBOptions, skip_log_error_on_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"skip_stats_update_on_db_open",
         {offsetof(struct DBOptions, skip_stats_update_on_db_open),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"new_table_reader_for_compaction_inputs",
         {offsetof(struct DBOptions, new_table_reader_for_compaction_inputs),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"compaction_readahead_size",
         {offsetof(struct DBOptions, compaction_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, compaction_readahead_size)}},
        {"random_access_max_buffer_size",
         {offsetof(struct DBOptions, random_access_max_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
        {"use_adaptive_mutex",
         {offsetof(struct DBOptions, use_adaptive_mutex), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"use_fsync",
         {offsetof(struct DBOptions, use_fsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"max_background_jobs",
         {offsetof(struct DBOptions, max_background_jobs), OptionType::kInt,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, max_background_jobs)}},
        {"max_background_compactions",
         {offsetof(struct DBOptions, max_background_compactions),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, max_background_compactions)}},
        {"base_background_compactions",
         {offsetof(struct DBOptions, base_background_compactions),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, base_background_compactions)}},
        {"max_background_flushes",
         {offsetof(struct DBOptions, max_background_flushes), OptionType::kInt,
          OptionVerificationType::kNormal, false, 0}},
        {"max_file_opening_threads",
         {offsetof(struct DBOptions, max_file_opening_threads),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"max_open_files",
         {offsetof(struct DBOptions, max_open_files), OptionType::kInt,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, max_open_files)}},
        {"table_cache_numshardbits",
         {offsetof(struct DBOptions, table_cache_numshardbits),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"db_write_buffer_size",
         {offsetof(struct DBOptions, db_write_buffer_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, false, 0}},
        {"keep_log_file_num",
         {offsetof(struct DBOptions, keep_log_file_num), OptionType::kSizeT,
          OptionVerificationType::kNormal, false, 0}},
        {"recycle_log_file_num",
         {offsetof(struct DBOptions, recycle_log_file_num), OptionType::kSizeT,
          OptionVerificationType::kNormal, false, 0}},
        {"log_file_time_to_roll",
         {offsetof(struct DBOptions, log_file_time_to_roll), OptionType::kSizeT,
          OptionVerificationType::kNormal, false, 0}},
        {"manifest_preallocation_size",
         {offsetof(struct DBOptions, manifest_preallocation_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
        {"max_log_file_size",
         {offsetof(struct DBOptions, max_log_file_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, false, 0}},
        {"db_log_dir",
         {offsetof(struct DBOptions, db_log_dir), OptionType::kString,
          OptionVerificationType::kNormal, false, 0}},
        {"wal_dir",
         {offsetof(struct DBOptions, wal_dir), OptionType::kString,
          OptionVerificationType::kNormal, false, 0}},
        {"max_subcompactions",
         {offsetof(struct DBOptions, max_subcompactions), OptionType::kUInt32T,
          OptionVerificationType::kNormal, false, 0}},
        {"WAL_size_limit_MB",
         {offsetof(struct DBOptions, WAL_size_limit_MB), OptionType::kUInt64T,
          OptionVerificationType::kNormal, false, 0}},
        {"WAL_ttl_seconds",
         {offsetof(struct DBOptions, WAL_ttl_seconds), OptionType::kUInt64T,
          OptionVerificationType::kNormal, false, 0}},
        {"bytes_per_sync",
         {offsetof(struct DBOptions, bytes_per_sync), OptionType::kUInt64T,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, bytes_per_sync)}},
        {"delayed_write_rate",
         {offsetof(struct DBOptions, delayed_write_rate), OptionType::kUInt64T,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, delayed_write_rate)}},
        {"delete_obsolete_files_period_micros",
         {offsetof(struct DBOptions, delete_obsolete_files_period_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions,
                   delete_obsolete_files_period_micros)}},
        {"max_manifest_file_size",
         {offsetof(struct DBOptions, max_manifest_file_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"max_total_wal_size",
         {offsetof(struct DBOptions, max_total_wal_size), OptionType::kUInt64T,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, max_total_wal_size)}},
        {"wal_bytes_per_sync",
         {offsetof(struct DBOptions, wal_bytes_per_sync), OptionType::kUInt64T,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, wal_bytes_per_sync)}},
        {"stats_dump_period_sec",
         {offsetof(struct DBOptions, stats_dump_period_sec), OptionType::kUInt,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, stats_dump_period_sec)}},
        {"stats_persist_period_sec",
         {offsetof(struct DBOptions, stats_persist_period_sec),
          OptionType::kUInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, stats_persist_period_sec)}},
        {"stats_history_buffer_size",
         {offsetof(struct DBOptions, stats_history_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, stats_history_buffer_size)}},
        {"fail_if_options_file_error",
         {offsetof(struct DBOptions, fail_if_options_file_error),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"enable_pipelined_write",
         {offsetof(struct DBOptions, enable_pipelined_write),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"allow_concurrent_memtable_write",
         {offsetof(struct DBOptions, allow_concurrent_memtable_write),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"wal_recovery_mode",
         {offsetof(struct DBOptions, wal_recovery_mode),
          OptionType::kWALRecoveryMode, OptionVerificationType::kNormal, false,
          0}},
        {"enable_write_thread_adaptive_yield",
         {offsetof(struct DBOptions, enable_write_thread_adaptive_yield),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"write_thread_slow_yield_usec",
         {offsetof(struct DBOptions, write_thread_slow_yield_usec),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"write_thread_max_yield_usec",
         {offsetof(struct DBOptions, write_thread_max_yield_usec),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"access_hint_on_compaction_start",
         {offsetof(struct DBOptions, access_hint_on_compaction_start),
          OptionType::kAccessHint, OptionVerificationType::kNormal, false, 0}},
        {"info_log_level",
         {offsetof(struct DBOptions, info_log_level), OptionType::kInfoLogLevel,
          OptionVerificationType::kNormal, false, 0}},
        {"dump_malloc_stats",
         {offsetof(struct DBOptions, dump_malloc_stats), OptionType::kBoolean,
          OptionVerificationType::kNormal, false, 0}},
        {"avoid_flush_during_recovery",
         {offsetof(struct DBOptions, avoid_flush_during_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"avoid_flush_during_shutdown",
         {offsetof(struct DBOptions, avoid_flush_during_shutdown),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, avoid_flush_during_shutdown)}},
        {"writable_file_max_buffer_size",
         {offsetof(struct DBOptions, writable_file_max_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableDBOptions, writable_file_max_buffer_size)}},
        {"allow_ingest_behind",
         {offsetof(struct DBOptions, allow_ingest_behind), OptionType::kBoolean,
          OptionVerificationType::kNormal, false,
          offsetof(struct ImmutableDBOptions, allow_ingest_behind)}},
        {"preserve_deletes",
         {offsetof(struct DBOptions, preserve_deletes), OptionType::kBoolean,
          OptionVerificationType::kNormal, false,
          offsetof(struct ImmutableDBOptions, preserve_deletes)}},
        {"concurrent_prepare",  // Deprecated by two_write_queues
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"two_write_queues",
         {offsetof(struct DBOptions, two_write_queues), OptionType::kBoolean,
          OptionVerificationType::kNormal, false,
          offsetof(struct ImmutableDBOptions, two_write_queues)}},
        {"manual_wal_flush",
         {offsetof(struct DBOptions, manual_wal_flush), OptionType::kBoolean,
          OptionVerificationType::kNormal, false,
          offsetof(struct ImmutableDBOptions, manual_wal_flush)}},
        {"seq_per_batch",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"atomic_flush",
         {offsetof(struct DBOptions, atomic_flush), OptionType::kBoolean,
          OptionVerificationType::kNormal, false,
          offsetof(struct ImmutableDBOptions, atomic_flush)}}};

std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
    OptionsHelper::block_base_table_index_type_string_map = {
        {"kBinarySearch", BlockBasedTableOptions::IndexType::kBinarySearch},
        {"kHashSearch", BlockBasedTableOptions::IndexType::kHashSearch},
        {"kTwoLevelIndexSearch",
         BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch}};

std::unordered_map<std::string, BlockBasedTableOptions::DataBlockIndexType>
    OptionsHelper::block_base_table_data_block_index_type_string_map = {
        {"kDataBlockBinarySearch",
         BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch},
        {"kDataBlockBinaryAndHash",
         BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash}};

std::unordered_map<std::string, EncodingType>
    OptionsHelper::encoding_type_string_map = {{"kPlain", kPlain},
                                               {"kPrefix", kPrefix}};

std::unordered_map<std::string, CompactionStyle>
    OptionsHelper::compaction_style_string_map = {
        {"kCompactionStyleLevel", kCompactionStyleLevel},
        {"kCompactionStyleUniversal", kCompactionStyleUniversal},
        {"kCompactionStyleFIFO", kCompactionStyleFIFO},
        {"kCompactionStyleNone", kCompactionStyleNone}};

std::unordered_map<std::string, CompactionPri>
    OptionsHelper::compaction_pri_string_map = {
        {"kByCompensatedSize", kByCompensatedSize},
        {"kOldestLargestSeqFirst", kOldestLargestSeqFirst},
        {"kOldestSmallestSeqFirst", kOldestSmallestSeqFirst},
        {"kMinOverlappingRatio", kMinOverlappingRatio}};

std::unordered_map<std::string, WALRecoveryMode>
    OptionsHelper::wal_recovery_mode_string_map = {
        {"kTolerateCorruptedTailRecords",
         WALRecoveryMode::kTolerateCorruptedTailRecords},
        {"kAbsoluteConsistency", WALRecoveryMode::kAbsoluteConsistency},
        {"kPointInTimeRecovery", WALRecoveryMode::kPointInTimeRecovery},
        {"kSkipAnyCorruptedRecords",
         WALRecoveryMode::kSkipAnyCorruptedRecords}};

std::unordered_map<std::string, DBOptions::AccessHint>
    OptionsHelper::access_hint_string_map = {
        {"NONE", DBOptions::AccessHint::NONE},
        {"NORMAL", DBOptions::AccessHint::NORMAL},
        {"SEQUENTIAL", DBOptions::AccessHint::SEQUENTIAL},
        {"WILLNEED", DBOptions::AccessHint::WILLNEED}};

std::unordered_map<std::string, InfoLogLevel>
    OptionsHelper::info_log_level_string_map = {
        {"DEBUG_LEVEL", InfoLogLevel::DEBUG_LEVEL},
        {"INFO_LEVEL", InfoLogLevel::INFO_LEVEL},
        {"WARN_LEVEL", InfoLogLevel::WARN_LEVEL},
        {"ERROR_LEVEL", InfoLogLevel::ERROR_LEVEL},
        {"FATAL_LEVEL", InfoLogLevel::FATAL_LEVEL},
        {"HEADER_LEVEL", InfoLogLevel::HEADER_LEVEL}};

ColumnFamilyOptions OptionsHelper::dummy_cf_options;
CompactionOptionsFIFO OptionsHelper::dummy_comp_options;
LRUCacheOptions OptionsHelper::dummy_lru_cache_options;
CompactionOptionsUniversal OptionsHelper::dummy_comp_options_universal;

// offset_of is used to get the offset of a class data member
// ex: offset_of(&ColumnFamilyOptions::num_levels)
// This call will return the offset of num_levels in ColumnFamilyOptions class
//
// This is the same as offsetof() but allow us to work with non standard-layout
// classes and structures
// refs:
// http://en.cppreference.com/w/cpp/concept/StandardLayoutType
// https://gist.github.com/graphitemaster/494f21190bb2c63c5516
template <typename T1>
int offset_of(T1 ColumnFamilyOptions::*member) {
  return int(size_t(&(OptionsHelper::dummy_cf_options.*member)) -
             size_t(&OptionsHelper::dummy_cf_options));
}
template <typename T1>
int offset_of(T1 AdvancedColumnFamilyOptions::*member) {
  return int(size_t(&(OptionsHelper::dummy_cf_options.*member)) -
             size_t(&OptionsHelper::dummy_cf_options));
}
template <typename T1>
int offset_of(T1 CompactionOptionsFIFO::*member) {
  return int(size_t(&(OptionsHelper::dummy_comp_options.*member)) -
             size_t(&OptionsHelper::dummy_comp_options));
}
template <typename T1>
int offset_of(T1 LRUCacheOptions::*member) {
  return int(size_t(&(OptionsHelper::dummy_lru_cache_options.*member)) -
             size_t(&OptionsHelper::dummy_lru_cache_options));
}
template <typename T1>
int offset_of(T1 CompactionOptionsUniversal::*member) {
  return int(size_t(&(OptionsHelper::dummy_comp_options_universal.*member)) -
             size_t(&OptionsHelper::dummy_comp_options_universal));
}

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::cf_options_type_info = {
        /* not yet supported
        CompressionOptions compression_opts;
        TablePropertiesCollectorFactories table_properties_collector_factories;
        typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
            TablePropertiesCollectorFactories;
        UpdateStatus (*inplace_callback)(char* existing_value,
                                         uint34_t* existing_value_size,
                                         Slice delta_value,
                                         std::string* merged_value);
        std::vector<DbPath> cf_paths;
         */
        {"report_bg_io_stats",
         {offset_of(&ColumnFamilyOptions::report_bg_io_stats),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, report_bg_io_stats)}},
        {"compaction_measure_io_stats",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"disable_auto_compactions",
         {offset_of(&ColumnFamilyOptions::disable_auto_compactions),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, disable_auto_compactions)}},
        {"filter_deletes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, true,
          0}},
        {"inplace_update_support",
         {offset_of(&ColumnFamilyOptions::inplace_update_support),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"level_compaction_dynamic_level_bytes",
         {offset_of(&ColumnFamilyOptions::level_compaction_dynamic_level_bytes),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"optimize_filters_for_hits",
         {offset_of(&ColumnFamilyOptions::optimize_filters_for_hits),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"paranoid_file_checks",
         {offset_of(&ColumnFamilyOptions::paranoid_file_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, paranoid_file_checks)}},
        {"force_consistency_checks",
         {offset_of(&ColumnFamilyOptions::force_consistency_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"purge_redundant_kvs_while_flush",
         {offset_of(&ColumnFamilyOptions::purge_redundant_kvs_while_flush),
          OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}},
        {"verify_checksums_in_compaction",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, true,
          0}},
        {"soft_pending_compaction_bytes_limit",
         {offset_of(&ColumnFamilyOptions::soft_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions,
                   soft_pending_compaction_bytes_limit)}},
        {"hard_pending_compaction_bytes_limit",
         {offset_of(&ColumnFamilyOptions::hard_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions,
                   hard_pending_compaction_bytes_limit)}},
        {"hard_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated, true,
          0}},
        {"soft_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated, true,
          0}},
        {"max_compaction_bytes",
         {offset_of(&ColumnFamilyOptions::max_compaction_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, max_compaction_bytes)}},
        {"expanded_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated, true, 0}},
        {"level0_file_num_compaction_trigger",
         {offset_of(&ColumnFamilyOptions::level0_file_num_compaction_trigger),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions,
                   level0_file_num_compaction_trigger)}},
        {"level0_slowdown_writes_trigger",
         {offset_of(&ColumnFamilyOptions::level0_slowdown_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, level0_slowdown_writes_trigger)}},
        {"level0_stop_writes_trigger",
         {offset_of(&ColumnFamilyOptions::level0_stop_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, level0_stop_writes_trigger)}},
        {"max_grandparent_overlap_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated, true, 0}},
        {"max_mem_compaction_level",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated, false, 0}},
        {"max_write_buffer_number",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_number),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, max_write_buffer_number)}},
        {"max_write_buffer_number_to_maintain",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_number_to_maintain),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"min_write_buffer_number_to_merge",
         {offset_of(&ColumnFamilyOptions::min_write_buffer_number_to_merge),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"num_levels",
         {offset_of(&ColumnFamilyOptions::num_levels), OptionType::kInt,
          OptionVerificationType::kNormal, false, 0}},
        {"source_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated, true, 0}},
        {"target_file_size_multiplier",
         {offset_of(&ColumnFamilyOptions::target_file_size_multiplier),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, target_file_size_multiplier)}},
        {"arena_block_size",
         {offset_of(&ColumnFamilyOptions::arena_block_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, arena_block_size)}},
        {"inplace_update_num_locks",
         {offset_of(&ColumnFamilyOptions::inplace_update_num_locks),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, inplace_update_num_locks)}},
        {"max_successive_merges",
         {offset_of(&ColumnFamilyOptions::max_successive_merges),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, max_successive_merges)}},
        {"memtable_huge_page_size",
         {offset_of(&ColumnFamilyOptions::memtable_huge_page_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, memtable_huge_page_size)}},
        {"memtable_prefix_bloom_huge_page_tlb_size",
         {0, OptionType::kSizeT, OptionVerificationType::kDeprecated, true, 0}},
        {"write_buffer_size",
         {offset_of(&ColumnFamilyOptions::write_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, write_buffer_size)}},
        {"bloom_locality",
         {offset_of(&ColumnFamilyOptions::bloom_locality), OptionType::kUInt32T,
          OptionVerificationType::kNormal, false, 0}},
        {"memtable_prefix_bloom_bits",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated, true,
          0}},
        {"memtable_prefix_bloom_size_ratio",
         {offset_of(&ColumnFamilyOptions::memtable_prefix_bloom_size_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, memtable_prefix_bloom_size_ratio)}},
        {"memtable_prefix_bloom_probes",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated, true,
          0}},
        {"memtable_whole_key_filtering",
         {offset_of(&ColumnFamilyOptions::memtable_whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, memtable_whole_key_filtering)}},
        {"min_partial_merge_operands",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated, true,
          0}},
        {"max_bytes_for_level_base",
         {offset_of(&ColumnFamilyOptions::max_bytes_for_level_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, max_bytes_for_level_base)}},
        {"max_bytes_for_level_multiplier",
         {offset_of(&ColumnFamilyOptions::max_bytes_for_level_multiplier),
          OptionType::kDouble, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, max_bytes_for_level_multiplier)}},
        {"max_bytes_for_level_multiplier_additional",
         {offset_of(
              &ColumnFamilyOptions::max_bytes_for_level_multiplier_additional),
          OptionType::kVectorInt, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions,
                   max_bytes_for_level_multiplier_additional)}},
        {"max_sequential_skip_in_iterations",
         {offset_of(&ColumnFamilyOptions::max_sequential_skip_in_iterations),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions,
                   max_sequential_skip_in_iterations)}},
        {"target_file_size_base",
         {offset_of(&ColumnFamilyOptions::target_file_size_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, target_file_size_base)}},
        {"rate_limit_delay_max_milliseconds",
         {0, OptionType::kUInt, OptionVerificationType::kDeprecated, false, 0}},
        {"compression",
         {offset_of(&ColumnFamilyOptions::compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, compression)}},
        {"compression_per_level",
         {offset_of(&ColumnFamilyOptions::compression_per_level),
          OptionType::kVectorCompressionType, OptionVerificationType::kNormal,
          false, 0}},
        {"bottommost_compression",
         {offset_of(&ColumnFamilyOptions::bottommost_compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal, false,
          0}},
        {"comparator",
         {offset_of(&ColumnFamilyOptions::comparator), OptionType::kComparator,
          OptionVerificationType::kByName, false, 0}},
        {"prefix_extractor",
         {offset_of(&ColumnFamilyOptions::prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          true, offsetof(struct MutableCFOptions, prefix_extractor)}},
        {"memtable_insert_with_hint_prefix_extractor",
         {offset_of(
              &ColumnFamilyOptions::memtable_insert_with_hint_prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          false, 0}},
        {"memtable_factory",
         {offset_of(&ColumnFamilyOptions::memtable_factory),
          OptionType::kMemTableRepFactory, OptionVerificationType::kByName,
          false, 0}},
        {"table_factory",
         {offset_of(&ColumnFamilyOptions::table_factory),
          OptionType::kTableFactory, OptionVerificationType::kByName, false,
          0}},
        {"compaction_filter",
         {offset_of(&ColumnFamilyOptions::compaction_filter),
          OptionType::kCompactionFilter, OptionVerificationType::kByName, false,
          0}},
        {"compaction_filter_factory",
         {offset_of(&ColumnFamilyOptions::compaction_filter_factory),
          OptionType::kCompactionFilterFactory, OptionVerificationType::kByName,
          false, 0}},
        {"merge_operator",
         {offset_of(&ColumnFamilyOptions::merge_operator),
          OptionType::kMergeOperator,
          OptionVerificationType::kByNameAllowFromNull, false, 0}},
        {"compaction_style",
         {offset_of(&ColumnFamilyOptions::compaction_style),
          OptionType::kCompactionStyle, OptionVerificationType::kNormal, false,
          0}},
        {"compaction_pri",
         {offset_of(&ColumnFamilyOptions::compaction_pri),
          OptionType::kCompactionPri, OptionVerificationType::kNormal, false,
          0}},
        {"compaction_options_fifo",
         {offset_of(&ColumnFamilyOptions::compaction_options_fifo),
          OptionType::kCompactionOptionsFIFO, OptionVerificationType::kNormal,
          true, offsetof(struct MutableCFOptions, compaction_options_fifo)}},
        {"compaction_options_universal",
         {offset_of(&ColumnFamilyOptions::compaction_options_universal),
          OptionType::kCompactionOptionsUniversal,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, compaction_options_universal)}},
        {"ttl",
         {offset_of(&ColumnFamilyOptions::ttl), OptionType::kUInt64T,
          OptionVerificationType::kNormal, true,
          offsetof(struct MutableCFOptions, ttl)}}};

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::fifo_compaction_options_type_info = {
        {"max_table_files_size",
         {offset_of(&CompactionOptionsFIFO::max_table_files_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal, true,
          offsetof(struct CompactionOptionsFIFO, max_table_files_size)}},
        {"ttl",
         {0, OptionType::kUInt64T,
          OptionVerificationType::kDeprecated, false,
          0}},
        {"allow_compaction",
         {offset_of(&CompactionOptionsFIFO::allow_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct CompactionOptionsFIFO, allow_compaction)}}};

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::universal_compaction_options_type_info = {
        {"size_ratio",
         {offset_of(&CompactionOptionsUniversal::size_ratio), OptionType::kUInt,
          OptionVerificationType::kNormal, true,
          offsetof(class CompactionOptionsUniversal, size_ratio)}},
        {"min_merge_width",
         {offset_of(&CompactionOptionsUniversal::min_merge_width),
          OptionType::kUInt, OptionVerificationType::kNormal, true,
          offsetof(class CompactionOptionsUniversal, min_merge_width)}},
        {"max_merge_width",
         {offset_of(&CompactionOptionsUniversal::max_merge_width),
          OptionType::kUInt, OptionVerificationType::kNormal, true,
          offsetof(class CompactionOptionsUniversal, max_merge_width)}},
        {"max_size_amplification_percent",
         {offset_of(
              &CompactionOptionsUniversal::max_size_amplification_percent),
          OptionType::kUInt, OptionVerificationType::kNormal, true,
          offsetof(class CompactionOptionsUniversal,
                   max_size_amplification_percent)}},
        {"compression_size_percent",
         {offset_of(&CompactionOptionsUniversal::compression_size_percent),
          OptionType::kInt, OptionVerificationType::kNormal, true,
          offsetof(class CompactionOptionsUniversal,
                   compression_size_percent)}},
        {"stop_style",
         {offset_of(&CompactionOptionsUniversal::stop_style),
          OptionType::kCompactionStopStyle, OptionVerificationType::kNormal,
          true, offsetof(class CompactionOptionsUniversal, stop_style)}},
        {"allow_trivial_move",
         {offset_of(&CompactionOptionsUniversal::allow_trivial_move),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(class CompactionOptionsUniversal, allow_trivial_move)}}};

std::unordered_map<std::string, CompactionStopStyle>
    OptionsHelper::compaction_stop_style_string_map = {
        {"kCompactionStopStyleSimilarSize", kCompactionStopStyleSimilarSize},
        {"kCompactionStopStyleTotalSize", kCompactionStopStyleTotalSize}};

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::lru_cache_options_type_info = {
        {"capacity",
         {offset_of(&LRUCacheOptions::capacity), OptionType::kSizeT,
          OptionVerificationType::kNormal, true,
          offsetof(struct LRUCacheOptions, capacity)}},
        {"num_shard_bits",
         {offset_of(&LRUCacheOptions::num_shard_bits), OptionType::kInt,
          OptionVerificationType::kNormal, true,
          offsetof(struct LRUCacheOptions, num_shard_bits)}},
        {"strict_capacity_limit",
         {offset_of(&LRUCacheOptions::strict_capacity_limit),
          OptionType::kBoolean, OptionVerificationType::kNormal, true,
          offsetof(struct LRUCacheOptions, strict_capacity_limit)}},
        {"high_pri_pool_ratio",
         {offset_of(&LRUCacheOptions::high_pri_pool_ratio), OptionType::kDouble,
          OptionVerificationType::kNormal, true,
          offsetof(struct LRUCacheOptions, high_pri_pool_ratio)}}};

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
