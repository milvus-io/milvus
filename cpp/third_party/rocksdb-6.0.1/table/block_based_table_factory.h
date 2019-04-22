//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <memory>
#include <string>

#include "db/dbformat.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/table.h"

namespace rocksdb {

struct EnvOptions;

using std::unique_ptr;
class BlockBasedTableBuilder;

// A class used to track actual bytes written from the tail in the recent SST
// file opens, and provide a suggestion for following open.
class TailPrefetchStats {
 public:
  void RecordEffectiveSize(size_t len);
  // 0 indicates no information to determine.
  size_t GetSuggestedPrefetchSize();

 private:
  const static size_t kNumTracked = 32;
  size_t records_[kNumTracked];
  port::Mutex mutex_;
  size_t next_ = 0;
  size_t num_records_ = 0;
};

class BlockBasedTableFactory : public TableFactory {
 public:
  explicit BlockBasedTableFactory(
      const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

  ~BlockBasedTableFactory() {}

  const char* Name() const override { return kName.c_str(); }

  Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;

  std::string GetPrintableTableOptions() const override;

  Status GetOptionString(std::string* opt_string,
                         const std::string& delimiter) const override;

  const BlockBasedTableOptions& table_options() const;

  void* GetOptions() override { return &table_options_; }

  bool IsDeleteRangeSupported() const override { return true; }

  static const std::string kName;

 private:
  BlockBasedTableOptions table_options_;
  mutable TailPrefetchStats tail_prefetch_stats_;
};

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
extern const std::string kPropTrue;
extern const std::string kPropFalse;

#ifndef ROCKSDB_LITE
extern Status VerifyBlockBasedTableFactory(
    const BlockBasedTableFactory* base_tf,
    const BlockBasedTableFactory* file_tf,
    OptionsSanityCheckLevel sanity_check_level);

static std::unordered_map<std::string, OptionTypeInfo>
    block_based_table_type_info = {
        /* currently not supported
          std::shared_ptr<Cache> block_cache = nullptr;
          std::shared_ptr<Cache> block_cache_compressed = nullptr;
         */
        {"flush_block_policy_factory",
         {offsetof(struct BlockBasedTableOptions, flush_block_policy_factory),
          OptionType::kFlushBlockPolicyFactory, OptionVerificationType::kByName,
          false, 0}},
        {"cache_index_and_filter_blocks",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"cache_index_and_filter_blocks_with_high_priority",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks_with_high_priority),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"pin_l0_filter_and_index_blocks_in_cache",
         {offsetof(struct BlockBasedTableOptions,
                   pin_l0_filter_and_index_blocks_in_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"index_type",
         {offsetof(struct BlockBasedTableOptions, index_type),
          OptionType::kBlockBasedTableIndexType,
          OptionVerificationType::kNormal, false, 0}},
        {"hash_index_allow_collision",
         {offsetof(struct BlockBasedTableOptions, hash_index_allow_collision),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"data_block_index_type",
         {offsetof(struct BlockBasedTableOptions, data_block_index_type),
          OptionType::kBlockBasedTableDataBlockIndexType,
          OptionVerificationType::kNormal, false, 0}},
        {"data_block_hash_table_util_ratio",
         {offsetof(struct BlockBasedTableOptions,
                   data_block_hash_table_util_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal, false, 0}},
        {"checksum",
         {offsetof(struct BlockBasedTableOptions, checksum),
          OptionType::kChecksumType, OptionVerificationType::kNormal, false,
          0}},
        {"no_block_cache",
         {offsetof(struct BlockBasedTableOptions, no_block_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"block_size",
         {offsetof(struct BlockBasedTableOptions, block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
        {"block_size_deviation",
         {offsetof(struct BlockBasedTableOptions, block_size_deviation),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"index_block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, index_block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"index_per_partition",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated, false,
          0}},
        {"metadata_block_size",
         {offsetof(struct BlockBasedTableOptions, metadata_block_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"partition_filters",
         {offsetof(struct BlockBasedTableOptions, partition_filters),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"filter_policy",
         {offsetof(struct BlockBasedTableOptions, filter_policy),
          OptionType::kFilterPolicy, OptionVerificationType::kByName, false,
          0}},
        {"whole_key_filtering",
         {offsetof(struct BlockBasedTableOptions, whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"skip_table_builder_flush",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false,
          0}},
        {"format_version",
         {offsetof(struct BlockBasedTableOptions, format_version),
          OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
        {"verify_compression",
         {offsetof(struct BlockBasedTableOptions, verify_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"read_amp_bytes_per_bit",
         {offsetof(struct BlockBasedTableOptions, read_amp_bytes_per_bit),
          OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
        {"enable_index_compression",
         {offsetof(struct BlockBasedTableOptions, enable_index_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"block_align",
         {offsetof(struct BlockBasedTableOptions, block_align),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"pin_top_level_index_and_filter",
         {offsetof(struct BlockBasedTableOptions,
                   pin_top_level_index_and_filter),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}}};
#endif  // !ROCKSDB_LITE
}  // namespace rocksdb
