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
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/flush_block_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/compression.h"

namespace rocksdb {

class BlockBuilder;
class BlockHandle;
class WritableFile;
struct BlockBasedTableOptions;

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;

class BlockBasedTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  BlockBasedTableBuilder(
      const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_comparator,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          int_tbl_prop_collector_factories,
      uint32_t column_family_id, WritableFileWriter* file,
      const CompressionType compression_type,
      const CompressionOptions& compression_opts, const bool skip_filters,
      const std::string& column_family_name, const uint64_t creation_time = 0,
      const uint64_t oldest_key_time = 0, const uint64_t target_file_size = 0);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~BlockBasedTableBuilder();

  // No copying allowed
  BlockBasedTableBuilder(const BlockBasedTableBuilder&) = delete;
  BlockBasedTableBuilder& operator=(const BlockBasedTableBuilder&) = delete;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

 private:
  bool ok() const { return status().ok(); }

  // Transition state from buffered to unbuffered. See `Rep::State` API comment
  // for details of the states.
  // REQUIRES: `rep_->state == kBuffered`
  void EnterUnbuffered();

  // Call block's Finish() method
  // and then write the compressed block contents to file.
  void WriteBlock(BlockBuilder* block, BlockHandle* handle, bool is_data_block);

  // Compress and write block content to the file.
  void WriteBlock(const Slice& block_contents, BlockHandle* handle,
                  bool is_data_block);
  // Directly write data to the file.
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle,
                     bool is_data_block = false);
  Status InsertBlockInCache(const Slice& block_contents,
                            const CompressionType type,
                            const BlockHandle* handle);

  void WriteFilterBlock(MetaIndexBuilder* meta_index_builder);
  void WriteIndexBlock(MetaIndexBuilder* meta_index_builder,
                       BlockHandle* index_block_handle);
  void WritePropertiesBlock(MetaIndexBuilder* meta_index_builder);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder);
  void WriteRangeDelBlock(MetaIndexBuilder* meta_index_builder);
  void WriteFooter(BlockHandle& metaindex_block_handle,
                   BlockHandle& index_block_handle);

  struct Rep;
  class BlockBasedTablePropertiesCollectorFactory;
  class BlockBasedTablePropertiesCollector;
  Rep* rep_;

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush();

  // Some compression libraries fail when the raw size is bigger than int. If
  // uncompressed size is bigger than kCompressionSizeLimit, don't compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();
};

Slice CompressBlock(const Slice& raw, const CompressionInfo& info,
                    CompressionType* type, uint32_t format_version,
                    std::string* compressed_output);

}  // namespace rocksdb
