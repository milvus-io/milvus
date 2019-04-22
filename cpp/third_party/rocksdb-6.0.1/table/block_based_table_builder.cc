//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based_table_builder.h"

#include <assert.h>
#include <stdio.h>

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "db/dbformat.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table.h"

#include "table/block.h"
#include "table/block_based_filter_block.h"
#include "table/block_based_table_factory.h"
#include "table/block_based_table_reader.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/full_filter_block.h"
#include "table/table_builder.h"

#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/memory_allocator.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

#include "table/index_builder.h"
#include "table/partitioned_filter_block.h"

namespace rocksdb {

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;

typedef BlockBasedTableOptions::IndexType IndexType;

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

// Create a filter block builder based on its type.
FilterBlockBuilder* CreateFilterBlockBuilder(
    const ImmutableCFOptions& /*opt*/, const MutableCFOptions& mopt,
    const BlockBasedTableOptions& table_opt,
    const bool use_delta_encoding_for_index_values,
    PartitionedIndexBuilder* const p_index_builder) {
  if (table_opt.filter_policy == nullptr) return nullptr;

  FilterBitsBuilder* filter_bits_builder =
      table_opt.filter_policy->GetFilterBitsBuilder();
  if (filter_bits_builder == nullptr) {
    return new BlockBasedFilterBlockBuilder(mopt.prefix_extractor.get(),
                                            table_opt);
  } else {
    if (table_opt.partition_filters) {
      assert(p_index_builder != nullptr);
      // Since after partition cut request from filter builder it takes time
      // until index builder actully cuts the partition, we take the lower bound
      // as partition size.
      assert(table_opt.block_size_deviation <= 100);
      auto partition_size = static_cast<uint32_t>(
          ((table_opt.metadata_block_size *
          (100 - table_opt.block_size_deviation)) + 99) / 100);
      partition_size = std::max(partition_size, static_cast<uint32_t>(1));
      return new PartitionedFilterBlockBuilder(
          mopt.prefix_extractor.get(), table_opt.whole_key_filtering,
          filter_bits_builder, table_opt.index_block_restart_interval,
          use_delta_encoding_for_index_values, p_index_builder, partition_size);
    } else {
      return new FullFilterBlockBuilder(mopt.prefix_extractor.get(),
                                        table_opt.whole_key_filtering,
                                        filter_bits_builder);
    }
  }
}

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

}  // namespace

// format_version is the block format as defined in include/rocksdb/table.h
Slice CompressBlock(const Slice& raw, const CompressionInfo& compression_info,
                    CompressionType* type, uint32_t format_version,
                    std::string* compressed_output) {
  *type = compression_info.type();
  if (compression_info.type() == kNoCompression) {
    return raw;
  }

  // Will return compressed block contents if (1) the compression method is
  // supported in this platform and (2) the compression rate is "good enough".
  switch (compression_info.type()) {
    case kSnappyCompression:
      if (Snappy_Compress(compression_info, raw.data(), raw.size(),
                          compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kZlibCompression:
      if (Zlib_Compress(
              compression_info,
              GetCompressFormatForVersion(kZlibCompression, format_version),
              raw.data(), raw.size(), compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kBZip2Compression:
      if (BZip2_Compress(
              compression_info,
              GetCompressFormatForVersion(kBZip2Compression, format_version),
              raw.data(), raw.size(), compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kLZ4Compression:
      if (LZ4_Compress(
              compression_info,
              GetCompressFormatForVersion(kLZ4Compression, format_version),
              raw.data(), raw.size(), compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;  // fall back to no compression.
    case kLZ4HCCompression:
      if (LZ4HC_Compress(
              compression_info,
              GetCompressFormatForVersion(kLZ4HCCompression, format_version),
              raw.data(), raw.size(), compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;     // fall back to no compression.
    case kXpressCompression:
      if (XPRESS_Compress(raw.data(), raw.size(),
          compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      if (ZSTD_Compress(compression_info, raw.data(), raw.size(),
                        compressed_output) &&
          GoodCompressionRatio(compressed_output->size(), raw.size())) {
        return *compressed_output;
      }
      break;     // fall back to no compression.
    default: {}  // Do not recognize this compression type
  }

  // Compression method is not supported, or not good compression ratio, so just
  // fall back to uncompressed form.
  *type = kNoCompression;
  return raw;
}

// kBlockBasedTableMagicNumber was picked by running
//    echo rocksdb.table.block_based | sha1sum
// and taking the leading 64 bits.
// Please note that kBlockBasedTableMagicNumber may also be accessed by other
// .cc files
// for that reason we declare it extern in the header but to get the space
// allocated
// it must be not extern in one place.
const uint64_t kBlockBasedTableMagicNumber = 0x88e241b785f4cff7ull;
// We also support reading and writing legacy block based table format (for
// backwards compatibility)
const uint64_t kLegacyBlockBasedTableMagicNumber = 0xdb4775248b80fb57ull;

// A collector that collects properties of interest to block-based table.
// For now this class looks heavy-weight since we only write one additional
// property.
// But in the foreseeable future, we will add more and more properties that are
// specific to block-based table.
class BlockBasedTableBuilder::BlockBasedTablePropertiesCollector
    : public IntTblPropCollector {
 public:
  explicit BlockBasedTablePropertiesCollector(
      BlockBasedTableOptions::IndexType index_type, bool whole_key_filtering,
      bool prefix_filtering)
      : index_type_(index_type),
        whole_key_filtering_(whole_key_filtering),
        prefix_filtering_(prefix_filtering) {}

  Status InternalAdd(const Slice& /*key*/, const Slice& /*value*/,
                     uint64_t /*file_size*/) override {
    // Intentionally left blank. Have no interest in collecting stats for
    // individual key/value pairs.
    return Status::OK();
  }

  Status Finish(UserCollectedProperties* properties) override {
    std::string val;
    PutFixed32(&val, static_cast<uint32_t>(index_type_));
    properties->insert({BlockBasedTablePropertyNames::kIndexType, val});
    properties->insert({BlockBasedTablePropertyNames::kWholeKeyFiltering,
                        whole_key_filtering_ ? kPropTrue : kPropFalse});
    properties->insert({BlockBasedTablePropertyNames::kPrefixFiltering,
                        prefix_filtering_ ? kPropTrue : kPropFalse});
    return Status::OK();
  }

  // The name of the properties collector can be used for debugging purpose.
  const char* Name() const override {
    return "BlockBasedTablePropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override {
    // Intentionally left blank.
    return UserCollectedProperties();
  }

 private:
  BlockBasedTableOptions::IndexType index_type_;
  bool whole_key_filtering_;
  bool prefix_filtering_;
};

struct BlockBasedTableBuilder::Rep {
  const ImmutableCFOptions ioptions;
  const MutableCFOptions moptions;
  const BlockBasedTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  WritableFileWriter* file;
  uint64_t offset = 0;
  Status status;
  size_t alignment;
  BlockBuilder data_block;
  // Buffers uncompressed data blocks and keys to replay later. Needed when
  // compression dictionary is enabled so we can finalize the dictionary before
  // compressing any data blocks.
  // TODO(ajkr): ideally we don't buffer all keys and all uncompressed data
  // blocks as it's redundant, but it's easier to implement for now.
  std::vector<std::pair<std::string, std::vector<std::string>>>
      data_block_and_keys_buffers;
  BlockBuilder range_del_block;

  InternalKeySliceTransform internal_prefix_transform;
  std::unique_ptr<IndexBuilder> index_builder;
  PartitionedIndexBuilder* p_index_builder_ = nullptr;

  std::string last_key;
  CompressionType compression_type;
  CompressionOptions compression_opts;
  std::unique_ptr<CompressionDict> compression_dict;
  CompressionContext compression_ctx;
  std::unique_ptr<UncompressionContext> verify_ctx;
  std::unique_ptr<UncompressionDict> verify_dict;

  size_t data_begin_offset = 0;

  TableProperties props;

  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  //
  // - `kClosed`: This indicates either `Finish()` or `Abandon()` has been
  //   called, so the table builder is no longer usable. We must be in this
  //   state by the time the destructor runs.
  enum class State {
    kBuffered,
    kUnbuffered,
    kClosed,
  };
  State state;

  const bool use_delta_encoding_for_index_values;
  std::unique_ptr<FilterBlockBuilder> filter_builder;
  char compressed_cache_key_prefix[BlockBasedTable::kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size;

  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;
  uint32_t column_family_id;
  const std::string& column_family_name;
  uint64_t creation_time = 0;
  uint64_t oldest_key_time = 0;
  const uint64_t target_file_size;

  std::vector<std::unique_ptr<IntTblPropCollector>> table_properties_collectors;

  Rep(const ImmutableCFOptions& _ioptions, const MutableCFOptions& _moptions,
      const BlockBasedTableOptions& table_opt,
      const InternalKeyComparator& icomparator,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          int_tbl_prop_collector_factories,
      uint32_t _column_family_id, WritableFileWriter* f,
      const CompressionType _compression_type,
      const CompressionOptions& _compression_opts, const bool skip_filters,
      const std::string& _column_family_name, const uint64_t _creation_time,
      const uint64_t _oldest_key_time, const uint64_t _target_file_size)
      : ioptions(_ioptions),
        moptions(_moptions),
        table_options(table_opt),
        internal_comparator(icomparator),
        file(f),
        alignment(table_options.block_align
                      ? std::min(table_options.block_size, kDefaultPageSize)
                      : 0),
        data_block(table_options.block_restart_interval,
                   table_options.use_delta_encoding,
                   false /* use_value_delta_encoding */,
                   icomparator.user_comparator()
                           ->CanKeysWithDifferentByteContentsBeEqual()
                       ? BlockBasedTableOptions::kDataBlockBinarySearch
                       : table_options.data_block_index_type,
                   table_options.data_block_hash_table_util_ratio),
        range_del_block(1 /* block_restart_interval */),
        internal_prefix_transform(_moptions.prefix_extractor.get()),
        compression_type(_compression_type),
        compression_opts(_compression_opts),
        compression_dict(),
        compression_ctx(_compression_type),
        verify_dict(),
        state((_compression_opts.max_dict_bytes > 0) ? State::kBuffered
                                                     : State::kUnbuffered),
        use_delta_encoding_for_index_values(table_opt.format_version >= 4 &&
                                            !table_opt.block_align),
        compressed_cache_key_prefix_size(0),
        flush_block_policy(
            table_options.flush_block_policy_factory->NewFlushBlockPolicy(
                table_options, data_block)),
        column_family_id(_column_family_id),
        column_family_name(_column_family_name),
        creation_time(_creation_time),
        oldest_key_time(_oldest_key_time),
        target_file_size(_target_file_size) {
    if (table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      p_index_builder_ = PartitionedIndexBuilder::CreateIndexBuilder(
          &internal_comparator, use_delta_encoding_for_index_values,
          table_options);
      index_builder.reset(p_index_builder_);
    } else {
      index_builder.reset(IndexBuilder::CreateIndexBuilder(
          table_options.index_type, &internal_comparator,
          &this->internal_prefix_transform, use_delta_encoding_for_index_values,
          table_options));
    }
    if (skip_filters) {
      filter_builder = nullptr;
    } else {
      filter_builder.reset(CreateFilterBlockBuilder(
          _ioptions, _moptions, table_options,
          use_delta_encoding_for_index_values, p_index_builder_));
    }

    for (auto& collector_factories : *int_tbl_prop_collector_factories) {
      table_properties_collectors.emplace_back(
          collector_factories->CreateIntTblPropCollector(column_family_id));
    }
    table_properties_collectors.emplace_back(
        new BlockBasedTablePropertiesCollector(
            table_options.index_type, table_options.whole_key_filtering,
            _moptions.prefix_extractor != nullptr));
    if (table_options.verify_compression) {
      verify_ctx.reset(new UncompressionContext(UncompressionContext::NoCache(),
                                                compression_type));
    }
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;

  ~Rep() {}
};

BlockBasedTableBuilder::BlockBasedTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file,
    const CompressionType compression_type,
    const CompressionOptions& compression_opts, const bool skip_filters,
    const std::string& column_family_name, const uint64_t creation_time,
    const uint64_t oldest_key_time, const uint64_t target_file_size) {
  BlockBasedTableOptions sanitized_table_options(table_options);
  if (sanitized_table_options.format_version == 0 &&
      sanitized_table_options.checksum != kCRC32c) {
    ROCKS_LOG_WARN(
        ioptions.info_log,
        "Silently converting format_version to 1 because checksum is "
        "non-default");
    // silently convert format_version to 1 to keep consistent with current
    // behavior
    sanitized_table_options.format_version = 1;
  }

  rep_ = new Rep(ioptions, moptions, sanitized_table_options,
                 internal_comparator, int_tbl_prop_collector_factories,
                 column_family_id, file, compression_type, compression_opts,
                 skip_filters, column_family_name, creation_time,
                 oldest_key_time, target_file_size);

  if (rep_->filter_builder != nullptr) {
    rep_->filter_builder->StartBlock(0);
  }
  if (table_options.block_cache_compressed.get() != nullptr) {
    BlockBasedTable::GenerateCachePrefix(
        table_options.block_cache_compressed.get(), file->writable_file(),
        &rep_->compressed_cache_key_prefix[0],
        &rep_->compressed_cache_key_prefix_size);
  }
}

BlockBasedTableBuilder::~BlockBasedTableBuilder() {
  // Catch errors where caller forgot to call Finish()
  assert(rep_->state == Rep::State::kClosed);
  delete rep_;
}

void BlockBasedTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(rep_->state != Rep::State::kClosed);
  if (!ok()) return;
  ValueType value_type = ExtractValueType(key);
  if (IsValueType(value_type)) {
#ifndef NDEBUG
    if (r->props.num_entries > r->props.num_range_deletions) {
      assert(r->internal_comparator.Compare(key, Slice(r->last_key)) > 0);
    }
#endif  // NDEBUG

    auto should_flush = r->flush_block_policy->Update(key, value);
    if (should_flush) {
      assert(!r->data_block.empty());
      Flush();

      if (r->state == Rep::State::kBuffered &&
          r->data_begin_offset > r->target_file_size) {
        EnterUnbuffered();
      }

      // Add item to index block.
      // We do not emit the index entry for a block until we have seen the
      // first key for the next data block.  This allows us to use shorter
      // keys in the index block.  For example, consider a block boundary
      // between the keys "the quick brown fox" and "the who".  We can use
      // "the r" as the key for the index block entry since it is >= all
      // entries in the first block and < all entries in subsequent
      // blocks.
      if (ok() && r->state == Rep::State::kUnbuffered) {
        r->index_builder->AddIndexEntry(&r->last_key, &key, r->pending_handle);
      }
    }

    // Note: PartitionedFilterBlockBuilder requires key being added to filter
    // builder after being added to index builder.
    if (r->state == Rep::State::kUnbuffered && r->filter_builder != nullptr) {
      r->filter_builder->Add(ExtractUserKey(key));
    }

    r->last_key.assign(key.data(), key.size());
    r->data_block.Add(key, value);
    if (r->state == Rep::State::kBuffered) {
      // Buffer keys to be replayed during `Finish()` once compression
      // dictionary has been finalized.
      if (r->data_block_and_keys_buffers.empty() || should_flush) {
        r->data_block_and_keys_buffers.emplace_back();
      }
      r->data_block_and_keys_buffers.back().second.emplace_back(key.ToString());
    } else {
      r->index_builder->OnKeyAdded(key);
    }
    NotifyCollectTableCollectorsOnAdd(key, value, r->offset,
                                      r->table_properties_collectors,
                                      r->ioptions.info_log);

  } else if (value_type == kTypeRangeDeletion) {
    r->range_del_block.Add(key, value);
    NotifyCollectTableCollectorsOnAdd(key, value, r->offset,
                                      r->table_properties_collectors,
                                      r->ioptions.info_log);
  } else {
    assert(false);
  }

  r->props.num_entries++;
  r->props.raw_key_size += key.size();
  r->props.raw_value_size += value.size();
  if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion) {
    r->props.num_deletions++;
  } else if (value_type == kTypeRangeDeletion) {
    r->props.num_deletions++;
    r->props.num_range_deletions++;
  } else if (value_type == kTypeMerge) {
    r->props.num_merge_operands++;
  }
}

void BlockBasedTableBuilder::Flush() {
  Rep* r = rep_;
  assert(rep_->state != Rep::State::kClosed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  WriteBlock(&r->data_block, &r->pending_handle, true /* is_data_block */);
}

void BlockBasedTableBuilder::WriteBlock(BlockBuilder* block,
                                        BlockHandle* handle,
                                        bool is_data_block) {
  WriteBlock(block->Finish(), handle, is_data_block);
  block->Reset();
}

void BlockBasedTableBuilder::WriteBlock(const Slice& raw_block_contents,
                                        BlockHandle* handle,
                                        bool is_data_block) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;

  auto type = r->compression_type;
  Slice block_contents;
  bool abort_compression = false;

  StopWatchNano timer(r->ioptions.env,
    ShouldReportDetailedTime(r->ioptions.env, r->ioptions.statistics));

  if (r->state == Rep::State::kBuffered) {
    assert(is_data_block);
    assert(!r->data_block_and_keys_buffers.empty());
    r->data_block_and_keys_buffers.back().first = raw_block_contents.ToString();
    r->data_begin_offset += r->data_block_and_keys_buffers.back().first.size();
    return;
  }

  if (raw_block_contents.size() < kCompressionSizeLimit) {
    const CompressionDict* compression_dict;
    if (!is_data_block || r->compression_dict == nullptr) {
      compression_dict = &CompressionDict::GetEmptyDict();
    } else {
      compression_dict = r->compression_dict.get();
    }
    assert(compression_dict != nullptr);
    CompressionInfo compression_info(r->compression_opts, r->compression_ctx,
                                     *compression_dict, r->compression_type);
    block_contents =
        CompressBlock(raw_block_contents, compression_info, &type,
                      r->table_options.format_version, &r->compressed_output);

    // Some of the compression algorithms are known to be unreliable. If
    // the verify_compression flag is set then try to de-compress the
    // compressed data and compare to the input.
    if (type != kNoCompression && r->table_options.verify_compression) {
      // Retrieve the uncompressed contents into a new buffer
      const UncompressionDict* verify_dict;
      if (!is_data_block || r->verify_dict == nullptr) {
        verify_dict = &UncompressionDict::GetEmptyDict();
      } else {
        verify_dict = r->verify_dict.get();
      }
      assert(verify_dict != nullptr);
      BlockContents contents;
      UncompressionInfo uncompression_info(*r->verify_ctx, *verify_dict,
                                           r->compression_type);
      Status stat = UncompressBlockContentsForCompressionType(
          uncompression_info, block_contents.data(), block_contents.size(),
          &contents, r->table_options.format_version, r->ioptions);

      if (stat.ok()) {
        bool compressed_ok = contents.data.compare(raw_block_contents) == 0;
        if (!compressed_ok) {
          // The result of the compression was invalid. abort.
          abort_compression = true;
          ROCKS_LOG_ERROR(r->ioptions.info_log,
                          "Decompressed block did not match raw block");
          r->status =
              Status::Corruption("Decompressed block did not match raw block");
        }
      } else {
        // Decompression reported an error. abort.
        r->status = Status::Corruption("Could not decompress");
        abort_compression = true;
      }
    }
  } else {
    // Block is too big to be compressed.
    abort_compression = true;
  }

  // Abort compression if the block is too big, or did not pass
  // verification.
  if (abort_compression) {
    RecordTick(r->ioptions.statistics, NUMBER_BLOCK_NOT_COMPRESSED);
    type = kNoCompression;
    block_contents = raw_block_contents;
  } else if (type != kNoCompression) {
    if (ShouldReportDetailedTime(r->ioptions.env, r->ioptions.statistics)) {
      MeasureTime(r->ioptions.statistics, COMPRESSION_TIMES_NANOS,
                  timer.ElapsedNanos());
    }
    MeasureTime(r->ioptions.statistics, BYTES_COMPRESSED,
                raw_block_contents.size());
    RecordTick(r->ioptions.statistics, NUMBER_BLOCK_COMPRESSED);
  } else if (type != r->compression_type) {
    RecordTick(r->ioptions.statistics, NUMBER_BLOCK_NOT_COMPRESSED);
  }

  WriteRawBlock(block_contents, type, handle, is_data_block);
  r->compressed_output.clear();
  if (is_data_block) {
    if (r->filter_builder != nullptr) {
      r->filter_builder->StartBlock(r->offset);
    }
    r->props.data_size = r->offset;
    ++r->props.num_data_blocks;
  }
}

void BlockBasedTableBuilder::WriteRawBlock(const Slice& block_contents,
                                           CompressionType type,
                                           BlockHandle* handle,
                                           bool is_data_block) {
  Rep* r = rep_;
  StopWatch sw(r->ioptions.env, r->ioptions.statistics, WRITE_RAW_BLOCK_MICROS);
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  assert(r->status.ok());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    char* trailer_without_type = trailer + 1;
    switch (r->table_options.checksum) {
      case kNoChecksum:
        EncodeFixed32(trailer_without_type, 0);
        break;
      case kCRC32c: {
        auto crc = crc32c::Value(block_contents.data(), block_contents.size());
        crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
        EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
        break;
      }
      case kxxHash: {
        void* xxh = XXH32_init(0);
        XXH32_update(xxh, block_contents.data(),
                     static_cast<uint32_t>(block_contents.size()));
        XXH32_update(xxh, trailer, 1);  // Extend  to cover block type
        EncodeFixed32(trailer_without_type, XXH32_digest(xxh));
        break;
      }
      case kxxHash64: {
        XXH64_state_t* const state = XXH64_createState();
        XXH64_reset(state, 0);
        XXH64_update(state, block_contents.data(),
                static_cast<uint32_t>(block_contents.size()));
        XXH64_update(state, trailer, 1);  // Extend  to cover block type
        EncodeFixed32(trailer_without_type,
          static_cast<uint32_t>(XXH64_digest(state) & // lower 32 bits
                                   uint64_t{0xffffffff}));
        XXH64_freeState(state);
        break;
      }
    }

    assert(r->status.ok());
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WriteRawBlock:TamperWithChecksum",
        static_cast<char*>(trailer));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->status = InsertBlockInCache(block_contents, type, handle);
    }
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
      if (r->table_options.block_align && is_data_block) {
        size_t pad_bytes =
            (r->alignment - ((block_contents.size() + kBlockTrailerSize) &
                             (r->alignment - 1))) &
            (r->alignment - 1);
        r->status = r->file->Pad(pad_bytes);
        if (r->status.ok()) {
          r->offset += pad_bytes;
        }
      }
    }
  }
}

Status BlockBasedTableBuilder::status() const {
  return rep_->status;
}

static void DeleteCachedBlockContents(const Slice& /*key*/, void* value) {
  BlockContents* bc = reinterpret_cast<BlockContents*>(value);
  delete bc;
}

//
// Make a copy of the block contents and insert into compressed block cache
//
Status BlockBasedTableBuilder::InsertBlockInCache(const Slice& block_contents,
                                                  const CompressionType type,
                                                  const BlockHandle* handle) {
  Rep* r = rep_;
  Cache* block_cache_compressed = r->table_options.block_cache_compressed.get();

  if (type != kNoCompression && block_cache_compressed != nullptr) {

    size_t size = block_contents.size();

    auto ubuf =
        AllocateBlock(size + 1, block_cache_compressed->memory_allocator());
    memcpy(ubuf.get(), block_contents.data(), size);
    ubuf[size] = type;

    BlockContents* block_contents_to_cache =
        new BlockContents(std::move(ubuf), size);
#ifndef NDEBUG
    block_contents_to_cache->is_raw_block = true;
#endif  // NDEBUG

    // make cache key by appending the file offset to the cache prefix id
    char* end = EncodeVarint64(
                  r->compressed_cache_key_prefix +
                  r->compressed_cache_key_prefix_size,
                  handle->offset());
    Slice key(r->compressed_cache_key_prefix, static_cast<size_t>
              (end - r->compressed_cache_key_prefix));

    // Insert into compressed block cache.
    block_cache_compressed->Insert(
        key, block_contents_to_cache,
        block_contents_to_cache->ApproximateMemoryUsage(),
        &DeleteCachedBlockContents);

    // Invalidate OS cache.
    r->file->InvalidateCache(static_cast<size_t>(r->offset), size);
  }
  return Status::OK();
}

void BlockBasedTableBuilder::WriteFilterBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle filter_block_handle;
  bool empty_filter_block = (rep_->filter_builder == nullptr ||
                             rep_->filter_builder->NumAdded() == 0);
  if (ok() && !empty_filter_block) {
    Status s = Status::Incomplete();
    while (ok() && s.IsIncomplete()) {
      Slice filter_content =
          rep_->filter_builder->Finish(filter_block_handle, &s);
      assert(s.ok() || s.IsIncomplete());
      rep_->props.filter_size += filter_content.size();
      WriteRawBlock(filter_content, kNoCompression, &filter_block_handle);
    }
  }
  if (ok() && !empty_filter_block) {
    // Add mapping from "<filter_block_prefix>.Name" to location
    // of filter data.
    std::string key;
    if (rep_->filter_builder->IsBlockBased()) {
      key = BlockBasedTable::kFilterBlockPrefix;
    } else {
      key = rep_->table_options.partition_filters
                ? BlockBasedTable::kPartitionedFilterBlockPrefix
                : BlockBasedTable::kFullFilterBlockPrefix;
    }
    key.append(rep_->table_options.filter_policy->Name());
    meta_index_builder->Add(key, filter_block_handle);
  }
}

void BlockBasedTableBuilder::WriteIndexBlock(
    MetaIndexBuilder* meta_index_builder, BlockHandle* index_block_handle) {
  IndexBuilder::IndexBlocks index_blocks;
  auto index_builder_status = rep_->index_builder->Finish(&index_blocks);
  if (index_builder_status.IsIncomplete()) {
    // We we have more than one index partition then meta_blocks are not
    // supported for the index. Currently meta_blocks are used only by
    // HashIndexBuilder which is not multi-partition.
    assert(index_blocks.meta_blocks.empty());
  } else if (ok() && !index_builder_status.ok()) {
    rep_->status = index_builder_status;
  }
  if (ok()) {
    for (const auto& item : index_blocks.meta_blocks) {
      BlockHandle block_handle;
      WriteBlock(item.second, &block_handle, false /* is_data_block */);
      if (!ok()) {
        break;
      }
      meta_index_builder->Add(item.first, block_handle);
    }
  }
  if (ok()) {
    if (rep_->table_options.enable_index_compression) {
      WriteBlock(index_blocks.index_block_contents, index_block_handle, false);
    } else {
      WriteRawBlock(index_blocks.index_block_contents, kNoCompression,
                    index_block_handle);
    }
  }
  // If there are more index partitions, finish them and write them out
  Status s = index_builder_status;
  while (ok() && s.IsIncomplete()) {
    s = rep_->index_builder->Finish(&index_blocks, *index_block_handle);
    if (!s.ok() && !s.IsIncomplete()) {
      rep_->status = s;
      return;
    }
    if (rep_->table_options.enable_index_compression) {
      WriteBlock(index_blocks.index_block_contents, index_block_handle, false);
    } else {
      WriteRawBlock(index_blocks.index_block_contents, kNoCompression,
                    index_block_handle);
    }
    // The last index_block_handle will be for the partition index block
  }
}

void BlockBasedTableBuilder::WritePropertiesBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle properties_block_handle;
  if (ok()) {
    PropertyBlockBuilder property_block_builder;
    rep_->props.column_family_id = rep_->column_family_id;
    rep_->props.column_family_name = rep_->column_family_name;
    rep_->props.filter_policy_name =
        rep_->table_options.filter_policy != nullptr
            ? rep_->table_options.filter_policy->Name()
            : "";
    rep_->props.index_size =
        rep_->index_builder->IndexSize() + kBlockTrailerSize;
    rep_->props.comparator_name = rep_->ioptions.user_comparator != nullptr
                                      ? rep_->ioptions.user_comparator->Name()
                                      : "nullptr";
    rep_->props.merge_operator_name =
        rep_->ioptions.merge_operator != nullptr
            ? rep_->ioptions.merge_operator->Name()
            : "nullptr";
    rep_->props.compression_name =
        CompressionTypeToString(rep_->compression_type);
    rep_->props.prefix_extractor_name =
        rep_->moptions.prefix_extractor != nullptr
            ? rep_->moptions.prefix_extractor->Name()
            : "nullptr";

    std::string property_collectors_names = "[";
    for (size_t i = 0;
         i < rep_->ioptions.table_properties_collector_factories.size(); ++i) {
      if (i != 0) {
        property_collectors_names += ",";
      }
      property_collectors_names +=
          rep_->ioptions.table_properties_collector_factories[i]->Name();
    }
    property_collectors_names += "]";
    rep_->props.property_collectors_names = property_collectors_names;
    if (rep_->table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      assert(rep_->p_index_builder_ != nullptr);
      rep_->props.index_partitions = rep_->p_index_builder_->NumPartitions();
      rep_->props.top_level_index_size =
          rep_->p_index_builder_->TopLevelIndexSize(rep_->offset);
    }
    rep_->props.index_key_is_user_key =
        !rep_->index_builder->seperator_is_key_plus_seq();
    rep_->props.index_value_is_delta_encoded =
        rep_->use_delta_encoding_for_index_values;
    rep_->props.creation_time = rep_->creation_time;
    rep_->props.oldest_key_time = rep_->oldest_key_time;

    // Add basic properties
    property_block_builder.AddTableProperty(rep_->props);

    // Add use collected properties
    NotifyCollectTableCollectorsOnFinish(rep_->table_properties_collectors,
                                         rep_->ioptions.info_log,
                                         &property_block_builder);

    WriteRawBlock(property_block_builder.Finish(), kNoCompression,
                  &properties_block_handle);
  }
  if (ok()) {
#ifndef NDEBUG
    {
      uint64_t props_block_offset = properties_block_handle.offset();
      uint64_t props_block_size = properties_block_handle.size();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
          &props_block_offset);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
          &props_block_size);
    }
#endif  // !NDEBUG
    meta_index_builder->Add(kPropertiesBlock, properties_block_handle);
  }
}

void BlockBasedTableBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (rep_->compression_dict != nullptr &&
      rep_->compression_dict->GetRawDict().size()) {
    BlockHandle compression_dict_block_handle;
    if (ok()) {
      WriteRawBlock(rep_->compression_dict->GetRawDict(), kNoCompression,
                    &compression_dict_block_handle);
#ifndef NDEBUG
      Slice compression_dict = rep_->compression_dict->GetRawDict();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
          &compression_dict);
#endif  // NDEBUG
    }
    if (ok()) {
      meta_index_builder->Add(kCompressionDictBlock,
                              compression_dict_block_handle);
    }
  }
}

void BlockBasedTableBuilder::WriteRangeDelBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (ok() && !rep_->range_del_block.empty()) {
    BlockHandle range_del_block_handle;
    WriteRawBlock(rep_->range_del_block.Finish(), kNoCompression,
                  &range_del_block_handle);
    meta_index_builder->Add(kRangeDelBlock, range_del_block_handle);
  }
}

void BlockBasedTableBuilder::WriteFooter(BlockHandle& metaindex_block_handle,
                                         BlockHandle& index_block_handle) {
  Rep* r = rep_;
  // No need to write out new footer if we're using default checksum.
  // We're writing legacy magic number because we want old versions of RocksDB
  // be able to read files generated with new release (just in case if
  // somebody wants to roll back after an upgrade)
  // TODO(icanadi) at some point in the future, when we're absolutely sure
  // nobody will roll back to RocksDB 2.x versions, retire the legacy magic
  // number and always write new table files with new magic number
  bool legacy = (r->table_options.format_version == 0);
  // this is guaranteed by BlockBasedTableBuilder's constructor
  assert(r->table_options.checksum == kCRC32c ||
         r->table_options.format_version != 0);
  Footer footer(
      legacy ? kLegacyBlockBasedTableMagicNumber : kBlockBasedTableMagicNumber,
      r->table_options.format_version);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(index_block_handle);
  footer.set_checksum(r->table_options.checksum);
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  assert(r->status.ok());
  r->status = r->file->Append(footer_encoding);
  if (r->status.ok()) {
    r->offset += footer_encoding.size();
  }
}

void BlockBasedTableBuilder::EnterUnbuffered() {
  Rep* r = rep_;
  assert(r->state == Rep::State::kBuffered);
  r->state = Rep::State::kUnbuffered;
  const size_t kSampleBytes = r->compression_opts.zstd_max_train_bytes > 0
                                  ? r->compression_opts.zstd_max_train_bytes
                                  : r->compression_opts.max_dict_bytes;
  Random64 generator{r->creation_time};
  std::string compression_dict_samples;
  std::vector<size_t> compression_dict_sample_lens;
  if (!r->data_block_and_keys_buffers.empty()) {
    while (compression_dict_samples.size() < kSampleBytes) {
      size_t rand_idx =
          generator.Uniform(r->data_block_and_keys_buffers.size());
      size_t copy_len =
          std::min(kSampleBytes - compression_dict_samples.size(),
                   r->data_block_and_keys_buffers[rand_idx].first.size());
      compression_dict_samples.append(
          r->data_block_and_keys_buffers[rand_idx].first, 0, copy_len);
      compression_dict_sample_lens.emplace_back(copy_len);
    }
  }

  // final data block flushed, now we can generate dictionary from the samples.
  // OK if compression_dict_samples is empty, we'll just get empty dictionary.
  std::string dict;
  if (r->compression_opts.zstd_max_train_bytes > 0) {
    dict = ZSTD_TrainDictionary(compression_dict_samples,
                                compression_dict_sample_lens,
                                r->compression_opts.max_dict_bytes);
  } else {
    dict = std::move(compression_dict_samples);
  }
  r->compression_dict.reset(new CompressionDict(dict, r->compression_type,
                                                r->compression_opts.level));
  r->verify_dict.reset(new UncompressionDict(
      dict, r->compression_type == kZSTD ||
                r->compression_type == kZSTDNotFinalCompression));

  for (size_t i = 0; ok() && i < r->data_block_and_keys_buffers.size(); ++i) {
    const auto& data_block = r->data_block_and_keys_buffers[i].first;
    auto& keys = r->data_block_and_keys_buffers[i].second;
    assert(!data_block.empty());
    assert(!keys.empty());

    for (const auto& key : keys) {
      if (r->filter_builder != nullptr) {
        r->filter_builder->Add(ExtractUserKey(key));
      }
      r->index_builder->OnKeyAdded(key);
    }
    WriteBlock(Slice(data_block), &r->pending_handle, true /* is_data_block */);
    if (ok() && i + 1 < r->data_block_and_keys_buffers.size()) {
      Slice first_key_in_next_block =
          r->data_block_and_keys_buffers[i + 1].second.front();
      Slice* first_key_in_next_block_ptr = &first_key_in_next_block;
      r->index_builder->AddIndexEntry(&keys.back(), first_key_in_next_block_ptr,
                                      r->pending_handle);
    }
  }
  r->data_block_and_keys_buffers.clear();
}

Status BlockBasedTableBuilder::Finish() {
  Rep* r = rep_;
  assert(r->state != Rep::State::kClosed);
  bool empty_data_block = r->data_block.empty();
  Flush();
  if (r->state == Rep::State::kBuffered) {
    EnterUnbuffered();
  }
  // To make sure properties block is able to keep the accurate size of index
  // block, we will finish writing all index entries first.
  if (ok() && !empty_data_block) {
    r->index_builder->AddIndexEntry(
        &r->last_key, nullptr /* no next data block */, r->pending_handle);
  }

  // Write meta blocks, metaindex block and footer in the following order.
  //    1. [meta block: filter]
  //    2. [meta block: index]
  //    3. [meta block: compression dictionary]
  //    4. [meta block: range deletion tombstone]
  //    5. [meta block: properties]
  //    6. [metaindex block]
  //    7. Footer
  BlockHandle metaindex_block_handle, index_block_handle;
  MetaIndexBuilder meta_index_builder;
  WriteFilterBlock(&meta_index_builder);
  WriteIndexBlock(&meta_index_builder, &index_block_handle);
  WriteCompressionDictBlock(&meta_index_builder);
  WriteRangeDelBlock(&meta_index_builder);
  WritePropertiesBlock(&meta_index_builder);
  if (ok()) {
    // flush the meta index block
    WriteRawBlock(meta_index_builder.Finish(), kNoCompression,
                  &metaindex_block_handle);
  }
  if (ok()) {
    WriteFooter(metaindex_block_handle, index_block_handle);
  }
  r->state = Rep::State::kClosed;
  return r->status;
}

void BlockBasedTableBuilder::Abandon() {
  assert(rep_->state != Rep::State::kClosed);
  rep_->state = Rep::State::kClosed;
}

uint64_t BlockBasedTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

uint64_t BlockBasedTableBuilder::FileSize() const { return rep_->offset; }

bool BlockBasedTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties BlockBasedTableBuilder::GetTableProperties() const {
  TableProperties ret = rep_->props;
  for (const auto& collector : rep_->table_properties_collectors) {
    for (const auto& prop : collector->GetReadableProperties()) {
      ret.readable_properties.insert(prop);
    }
    collector->Finish(&ret.user_collected_properties);
  }
  return ret;
}

const std::string BlockBasedTable::kFilterBlockPrefix = "filter.";
const std::string BlockBasedTable::kFullFilterBlockPrefix = "fullfilter.";
const std::string BlockBasedTable::kPartitionedFilterBlockPrefix =
    "partitionedfilter.";
}  // namespace rocksdb
