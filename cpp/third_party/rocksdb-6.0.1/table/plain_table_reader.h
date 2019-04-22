// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>
#include <stdint.h>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/table_reader.h"
#include "table/plain_table_factory.h"
#include "table/plain_table_index.h"
#include "util/arena.h"
#include "util/dynamic_bloom.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

class Block;
struct BlockContents;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class TableReader;
class InternalKeyComparator;
class PlainTableKeyDecoder;
class GetContext;

using std::unique_ptr;
using std::unordered_map;
using std::vector;
extern const uint32_t kPlainTableVariableLength;

struct PlainTableReaderFileInfo {
  bool is_mmap_mode;
  Slice file_data;
  uint32_t data_end_offset;
  std::unique_ptr<RandomAccessFileReader> file;

  PlainTableReaderFileInfo(unique_ptr<RandomAccessFileReader>&& _file,
                           const EnvOptions& storage_options,
                           uint32_t _data_size_offset)
      : is_mmap_mode(storage_options.use_mmap_reads),
        data_end_offset(_data_size_offset),
        file(std::move(_file)) {}
};

// Based on following output file format shown in plain_table_factory.h
// When opening the output file, IndexedTableReader creates a hash table
// from key prefixes to offset of the output file. IndexedTable will decide
// whether it points to the data offset of the first key with the key prefix
// or the offset of it. If there are too many keys share this prefix, it will
// create a binary search-able index from the suffix to offset on disk.
//
// The implementation of IndexedTableReader requires output file is mmaped
class PlainTableReader: public TableReader {
 public:
  static Status Open(const ImmutableCFOptions& ioptions,
                     const EnvOptions& env_options,
                     const InternalKeyComparator& internal_comparator,
                     std::unique_ptr<RandomAccessFileReader>&& file,
                     uint64_t file_size, std::unique_ptr<TableReader>* table,
                     const int bloom_bits_per_key, double hash_table_ratio,
                     size_t index_sparseness, size_t huge_page_tlb_size,
                     bool full_scan_mode, const bool immortal_table = false,
                     const SliceTransform* prefix_extractor = nullptr);

  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena = nullptr,
                                bool skip_filters = false,
                                bool for_compaction = false) override;

  void Prepare(const Slice& target) override;

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const Slice& key) override;

  uint32_t GetIndexSize() const { return index_.GetIndexSize(); }
  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }

  virtual size_t ApproximateMemoryUsage() const override {
    return arena_.MemoryAllocatedBytes();
  }

  PlainTableReader(const ImmutableCFOptions& ioptions,
                   std::unique_ptr<RandomAccessFileReader>&& file,
                   const EnvOptions& env_options,
                   const InternalKeyComparator& internal_comparator,
                   EncodingType encoding_type, uint64_t file_size,
                   const TableProperties* table_properties,
                   const SliceTransform* prefix_extractor);
  virtual ~PlainTableReader();

 protected:
  // Check bloom filter to see whether it might contain this prefix.
  // The hash of the prefix is given, since it can be reused for index lookup
  // too.
  virtual bool MatchBloom(uint32_t hash) const;

  // PopulateIndex() builds index of keys. It must be called before any query
  // to the table.
  //
  // props: the table properties object that need to be stored. Ownership of
  //        the object will be passed.
  //

  Status PopulateIndex(TableProperties* props, int bloom_bits_per_key,
                       double hash_table_ratio, size_t index_sparseness,
                       size_t huge_page_tlb_size);

  Status MmapDataIfNeeded();

 private:
  const InternalKeyComparator internal_comparator_;
  EncodingType encoding_type_;
  // represents plain table's current status.
  Status status_;

  PlainTableIndex index_;
  bool full_scan_mode_;

  // data_start_offset_ and data_end_offset_ defines the range of the
  // sst file that stores data.
  const uint32_t data_start_offset_ = 0;
  const uint32_t user_key_len_;
  const SliceTransform* prefix_extractor_;

  static const size_t kNumInternalBytes = 8;

  // Bloom filter is used to rule out non-existent key
  bool enable_bloom_;
  DynamicBloom bloom_;
  PlainTableReaderFileInfo file_info_;
  Arena arena_;
  CacheAllocationPtr index_block_alloc_;
  CacheAllocationPtr bloom_block_alloc_;

  const ImmutableCFOptions& ioptions_;
  std::unique_ptr<Cleanable> dummy_cleanable_;
  uint64_t file_size_;
  std::shared_ptr<const TableProperties> table_properties_;

  bool IsFixedLength() const {
    return user_key_len_ != kPlainTableVariableLength;
  }

  size_t GetFixedInternalKeyLength() const {
    return user_key_len_ + kNumInternalBytes;
  }

  Slice GetPrefix(const Slice& target) const {
    assert(target.size() >= 8);  // target is internal key
    return GetPrefixFromUserKey(GetUserKey(target));
  }

  Slice GetPrefix(const ParsedInternalKey& target) const {
    return GetPrefixFromUserKey(target.user_key);
  }

  Slice GetUserKey(const Slice& key) const {
    return Slice(key.data(), key.size() - 8);
  }

  Slice GetPrefixFromUserKey(const Slice& user_key) const {
    if (!IsTotalOrderMode()) {
      return prefix_extractor_->Transform(user_key);
    } else {
      // Use empty slice as prefix if prefix_extractor is not set.
      // In that case,
      // it falls back to pure binary search and
      // total iterator seek is supported.
      return Slice();
    }
  }

  friend class TableCache;
  friend class PlainTableIterator;

  // Internal helper function to generate an IndexRecordList object from all
  // the rows, which contains index records as a list.
  // If bloom_ is not null, all the keys' full-key hash will be added to the
  // bloom filter.
  Status PopulateIndexRecordList(PlainTableIndexBuilder* index_builder,
                                 vector<uint32_t>* prefix_hashes);

  // Internal helper function to allocate memory for bloom filter and fill it
  void AllocateAndFillBloom(int bloom_bits_per_key, int num_prefixes,
                            size_t huge_page_tlb_size,
                            vector<uint32_t>* prefix_hashes);

  void FillBloom(vector<uint32_t>* prefix_hashes);

  // Read the key and value at `offset` to parameters for keys, the and
  // `seekable`.
  // On success, `offset` will be updated as the offset for the next key.
  // `parsed_key` will be key in parsed format.
  // if `internal_key` is not empty, it will be filled with key with slice
  // format.
  // if `seekable` is not null, it will return whether we can directly read
  // data using this offset.
  Status Next(PlainTableKeyDecoder* decoder, uint32_t* offset,
              ParsedInternalKey* parsed_key, Slice* internal_key, Slice* value,
              bool* seekable = nullptr) const;
  // Get file offset for key target.
  // return value prefix_matched is set to true if the offset is confirmed
  // for a key with the same prefix as target.
  Status GetOffset(PlainTableKeyDecoder* decoder, const Slice& target,
                   const Slice& prefix, uint32_t prefix_hash,
                   bool& prefix_matched, uint32_t* offset) const;

  bool IsTotalOrderMode() const { return (prefix_extractor_ == nullptr); }

  // No copying allowed
  explicit PlainTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
