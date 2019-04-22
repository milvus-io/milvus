// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "table/plain_table_reader.h"

#include <string>
#include <vector>

#include "db/dbformat.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"

#include "table/block.h"
#include "table/bloom_block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"
#include "table/plain_table_factory.h"
#include "table/plain_table_key_coding.h"
#include "table/get_context.h"

#include "monitoring/histogram.h"
#include "monitoring/perf_context_imp.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/murmurhash.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {

// Safely getting a uint32_t element from a char array, where, starting from
// `base`, every 4 bytes are considered as an fixed 32 bit integer.
inline uint32_t GetFixed32Element(const char* base, size_t offset) {
  return DecodeFixed32(base + offset * sizeof(uint32_t));
}
}  // namespace

// Iterator to iterate IndexedTable
class PlainTableIterator : public InternalIterator {
 public:
  explicit PlainTableIterator(PlainTableReader* table, bool use_prefix_seek);
  ~PlainTableIterator() override;

  bool Valid() const override;

  void SeekToFirst() override;

  void SeekToLast() override;

  void Seek(const Slice& target) override;

  void SeekForPrev(const Slice& target) override;

  void Next() override;

  void Prev() override;

  Slice key() const override;

  Slice value() const override;

  Status status() const override;

 private:
  PlainTableReader* table_;
  PlainTableKeyDecoder decoder_;
  bool use_prefix_seek_;
  uint32_t offset_;
  uint32_t next_offset_;
  Slice key_;
  Slice value_;
  Status status_;
  // No copying allowed
  PlainTableIterator(const PlainTableIterator&) = delete;
  void operator=(const Iterator&) = delete;
};

extern const uint64_t kPlainTableMagicNumber;
PlainTableReader::PlainTableReader(
    const ImmutableCFOptions& ioptions,
    std::unique_ptr<RandomAccessFileReader>&& file,
    const EnvOptions& storage_options, const InternalKeyComparator& icomparator,
    EncodingType encoding_type, uint64_t file_size,
    const TableProperties* table_properties,
    const SliceTransform* prefix_extractor)
    : internal_comparator_(icomparator),
      encoding_type_(encoding_type),
      full_scan_mode_(false),
      user_key_len_(static_cast<uint32_t>(table_properties->fixed_key_len)),
      prefix_extractor_(prefix_extractor),
      enable_bloom_(false),
      bloom_(6),
      file_info_(std::move(file), storage_options,
                 static_cast<uint32_t>(table_properties->data_size)),
      ioptions_(ioptions),
      file_size_(file_size),
      table_properties_(nullptr) {}

PlainTableReader::~PlainTableReader() {
}

Status PlainTableReader::Open(
    const ImmutableCFOptions& ioptions, const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader, const int bloom_bits_per_key,
    double hash_table_ratio, size_t index_sparseness, size_t huge_page_tlb_size,
    bool full_scan_mode, const bool immortal_table,
    const SliceTransform* prefix_extractor) {
  if (file_size > PlainTableIndex::kMaxFileSize) {
    return Status::NotSupported("File is too large for PlainTableReader!");
  }

  TableProperties* props = nullptr;
  auto s = ReadTableProperties(file.get(), file_size, kPlainTableMagicNumber,
                               ioptions, &props,
                               true /* compression_type_missing */);
  if (!s.ok()) {
    return s;
  }

  assert(hash_table_ratio >= 0.0);
  auto& user_props = props->user_collected_properties;
  auto prefix_extractor_in_file = props->prefix_extractor_name;

  if (!full_scan_mode &&
      !prefix_extractor_in_file.empty() /* old version sst file*/
      && prefix_extractor_in_file != "nullptr") {
    if (!prefix_extractor) {
      return Status::InvalidArgument(
          "Prefix extractor is missing when opening a PlainTable built "
          "using a prefix extractor");
    } else if (prefix_extractor_in_file.compare(prefix_extractor->Name()) !=
               0) {
      return Status::InvalidArgument(
          "Prefix extractor given doesn't match the one used to build "
          "PlainTable");
    }
  }

  EncodingType encoding_type = kPlain;
  auto encoding_type_prop =
      user_props.find(PlainTablePropertyNames::kEncodingType);
  if (encoding_type_prop != user_props.end()) {
    encoding_type = static_cast<EncodingType>(
        DecodeFixed32(encoding_type_prop->second.c_str()));
  }

  std::unique_ptr<PlainTableReader> new_reader(new PlainTableReader(
      ioptions, std::move(file), env_options, internal_comparator,
      encoding_type, file_size, props, prefix_extractor));

  s = new_reader->MmapDataIfNeeded();
  if (!s.ok()) {
    return s;
  }

  if (!full_scan_mode) {
    s = new_reader->PopulateIndex(props, bloom_bits_per_key, hash_table_ratio,
                                  index_sparseness, huge_page_tlb_size);
    if (!s.ok()) {
      return s;
    }
  } else {
    // Flag to indicate it is a full scan mode so that none of the indexes
    // can be used.
    new_reader->full_scan_mode_ = true;
  }

  if (immortal_table && new_reader->file_info_.is_mmap_mode) {
    new_reader->dummy_cleanable_.reset(new Cleanable());
  }

  *table_reader = std::move(new_reader);
  return s;
}

void PlainTableReader::SetupForCompaction() {
}

InternalIterator* PlainTableReader::NewIterator(
    const ReadOptions& options, const SliceTransform* /* prefix_extractor */,
    Arena* arena, bool /*skip_filters*/, bool /*for_compaction*/) {
  bool use_prefix_seek = !IsTotalOrderMode() && !options.total_order_seek;
  if (arena == nullptr) {
    return new PlainTableIterator(this, use_prefix_seek);
  } else {
    auto mem = arena->AllocateAligned(sizeof(PlainTableIterator));
    return new (mem) PlainTableIterator(this, use_prefix_seek);
  }
}

Status PlainTableReader::PopulateIndexRecordList(
    PlainTableIndexBuilder* index_builder, vector<uint32_t>* prefix_hashes) {
  Slice prev_key_prefix_slice;
  std::string prev_key_prefix_buf;
  uint32_t pos = data_start_offset_;

  bool is_first_record = true;
  Slice key_prefix_slice;
  PlainTableKeyDecoder decoder(&file_info_, encoding_type_, user_key_len_,
                               prefix_extractor_);
  while (pos < file_info_.data_end_offset) {
    uint32_t key_offset = pos;
    ParsedInternalKey key;
    Slice value_slice;
    bool seekable = false;
    Status s = Next(&decoder, &pos, &key, nullptr, &value_slice, &seekable);
    if (!s.ok()) {
      return s;
    }

    key_prefix_slice = GetPrefix(key);
    if (enable_bloom_) {
      bloom_.AddHash(GetSliceHash(key.user_key));
    } else {
      if (is_first_record || prev_key_prefix_slice != key_prefix_slice) {
        if (!is_first_record) {
          prefix_hashes->push_back(GetSliceHash(prev_key_prefix_slice));
        }
        if (file_info_.is_mmap_mode) {
          prev_key_prefix_slice = key_prefix_slice;
        } else {
          prev_key_prefix_buf = key_prefix_slice.ToString();
          prev_key_prefix_slice = prev_key_prefix_buf;
        }
      }
    }

    index_builder->AddKeyPrefix(GetPrefix(key), key_offset);

    if (!seekable && is_first_record) {
      return Status::Corruption("Key for a prefix is not seekable");
    }

    is_first_record = false;
  }

  prefix_hashes->push_back(GetSliceHash(key_prefix_slice));
  auto s = index_.InitFromRawData(index_builder->Finish());
  return s;
}

void PlainTableReader::AllocateAndFillBloom(int bloom_bits_per_key,
                                            int num_prefixes,
                                            size_t huge_page_tlb_size,
                                            vector<uint32_t>* prefix_hashes) {
  if (!IsTotalOrderMode()) {
    uint32_t bloom_total_bits = num_prefixes * bloom_bits_per_key;
    if (bloom_total_bits > 0) {
      enable_bloom_ = true;
      bloom_.SetTotalBits(&arena_, bloom_total_bits, ioptions_.bloom_locality,
                          huge_page_tlb_size, ioptions_.info_log);
      FillBloom(prefix_hashes);
    }
  }
}

void PlainTableReader::FillBloom(vector<uint32_t>* prefix_hashes) {
  assert(bloom_.IsInitialized());
  for (auto prefix_hash : *prefix_hashes) {
    bloom_.AddHash(prefix_hash);
  }
}

Status PlainTableReader::MmapDataIfNeeded() {
  if (file_info_.is_mmap_mode) {
    // Get mmapped memory.
    return file_info_.file->Read(0, static_cast<size_t>(file_size_), &file_info_.file_data, nullptr);
  }
  return Status::OK();
}

Status PlainTableReader::PopulateIndex(TableProperties* props,
                                       int bloom_bits_per_key,
                                       double hash_table_ratio,
                                       size_t index_sparseness,
                                       size_t huge_page_tlb_size) {
  assert(props != nullptr);
  table_properties_.reset(props);

  BlockContents index_block_contents;
  Status s = ReadMetaBlock(file_info_.file.get(), nullptr /* prefetch_buffer */,
                           file_size_, kPlainTableMagicNumber, ioptions_,
                           PlainTableIndexBuilder::kPlainTableIndexBlock,
                           &index_block_contents,
                           true /* compression_type_missing */);

  bool index_in_file = s.ok();

  BlockContents bloom_block_contents;
  bool bloom_in_file = false;
  // We only need to read the bloom block if index block is in file.
  if (index_in_file) {
    s = ReadMetaBlock(file_info_.file.get(), nullptr /* prefetch_buffer */,
                      file_size_, kPlainTableMagicNumber, ioptions_,
                      BloomBlockBuilder::kBloomBlock, &bloom_block_contents,
                      true /* compression_type_missing */);
    bloom_in_file = s.ok() && bloom_block_contents.data.size() > 0;
  }

  Slice* bloom_block;
  if (bloom_in_file) {
    // If bloom_block_contents.allocation is not empty (which will be the case
    // for non-mmap mode), it holds the alloated memory for the bloom block.
    // It needs to be kept alive to keep `bloom_block` valid.
    bloom_block_alloc_ = std::move(bloom_block_contents.allocation);
    bloom_block = &bloom_block_contents.data;
  } else {
    bloom_block = nullptr;
  }

  Slice* index_block;
  if (index_in_file) {
    // If index_block_contents.allocation is not empty (which will be the case
    // for non-mmap mode), it holds the alloated memory for the index block.
    // It needs to be kept alive to keep `index_block` valid.
    index_block_alloc_ = std::move(index_block_contents.allocation);
    index_block = &index_block_contents.data;
  } else {
    index_block = nullptr;
  }

  if ((prefix_extractor_ == nullptr) && (hash_table_ratio != 0)) {
    // moptions.prefix_extractor is requried for a hash-based look-up.
    return Status::NotSupported(
        "PlainTable requires a prefix extractor enable prefix hash mode.");
  }

  // First, read the whole file, for every kIndexIntervalForSamePrefixKeys rows
  // for a prefix (starting from the first one), generate a record of (hash,
  // offset) and append it to IndexRecordList, which is a data structure created
  // to store them.

  if (!index_in_file) {
    // Allocate bloom filter here for total order mode.
    if (IsTotalOrderMode()) {
      uint32_t num_bloom_bits =
          static_cast<uint32_t>(table_properties_->num_entries) *
          bloom_bits_per_key;
      if (num_bloom_bits > 0) {
        enable_bloom_ = true;
        bloom_.SetTotalBits(&arena_, num_bloom_bits, ioptions_.bloom_locality,
                            huge_page_tlb_size, ioptions_.info_log);
      }
    }
  } else if (bloom_in_file) {
    enable_bloom_ = true;
    auto num_blocks_property = props->user_collected_properties.find(
        PlainTablePropertyNames::kNumBloomBlocks);

    uint32_t num_blocks = 0;
    if (num_blocks_property != props->user_collected_properties.end()) {
      Slice temp_slice(num_blocks_property->second);
      if (!GetVarint32(&temp_slice, &num_blocks)) {
        num_blocks = 0;
      }
    }
    // cast away const qualifier, because bloom_ won't be changed
    bloom_.SetRawData(
        const_cast<unsigned char*>(
            reinterpret_cast<const unsigned char*>(bloom_block->data())),
        static_cast<uint32_t>(bloom_block->size()) * 8, num_blocks);
  } else {
    // Index in file but no bloom in file. Disable bloom filter in this case.
    enable_bloom_ = false;
    bloom_bits_per_key = 0;
  }

  PlainTableIndexBuilder index_builder(&arena_, ioptions_, prefix_extractor_,
                                       index_sparseness, hash_table_ratio,
                                       huge_page_tlb_size);

  std::vector<uint32_t> prefix_hashes;
  if (!index_in_file) {
    s = PopulateIndexRecordList(&index_builder, &prefix_hashes);
    if (!s.ok()) {
      return s;
    }
  } else {
    s = index_.InitFromRawData(*index_block);
    if (!s.ok()) {
      return s;
    }
  }

  if (!index_in_file) {
    // Calculated bloom filter size and allocate memory for
    // bloom filter based on the number of prefixes, then fill it.
    AllocateAndFillBloom(bloom_bits_per_key, index_.GetNumPrefixes(),
                         huge_page_tlb_size, &prefix_hashes);
  }

  // Fill two table properties.
  if (!index_in_file) {
    props->user_collected_properties["plain_table_hash_table_size"] =
        ToString(index_.GetIndexSize() * PlainTableIndex::kOffsetLen);
    props->user_collected_properties["plain_table_sub_index_size"] =
        ToString(index_.GetSubIndexSize());
  } else {
    props->user_collected_properties["plain_table_hash_table_size"] =
        ToString(0);
    props->user_collected_properties["plain_table_sub_index_size"] =
        ToString(0);
  }

  return Status::OK();
}

Status PlainTableReader::GetOffset(PlainTableKeyDecoder* decoder,
                                   const Slice& target, const Slice& prefix,
                                   uint32_t prefix_hash, bool& prefix_matched,
                                   uint32_t* offset) const {
  prefix_matched = false;
  uint32_t prefix_index_offset;
  auto res = index_.GetOffset(prefix_hash, &prefix_index_offset);
  if (res == PlainTableIndex::kNoPrefixForBucket) {
    *offset = file_info_.data_end_offset;
    return Status::OK();
  } else if (res == PlainTableIndex::kDirectToFile) {
    *offset = prefix_index_offset;
    return Status::OK();
  }

  // point to sub-index, need to do a binary search
  uint32_t upper_bound;
  const char* base_ptr =
      index_.GetSubIndexBasePtrAndUpperBound(prefix_index_offset, &upper_bound);
  uint32_t low = 0;
  uint32_t high = upper_bound;
  ParsedInternalKey mid_key;
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    return Status::Corruption(Slice());
  }

  // The key is between [low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    uint32_t file_offset = GetFixed32Element(base_ptr, mid);
    uint32_t tmp;
    Status s = decoder->NextKeyNoValue(file_offset, &mid_key, nullptr, &tmp);
    if (!s.ok()) {
      return s;
    }
    int cmp_result = internal_comparator_.Compare(mid_key, parsed_target);
    if (cmp_result < 0) {
      low = mid;
    } else {
      if (cmp_result == 0) {
        // Happen to have found the exact key or target is smaller than the
        // first key after base_offset.
        prefix_matched = true;
        *offset = file_offset;
        return Status::OK();
      } else {
        high = mid;
      }
    }
  }
  // Both of the key at the position low or low+1 could share the same
  // prefix as target. We need to rule out one of them to avoid to go
  // to the wrong prefix.
  ParsedInternalKey low_key;
  uint32_t tmp;
  uint32_t low_key_offset = GetFixed32Element(base_ptr, low);
  Status s = decoder->NextKeyNoValue(low_key_offset, &low_key, nullptr, &tmp);
  if (!s.ok()) {
    return s;
  }

  if (GetPrefix(low_key) == prefix) {
    prefix_matched = true;
    *offset = low_key_offset;
  } else if (low + 1 < upper_bound) {
    // There is possible a next prefix, return it
    prefix_matched = false;
    *offset = GetFixed32Element(base_ptr, low + 1);
  } else {
    // target is larger than a key of the last prefix in this bucket
    // but with a different prefix. Key does not exist.
    *offset = file_info_.data_end_offset;
  }
  return Status::OK();
}

bool PlainTableReader::MatchBloom(uint32_t hash) const {
  if (!enable_bloom_) {
    return true;
  }

  if (bloom_.MayContainHash(hash)) {
    PERF_COUNTER_ADD(bloom_sst_hit_count, 1);
    return true;
  } else {
    PERF_COUNTER_ADD(bloom_sst_miss_count, 1);
    return false;
  }
}

Status PlainTableReader::Next(PlainTableKeyDecoder* decoder, uint32_t* offset,
                              ParsedInternalKey* parsed_key,
                              Slice* internal_key, Slice* value,
                              bool* seekable) const {
  if (*offset == file_info_.data_end_offset) {
    *offset = file_info_.data_end_offset;
    return Status::OK();
  }

  if (*offset > file_info_.data_end_offset) {
    return Status::Corruption("Offset is out of file size");
  }

  uint32_t bytes_read;
  Status s = decoder->NextKey(*offset, parsed_key, internal_key, value,
                              &bytes_read, seekable);
  if (!s.ok()) {
    return s;
  }
  *offset = *offset + bytes_read;
  return Status::OK();
}

void PlainTableReader::Prepare(const Slice& target) {
  if (enable_bloom_) {
    uint32_t prefix_hash = GetSliceHash(GetPrefix(target));
    bloom_.Prefetch(prefix_hash);
  }
}

Status PlainTableReader::Get(const ReadOptions& /*ro*/, const Slice& target,
                             GetContext* get_context,
                             const SliceTransform* /* prefix_extractor */,
                             bool /*skip_filters*/) {
  // Check bloom filter first.
  Slice prefix_slice;
  uint32_t prefix_hash;
  if (IsTotalOrderMode()) {
    if (full_scan_mode_) {
      status_ =
          Status::InvalidArgument("Get() is not allowed in full scan mode.");
    }
    // Match whole user key for bloom filter check.
    if (!MatchBloom(GetSliceHash(GetUserKey(target)))) {
      return Status::OK();
    }
    // in total order mode, there is only one bucket 0, and we always use empty
    // prefix.
    prefix_slice = Slice();
    prefix_hash = 0;
  } else {
    prefix_slice = GetPrefix(target);
    prefix_hash = GetSliceHash(prefix_slice);
    if (!MatchBloom(prefix_hash)) {
      return Status::OK();
    }
  }
  uint32_t offset;
  bool prefix_match;
  PlainTableKeyDecoder decoder(&file_info_, encoding_type_, user_key_len_,
                               prefix_extractor_);
  Status s = GetOffset(&decoder, target, prefix_slice, prefix_hash,
                       prefix_match, &offset);

  if (!s.ok()) {
    return s;
  }
  ParsedInternalKey found_key;
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    return Status::Corruption(Slice());
  }
  Slice found_value;
  while (offset < file_info_.data_end_offset) {
    s = Next(&decoder, &offset, &found_key, nullptr, &found_value);
    if (!s.ok()) {
      return s;
    }
    if (!prefix_match) {
      // Need to verify prefix for the first key found if it is not yet
      // checked.
      if (GetPrefix(found_key) != prefix_slice) {
        return Status::OK();
      }
      prefix_match = true;
    }
    // TODO(ljin): since we know the key comparison result here,
    // can we enable the fast path?
    if (internal_comparator_.Compare(found_key, parsed_target) >= 0) {
      bool dont_care __attribute__((__unused__));
      if (!get_context->SaveValue(found_key, found_value, &dont_care,
                                  dummy_cleanable_.get())) {
        break;
      }
    }
  }
  return Status::OK();
}

uint64_t PlainTableReader::ApproximateOffsetOf(const Slice& /*key*/) {
  return 0;
}

PlainTableIterator::PlainTableIterator(PlainTableReader* table,
                                       bool use_prefix_seek)
    : table_(table),
      decoder_(&table_->file_info_, table_->encoding_type_,
               table_->user_key_len_, table_->prefix_extractor_),
      use_prefix_seek_(use_prefix_seek) {
  next_offset_ = offset_ = table_->file_info_.data_end_offset;
}

PlainTableIterator::~PlainTableIterator() {
}

bool PlainTableIterator::Valid() const {
  return offset_ < table_->file_info_.data_end_offset &&
         offset_ >= table_->data_start_offset_;
}

void PlainTableIterator::SeekToFirst() {
  status_ = Status::OK();
  next_offset_ = table_->data_start_offset_;
  if (next_offset_ >= table_->file_info_.data_end_offset) {
    next_offset_ = offset_ = table_->file_info_.data_end_offset;
  } else {
    Next();
  }
}

void PlainTableIterator::SeekToLast() {
  assert(false);
  status_ = Status::NotSupported("SeekToLast() is not supported in PlainTable");
  next_offset_ = offset_ = table_->file_info_.data_end_offset;
}

void PlainTableIterator::Seek(const Slice& target) {
  if (use_prefix_seek_ != !table_->IsTotalOrderMode()) {
    // This check is done here instead of NewIterator() to permit creating an
    // iterator with total_order_seek = true even if we won't be able to Seek()
    // it. This is needed for compaction: it creates iterator with
    // total_order_seek = true but usually never does Seek() on it,
    // only SeekToFirst().
    status_ =
        Status::InvalidArgument(
          "total_order_seek not implemented for PlainTable.");
    offset_ = next_offset_ = table_->file_info_.data_end_offset;
    return;
  }

  // If the user doesn't set prefix seek option and we are not able to do a
  // total Seek(). assert failure.
  if (table_->IsTotalOrderMode()) {
    if (table_->full_scan_mode_) {
      status_ =
          Status::InvalidArgument("Seek() is not allowed in full scan mode.");
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
      return;
    } else if (table_->GetIndexSize() > 1) {
      assert(false);
      status_ = Status::NotSupported(
          "PlainTable cannot issue non-prefix seek unless in total order "
          "mode.");
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
      return;
    }
  }

  Slice prefix_slice = table_->GetPrefix(target);
  uint32_t prefix_hash = 0;
  // Bloom filter is ignored in total-order mode.
  if (!table_->IsTotalOrderMode()) {
    prefix_hash = GetSliceHash(prefix_slice);
    if (!table_->MatchBloom(prefix_hash)) {
      status_ = Status::OK();
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
      return;
    }
  }
  bool prefix_match;
  status_ = table_->GetOffset(&decoder_, target, prefix_slice, prefix_hash,
                              prefix_match, &next_offset_);
  if (!status_.ok()) {
    offset_ = next_offset_ = table_->file_info_.data_end_offset;
    return;
  }

  if (next_offset_ < table_->file_info_.data_end_offset) {
    for (Next(); status_.ok() && Valid(); Next()) {
      if (!prefix_match) {
        // Need to verify the first key's prefix
        if (table_->GetPrefix(key()) != prefix_slice) {
          offset_ = next_offset_ = table_->file_info_.data_end_offset;
          break;
        }
        prefix_match = true;
      }
      if (table_->internal_comparator_.Compare(key(), target) >= 0) {
        break;
      }
    }
  } else {
    offset_ = table_->file_info_.data_end_offset;
  }
}

void PlainTableIterator::SeekForPrev(const Slice& /*target*/) {
  assert(false);
  status_ =
      Status::NotSupported("SeekForPrev() is not supported in PlainTable");
  offset_ = next_offset_ = table_->file_info_.data_end_offset;
}

void PlainTableIterator::Next() {
  offset_ = next_offset_;
  if (offset_ < table_->file_info_.data_end_offset) {
    Slice tmp_slice;
    ParsedInternalKey parsed_key;
    status_ =
        table_->Next(&decoder_, &next_offset_, &parsed_key, &key_, &value_);
    if (!status_.ok()) {
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
    }
  }
}

void PlainTableIterator::Prev() {
  assert(false);
}

Slice PlainTableIterator::key() const {
  assert(Valid());
  return key_;
}

Slice PlainTableIterator::value() const {
  assert(Valid());
  return value_;
}

Status PlainTableIterator::status() const {
  return status_;
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
