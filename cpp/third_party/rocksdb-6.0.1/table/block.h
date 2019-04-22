//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "format.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "table/block_prefix_index.h"
#include "table/data_block_hash_index.h"
#include "table/internal_iterator.h"
#include "util/random.h"
#include "util/sync_point.h"

namespace rocksdb {

struct BlockContents;
class Comparator;
template <class TValue>
class BlockIter;
class DataBlockIter;
class IndexBlockIter;
class BlockPrefixIndex;

// BlockReadAmpBitmap is a bitmap that map the rocksdb::Block data bytes to
// a bitmap with ratio bytes_per_bit. Whenever we access a range of bytes in
// the Block we update the bitmap and increment READ_AMP_ESTIMATE_USEFUL_BYTES.
class BlockReadAmpBitmap {
 public:
  explicit BlockReadAmpBitmap(size_t block_size, size_t bytes_per_bit,
                              Statistics* statistics)
      : bitmap_(nullptr),
        bytes_per_bit_pow_(0),
        statistics_(statistics),
        rnd_(
            Random::GetTLSInstance()->Uniform(static_cast<int>(bytes_per_bit))) {
    TEST_SYNC_POINT_CALLBACK("BlockReadAmpBitmap:rnd", &rnd_);
    assert(block_size > 0 && bytes_per_bit > 0);

    // convert bytes_per_bit to be a power of 2
    while (bytes_per_bit >>= 1) {
      bytes_per_bit_pow_++;
    }

    // num_bits_needed = ceil(block_size / bytes_per_bit)
    size_t num_bits_needed =
      ((block_size - 1) >> bytes_per_bit_pow_) + 1;
    assert(num_bits_needed > 0);

    // bitmap_size = ceil(num_bits_needed / kBitsPerEntry)
    size_t bitmap_size = (num_bits_needed - 1) / kBitsPerEntry + 1;

    // Create bitmap and set all the bits to 0
    bitmap_ = new std::atomic<uint32_t>[bitmap_size]();

    RecordTick(GetStatistics(), READ_AMP_TOTAL_READ_BYTES, block_size);
  }

  ~BlockReadAmpBitmap() { delete[] bitmap_; }

  void Mark(uint32_t start_offset, uint32_t end_offset) {
    assert(end_offset >= start_offset);
    // Index of first bit in mask
    uint32_t start_bit =
        (start_offset + (1 << bytes_per_bit_pow_) - rnd_ - 1) >>
        bytes_per_bit_pow_;
    // Index of last bit in mask + 1
    uint32_t exclusive_end_bit =
        (end_offset + (1 << bytes_per_bit_pow_) - rnd_) >> bytes_per_bit_pow_;
    if (start_bit >= exclusive_end_bit) {
      return;
    }
    assert(exclusive_end_bit > 0);

    if (GetAndSet(start_bit) == 0) {
      uint32_t new_useful_bytes = (exclusive_end_bit - start_bit)
                                  << bytes_per_bit_pow_;
      RecordTick(GetStatistics(), READ_AMP_ESTIMATE_USEFUL_BYTES,
                 new_useful_bytes);
    }
  }

  Statistics* GetStatistics() {
    return statistics_.load(std::memory_order_relaxed);
  }

  void SetStatistics(Statistics* stats) { statistics_.store(stats); }

  uint32_t GetBytesPerBit() { return 1 << bytes_per_bit_pow_; }

  size_t ApproximateMemoryUsage() const {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    return malloc_usable_size((void*)this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return sizeof(*this);
  }

 private:
  // Get the current value of bit at `bit_idx` and set it to 1
  inline bool GetAndSet(uint32_t bit_idx) {
    const uint32_t byte_idx = bit_idx / kBitsPerEntry;
    const uint32_t bit_mask = 1 << (bit_idx % kBitsPerEntry);

    return bitmap_[byte_idx].fetch_or(bit_mask, std::memory_order_relaxed) &
           bit_mask;
  }

  const uint32_t kBytesPersEntry = sizeof(uint32_t);   // 4 bytes
  const uint32_t kBitsPerEntry = kBytesPersEntry * 8;  // 32 bits

  // Bitmap used to record the bytes that we read, use atomic to protect
  // against multiple threads updating the same bit
  std::atomic<uint32_t>* bitmap_;
  // (1 << bytes_per_bit_pow_) is bytes_per_bit. Use power of 2 to optimize
  // muliplication and division
  uint8_t bytes_per_bit_pow_;
  // Pointer to DB Statistics object, Since this bitmap may outlive the DB
  // this pointer maybe invalid, but the DB will update it to a valid pointer
  // by using SetStatistics() before calling Mark()
  std::atomic<Statistics*> statistics_;
  uint32_t rnd_;
};

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(BlockContents&& contents, SequenceNumber _global_seqno,
                 size_t read_amp_bytes_per_bit = 0,
                 Statistics* statistics = nullptr);

  ~Block();

  size_t size() const { return size_; }
  const char* data() const { return data_; }
  // The additional memory space taken by the block data.
  size_t usable_size() const { return contents_.usable_size(); }
  uint32_t NumRestarts() const;
  bool own_bytes() const { return contents_.own_bytes(); }

  BlockBasedTableOptions::DataBlockIndexType IndexType() const;

  // If comparator is InternalKeyComparator, user_comparator is its user
  // comparator; they are equal otherwise.
  //
  // If iter is null, return new Iterator
  // If iter is not null, update this one and return it as Iterator*
  //
  // key_includes_seq, default true, means that the keys are in internal key
  // format.
  // value_is_full, default true, means that no delta encoding is
  // applied to values.
  //
  // NewIterator<DataBlockIter>
  // Same as above but also updates read_amp_bitmap_ if it is not nullptr.
  //
  // NewIterator<IndexBlockIter>
  // If `prefix_index` is not nullptr this block will do hash lookup for the key
  // prefix. If total_order_seek is true, prefix_index_ is ignored.
  //
  // If `block_contents_pinned` is true, the caller will guarantee that when
  // the cleanup functions are transferred from the iterator to other
  // classes, e.g. PinnableSlice, the pointer to the bytes will still be
  // valid. Either the iterator holds cache handle or ownership of some resource
  // and release them in a release function, or caller is sure that the data
  // will not go away (for example, it's from mmapped file which will not be
  // closed).
  //
  // NOTE: for the hash based lookup, if a key prefix doesn't match any key,
  // the iterator will simply be set as "invalid", rather than returning
  // the key that is just pass the target key.
  template <typename TBlockIter>
  TBlockIter* NewIterator(
      const Comparator* comparator, const Comparator* user_comparator,
      TBlockIter* iter = nullptr, Statistics* stats = nullptr,
      bool total_order_seek = true, bool key_includes_seq = true,
      bool value_is_full = true, bool block_contents_pinned = false,
      BlockPrefixIndex* prefix_index = nullptr);

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const;

  SequenceNumber global_seqno() const { return global_seqno_; }

 private:
  BlockContents contents_;
  const char* data_;            // contents_.data.data()
  size_t size_;                 // contents_.data.size()
  uint32_t restart_offset_;     // Offset in data_ of restart array
  uint32_t num_restarts_;
  std::unique_ptr<BlockReadAmpBitmap> read_amp_bitmap_;
  // All keys in the block will have seqno = global_seqno_, regardless of
  // the encoded value (kDisableGlobalSequenceNumber means disabled)
  const SequenceNumber global_seqno_;

  DataBlockHashIndex data_block_hash_index_;

  // No copying allowed
  Block(const Block&) = delete;
  void operator=(const Block&) = delete;
};

template <class TValue>
class BlockIter : public InternalIteratorBase<TValue> {
 public:
  void InitializeBase(const Comparator* comparator, const char* data,
                      uint32_t restarts, uint32_t num_restarts,
                      SequenceNumber global_seqno, bool block_contents_pinned) {
    assert(data_ == nullptr);           // Ensure it is called only once
    assert(num_restarts > 0);           // Ensure the param is valid

    comparator_ = comparator;
    data_ = data;
    restarts_ = restarts;
    num_restarts_ = num_restarts;
    current_ = restarts_;
    restart_index_ = num_restarts_;
    global_seqno_ = global_seqno;
    block_contents_pinned_ = block_contents_pinned;
  }

  // Makes Valid() return false, status() return `s`, and Seek()/Prev()/etc do
  // nothing. Calls cleanup functions.
  void InvalidateBase(Status s) {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));

    data_ = nullptr;
    current_ = restarts_;
    status_ = s;

    // Call cleanup callbacks.
    Cleanable::Reset();
  }

  virtual bool Valid() const override { return current_ < restarts_; }
  virtual Status status() const override { return status_; }
  virtual Slice key() const override {
    assert(Valid());
    return key_.GetKey();
  }

#ifndef NDEBUG
  virtual ~BlockIter() {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
  }
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool IsKeyPinned() const override {
    return block_contents_pinned_ && key_pinned_;
  }

  virtual bool IsValuePinned() const override { return block_contents_pinned_; }

  size_t TEST_CurrentEntrySize() { return NextEntryOffset() - current_; }

  uint32_t ValueOffset() const {
    return static_cast<uint32_t>(value_.data() - data_);
  }

 protected:
  // Note: The type could be changed to InternalKeyComparator but we see a weird
  // performance drop by that.
  const Comparator* comparator_;
  const char* data_;       // underlying block contents
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // Index of restart block in which current_ or current_-1 falls
  uint32_t restart_index_;
  uint32_t restarts_;       // Offset of restart array (list of fixed32)
  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  IterKey key_;
  Slice value_;
  Status status_;
  bool key_pinned_;
  // Whether the block data is guaranteed to outlive this iterator, and
  // as long as the cleanup functions are transferred to another class,
  // e.g. PinnableSlice, the pointer to the bytes will still be valid.
  bool block_contents_pinned_;
  SequenceNumber global_seqno_;

 public:
  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    // NOTE: We don't support blocks bigger than 2GB
    return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

  void CorruptionError();

  template <typename DecodeKeyFunc>
  inline bool BinarySeek(const Slice& target, uint32_t left, uint32_t right,
                         uint32_t* index, const Comparator* comp);
};

class DataBlockIter final : public BlockIter<Slice> {
 public:
  DataBlockIter()
      : BlockIter(), read_amp_bitmap_(nullptr), last_bitmap_offset_(0) {}
  DataBlockIter(const Comparator* comparator, const Comparator* user_comparator,
                const char* data, uint32_t restarts, uint32_t num_restarts,
                SequenceNumber global_seqno,
                BlockReadAmpBitmap* read_amp_bitmap, bool block_contents_pinned,
                DataBlockHashIndex* data_block_hash_index)
      : DataBlockIter() {
    Initialize(comparator, user_comparator, data, restarts, num_restarts,
               global_seqno, read_amp_bitmap, block_contents_pinned,
               data_block_hash_index);
  }
  void Initialize(const Comparator* comparator,
                  const Comparator* user_comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts,
                  SequenceNumber global_seqno,
                  BlockReadAmpBitmap* read_amp_bitmap,
                  bool block_contents_pinned,
                  DataBlockHashIndex* data_block_hash_index) {
    InitializeBase(comparator, data, restarts, num_restarts, global_seqno,
                   block_contents_pinned);
    user_comparator_ = user_comparator;
    key_.SetIsUserKey(false);
    read_amp_bitmap_ = read_amp_bitmap;
    last_bitmap_offset_ = current_ + 1;
    data_block_hash_index_ = data_block_hash_index;
  }

  virtual Slice value() const override {
    assert(Valid());
    if (read_amp_bitmap_ && current_ < restarts_ &&
        current_ != last_bitmap_offset_) {
      read_amp_bitmap_->Mark(current_ /* current entry offset */,
                             NextEntryOffset() - 1);
      last_bitmap_offset_ = current_;
    }
    return value_;
  }

  virtual void Seek(const Slice& target) override;

  inline bool SeekForGet(const Slice& target) {
    if (!data_block_hash_index_) {
      Seek(target);
      return true;
    }

    return SeekForGetImpl(target);
  }

  virtual void SeekForPrev(const Slice& target) override;

  virtual void Prev() override;

  virtual void Next() override;

  // Try to advance to the next entry in the block. If there is data corruption
  // or error, report it to the caller instead of aborting the process. May
  // incur higher CPU overhead because we need to perform check on every entry.
  void NextOrReport();

  virtual void SeekToFirst() override;

  // Try to seek to the first entry in the block. If there is data corruption
  // or error, report it to caller instead of aborting the process. May incur
  // higher CPU overhead because we need to perform check on every entry.
  void SeekToFirstOrReport();

  virtual void SeekToLast() override;

  void Invalidate(Status s) {
    InvalidateBase(s);
    // Clear prev entries cache.
    prev_entries_keys_buff_.clear();
    prev_entries_.clear();
    prev_entries_idx_ = -1;
  }

 private:
  // read-amp bitmap
  BlockReadAmpBitmap* read_amp_bitmap_;
  // last `current_` value we report to read-amp bitmp
  mutable uint32_t last_bitmap_offset_;
  struct CachedPrevEntry {
    explicit CachedPrevEntry(uint32_t _offset, const char* _key_ptr,
                             size_t _key_offset, size_t _key_size, Slice _value)
        : offset(_offset),
          key_ptr(_key_ptr),
          key_offset(_key_offset),
          key_size(_key_size),
          value(_value) {}

    // offset of entry in block
    uint32_t offset;
    // Pointer to key data in block (nullptr if key is delta-encoded)
    const char* key_ptr;
    // offset of key in prev_entries_keys_buff_ (0 if key_ptr is not nullptr)
    size_t key_offset;
    // size of key
    size_t key_size;
    // value slice pointing to data in block
    Slice value;
  };
  std::string prev_entries_keys_buff_;
  std::vector<CachedPrevEntry> prev_entries_;
  int32_t prev_entries_idx_ = -1;

  DataBlockHashIndex* data_block_hash_index_;
  const Comparator* user_comparator_;

  template <typename DecodeEntryFunc>
  inline bool ParseNextDataKey(const char* limit = nullptr);

  inline int Compare(const IterKey& ikey, const Slice& b) const {
    return comparator_->Compare(ikey.GetInternalKey(), b);
  }

  bool SeekForGetImpl(const Slice& target);
};

class IndexBlockIter final : public BlockIter<BlockHandle> {
 public:
  IndexBlockIter() : BlockIter(), prefix_index_(nullptr) {}

  virtual Slice key() const override {
    assert(Valid());
    return key_.GetKey();
  }
  // key_includes_seq, default true, means that the keys are in internal key
  // format.
  // value_is_full, default true, means that no delta encoding is
  // applied to values.
  IndexBlockIter(const Comparator* comparator,
                 const Comparator* user_comparator, const char* data,
                 uint32_t restarts, uint32_t num_restarts,
                 BlockPrefixIndex* prefix_index, bool key_includes_seq,
                 bool value_is_full, bool block_contents_pinned)
      : IndexBlockIter() {
    Initialize(comparator, user_comparator, data, restarts, num_restarts,
               prefix_index, key_includes_seq, block_contents_pinned,
               value_is_full, nullptr /* data_block_hash_index */);
  }

  void Initialize(const Comparator* comparator,
                  const Comparator* user_comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts,
                  BlockPrefixIndex* prefix_index, bool key_includes_seq,
                  bool value_is_full, bool block_contents_pinned,
                  DataBlockHashIndex* /*data_block_hash_index*/) {
    InitializeBase(key_includes_seq ? comparator : user_comparator, data,
                   restarts, num_restarts, kDisableGlobalSequenceNumber,
                   block_contents_pinned);
    key_includes_seq_ = key_includes_seq;
    key_.SetIsUserKey(!key_includes_seq_);
    prefix_index_ = prefix_index;
    value_delta_encoded_ = !value_is_full;
  }

  virtual BlockHandle value() const override {
    assert(Valid());
    if (value_delta_encoded_) {
      return decoded_value_;
    } else {
      BlockHandle handle;
      Slice v = value_;
      Status decode_s __attribute__((__unused__)) = handle.DecodeFrom(&v);
      assert(decode_s.ok());
      return handle;
    }
  }

  virtual void Seek(const Slice& target) override;

  virtual void SeekForPrev(const Slice&) override {
    assert(false);
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::InvalidArgument(
        "RocksDB internal error: should never call SeekForPrev() on index "
        "blocks");
    key_.Clear();
    value_.clear();
  }

  virtual void Prev() override;

  virtual void Next() override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

  void Invalidate(Status s) { InvalidateBase(s); }

 private:
  // Key is in InternalKey format
  bool key_includes_seq_;
  bool value_delta_encoded_;
  BlockPrefixIndex* prefix_index_;
  // Whether the value is delta encoded. In that case the value is assumed to be
  // BlockHandle. The first value in each restart interval is the full encoded
  // BlockHandle; the restart of encoded size part of the BlockHandle. The
  // offset of delta encoded BlockHandles is computed by adding the size of
  // previous delta encoded values in the same restart interval to the offset of
  // the first value in that restart interval.
  BlockHandle decoded_value_;

  bool PrefixSeek(const Slice& target, uint32_t* index);
  bool BinaryBlockIndexSeek(const Slice& target, uint32_t* block_ids,
                            uint32_t left, uint32_t right,
                            uint32_t* index);
  inline int CompareBlockKey(uint32_t block_index, const Slice& target);

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  inline int Compare(const IterKey& ikey, const Slice& b) const {
    return comparator_->Compare(ikey.GetKey(), b);
  }

  inline bool ParseNextIndexKey();

  // When value_delta_encoded_ is enabled it decodes the value which is assumed
  // to be BlockHandle and put it to decoded_value_
  inline void DecodeCurrentValue(uint32_t shared);
};

}  // namespace rocksdb
