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
#include <string>
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

#include "options/cf_options.h"
#include "port/port.h"  // noexcept
#include "table/persistent_cache_options.h"
#include "util/file_reader_writer.h"
#include "util/memory_allocator.h"

namespace rocksdb {

class RandomAccessFile;
struct ReadOptions;

extern bool ShouldReportDetailedTime(Env* env, Statistics* stats);

// the length of the magic number in bytes.
const int kMagicNumberLengthByte = 8;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
 public:
  BlockHandle();
  BlockHandle(uint64_t offset, uint64_t size);

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t _offset) { offset_ = _offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t _size) { size_ = _size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
  Status DecodeSizeFrom(uint64_t offset, Slice* input);

  // Return a string that contains the copy of handle.
  std::string ToString(bool hex = true) const;

  // if the block handle's offset and size are both "0", we will view it
  // as a null block handle that points to no where.
  bool IsNull() const { return offset_ == 0 && size_ == 0; }

  static const BlockHandle& NullBlockHandle() { return kNullBlockHandle; }

  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

 private:
  uint64_t offset_;
  uint64_t size_;

  static const BlockHandle kNullBlockHandle;
};

inline uint32_t GetCompressFormatForVersion(
    CompressionType compression_type, uint32_t version) {
#ifdef NDEBUG
  (void)compression_type;
#endif
  // snappy is not versioned
  assert(compression_type != kSnappyCompression &&
         compression_type != kXpressCompression &&
         compression_type != kNoCompression);
  // As of version 2, we encode compressed block with
  // compress_format_version == 2. Before that, the version is 1.
  // DO NOT CHANGE THIS FUNCTION, it affects disk format
  return version >= 2 ? 2 : 1;
}

inline bool BlockBasedTableSupportedVersion(uint32_t version) {
  return version <= 4;
}

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
 public:
  // Constructs a footer without specifying its table magic number.
  // In such case, the table magic number of such footer should be
  // initialized via @ReadFooterFromFile().
  // Use this when you plan to load Footer with DecodeFrom(). Never use this
  // when you plan to EncodeTo.
  Footer() : Footer(kInvalidTableMagicNumber, 0) {}

  // Use this constructor when you plan to write out the footer using
  // EncodeTo(). Never use this constructor with DecodeFrom().
  Footer(uint64_t table_magic_number, uint32_t version);

  // The version of the footer in this file
  uint32_t version() const { return version_; }

  // The checksum type used in this file
  ChecksumType checksum() const { return checksum_; }
  void set_checksum(const ChecksumType c) { checksum_ = c; }

  // The block handle for the metaindex block of the table
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  const BlockHandle& index_handle() const { return index_handle_; }

  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  uint64_t table_magic_number() const { return table_magic_number_; }

  void EncodeTo(std::string* dst) const;

  // Set the current footer based on the input slice.
  //
  // REQUIRES: table_magic_number_ is not set (i.e.,
  // HasInitializedTableMagicNumber() is true). The function will initialize the
  // magic number
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a Footer will
  // always occupy at least kMinEncodedLength bytes.  If fields are changed
  // the version number should be incremented and kMaxEncodedLength should be
  // increased accordingly.
  enum {
    // Footer version 0 (legacy) will always occupy exactly this many bytes.
    // It consists of two block handles, padding, and a magic number.
    kVersion0EncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8,
    // Footer of versions 1 and higher will always occupy exactly this many
    // bytes. It consists of the checksum type, two block handles, padding,
    // a version number (bigger than 1), and a magic number
    kNewVersionsEncodedLength = 1 + 2 * BlockHandle::kMaxEncodedLength + 4 + 8,
    kMinEncodedLength = kVersion0EncodedLength,
    kMaxEncodedLength = kNewVersionsEncodedLength,
  };

  static const uint64_t kInvalidTableMagicNumber = 0;

  // convert this object to a human readable form
  std::string ToString() const;

 private:
  // REQUIRES: magic number wasn't initialized.
  void set_table_magic_number(uint64_t magic_number) {
    assert(!HasInitializedTableMagicNumber());
    table_magic_number_ = magic_number;
  }

  // return true if @table_magic_number_ is set to a value different
  // from @kInvalidTableMagicNumber.
  bool HasInitializedTableMagicNumber() const {
    return (table_magic_number_ != kInvalidTableMagicNumber);
  }

  uint32_t version_;
  ChecksumType checksum_;
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
  uint64_t table_magic_number_ = 0;
};

// Read the footer from file
// If enforce_table_magic_number != 0, ReadFooterFromFile() will return
// corruption if table_magic number is not equal to enforce_table_magic_number
Status ReadFooterFromFile(RandomAccessFileReader* file,
                          FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number = 0);

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

inline CompressionType get_block_compression_type(const char* block_data,
                                                  size_t block_size) {
  return static_cast<CompressionType>(block_data[block_size]);
}

struct BlockContents {
  Slice data;     // Actual contents of data
  CacheAllocationPtr allocation;

#ifndef NDEBUG
  // Whether the block is a raw block, which contains compression type
  // byte. It is only used for assertion.
  bool is_raw_block = false;
#endif  // NDEBUG

  BlockContents() {}

  BlockContents(const Slice& _data) : data(_data) {}

  BlockContents(CacheAllocationPtr&& _data, size_t _size)
      : data(_data.get(), _size), allocation(std::move(_data)) {}

  BlockContents(std::unique_ptr<char[]>&& _data, size_t _size)
      : data(_data.get(), _size) {
    allocation.reset(_data.release());
  }

  bool own_bytes() const { return allocation.get() != nullptr; }

  // It's the caller's responsibility to make sure that this is
  // for raw block contents, which contains the compression
  // byte in the end.
  CompressionType get_compression_type() const {
    assert(is_raw_block);
    return get_block_compression_type(data.data(), data.size());
  }

  // The additional memory space taken by the block data.
  size_t usable_size() const {
    if (allocation.get() != nullptr) {
      auto allocator = allocation.get_deleter().allocator;
      if (allocator) {
        return allocator->UsableSize(allocation.get(), data.size());
      }
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(allocation.get());
#else
      return data.size();
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    } else {
      return 0;  // no extra memory is occupied by the data
    }
  }

  size_t ApproximateMemoryUsage() const {
    return usable_size() + sizeof(*this);
  }

  BlockContents(BlockContents&& other) ROCKSDB_NOEXCEPT {
    *this = std::move(other);
  }

  BlockContents& operator=(BlockContents&& other) {
    data = std::move(other.data);
    allocation = std::move(other.allocation);
#ifndef NDEBUG
    is_raw_block = other.is_raw_block;
#endif  // NDEBUG
    return *this;
  }
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
extern Status ReadBlockContents(
    RandomAccessFileReader* file, FilePrefetchBuffer* prefetch_buffer,
    const Footer& footer, const ReadOptions& options, const BlockHandle& handle,
    BlockContents* contents, const ImmutableCFOptions& ioptions,
    bool do_uncompress = true, const Slice& compression_dict = Slice(),
    const PersistentCacheOptions& cache_options = PersistentCacheOptions());

// The 'data' points to the raw block contents read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This buffer is
// returned via 'result' and it is upto the caller to
// free this buffer.
// For description of compress_format_version and possible values, see
// util/compression.h
extern Status UncompressBlockContents(const UncompressionInfo& info,
                                      const char* data, size_t n,
                                      BlockContents* contents,
                                      uint32_t compress_format_version,
                                      const ImmutableCFOptions& ioptions,
                                      MemoryAllocator* allocator = nullptr);

// This is an extension to UncompressBlockContents that accepts
// a specific compression type. This is used by un-wrapped blocks
// with no compression header.
extern Status UncompressBlockContentsForCompressionType(
    const UncompressionInfo& info, const char* data, size_t n,
    BlockContents* contents, uint32_t compress_format_version,
    const ImmutableCFOptions& ioptions, MemoryAllocator* allocator = nullptr);

// Implementation details follow.  Clients should ignore,

// TODO(andrewkr): we should prefer one way of representing a null/uninitialized
// BlockHandle. Currently we use zeros for null and use negation-of-zeros for
// uninitialized.
inline BlockHandle::BlockHandle()
    : BlockHandle(~static_cast<uint64_t>(0), ~static_cast<uint64_t>(0)) {}

inline BlockHandle::BlockHandle(uint64_t _offset, uint64_t _size)
    : offset_(_offset), size_(_size) {}

}  // namespace rocksdb
