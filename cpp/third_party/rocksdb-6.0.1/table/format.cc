//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include <string>
#include <inttypes.h>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/env.h"
#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/block_fetcher.h"
#include "table/persistent_cache_helper.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/logging.h"
#include "util/memory_allocator.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace rocksdb {

extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;

#ifndef ROCKSDB_LITE
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
#else
// ROCKSDB_LITE doesn't have plain table
const uint64_t kLegacyPlainTableMagicNumber = 0;
const uint64_t kPlainTableMagicNumber = 0;
#endif

bool ShouldReportDetailedTime(Env* env, Statistics* stats) {
  return env != nullptr && stats != nullptr &&
         stats->stats_level_ > kExceptDetailedTimers;
}

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64Varint64(dst, offset_, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

Status BlockHandle::DecodeSizeFrom(uint64_t _offset, Slice* input) {
  if (GetVarint64(input, &size_)) {
    offset_ = _offset;
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

// Return a string that contains the copy of handle.
std::string BlockHandle::ToString(bool hex) const {
  std::string handle_str;
  EncodeTo(&handle_str);
  if (hex) {
    return Slice(handle_str).ToString(true);
  } else {
    return handle_str;
  }
}

const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

namespace {
inline bool IsLegacyFooterFormat(uint64_t magic_number) {
  return magic_number == kLegacyBlockBasedTableMagicNumber ||
         magic_number == kLegacyPlainTableMagicNumber;
}
inline uint64_t UpconvertLegacyFooterFormat(uint64_t magic_number) {
  if (magic_number == kLegacyBlockBasedTableMagicNumber) {
    return kBlockBasedTableMagicNumber;
  }
  if (magic_number == kLegacyPlainTableMagicNumber) {
    return kPlainTableMagicNumber;
  }
  assert(false);
  return 0;
}
}  // namespace

// legacy footer format:
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
//    table_magic_number (8 bytes)
// new footer format:
//    checksum type (char, 1 byte)
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
//    footer version (4 bytes)
//    table_magic_number (8 bytes)
void Footer::EncodeTo(std::string* dst) const {
  assert(HasInitializedTableMagicNumber());
  if (IsLegacyFooterFormat(table_magic_number())) {
    // has to be default checksum with legacy footer
    assert(checksum_ == kCRC32c);
    const size_t original_size = dst->size();
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(original_size + 2 * BlockHandle::kMaxEncodedLength);  // Padding
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
    assert(dst->size() == original_size + kVersion0EncodedLength);
  } else {
    const size_t original_size = dst->size();
    dst->push_back(static_cast<char>(checksum_));
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(original_size + kNewVersionsEncodedLength - 12);  // Padding
    PutFixed32(dst, version());
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
    assert(dst->size() == original_size + kNewVersionsEncodedLength);
  }
}

Footer::Footer(uint64_t _table_magic_number, uint32_t _version)
    : version_(_version),
      checksum_(kCRC32c),
      table_magic_number_(_table_magic_number) {
  // This should be guaranteed by constructor callers
  assert(!IsLegacyFooterFormat(_table_magic_number) || version_ == 0);
}

Status Footer::DecodeFrom(Slice* input) {
  assert(!HasInitializedTableMagicNumber());
  assert(input != nullptr);
  assert(input->size() >= kMinEncodedLength);

  const char *magic_ptr =
      input->data() + input->size() - kMagicNumberLengthByte;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                    (static_cast<uint64_t>(magic_lo)));

  // We check for legacy formats here and silently upconvert them
  bool legacy = IsLegacyFooterFormat(magic);
  if (legacy) {
    magic = UpconvertLegacyFooterFormat(magic);
  }
  set_table_magic_number(magic);

  if (legacy) {
    // The size is already asserted to be at least kMinEncodedLength
    // at the beginning of the function
    input->remove_prefix(input->size() - kVersion0EncodedLength);
    version_ = 0 /* legacy */;
    checksum_ = kCRC32c;
  } else {
    version_ = DecodeFixed32(magic_ptr - 4);
    // Footer version 1 and higher will always occupy exactly this many bytes.
    // It consists of the checksum type, two block handles, padding,
    // a version number, and a magic number
    if (input->size() < kNewVersionsEncodedLength) {
      return Status::Corruption("input is too short to be an sstable");
    } else {
      input->remove_prefix(input->size() - kNewVersionsEncodedLength);
    }
    uint32_t chksum;
    if (!GetVarint32(input, &chksum)) {
      return Status::Corruption("bad checksum type");
    }
    checksum_ = static_cast<ChecksumType>(chksum);
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + kMagicNumberLengthByte;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

std::string Footer::ToString() const {
  std::string result;
  result.reserve(1024);

  bool legacy = IsLegacyFooterFormat(table_magic_number_);
  if (legacy) {
    result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
    result.append("index handle: " + index_handle_.ToString() + "\n  ");
    result.append("table_magic_number: " +
                  rocksdb::ToString(table_magic_number_) + "\n  ");
  } else {
    result.append("checksum: " + rocksdb::ToString(checksum_) + "\n  ");
    result.append("metaindex handle: " + metaindex_handle_.ToString() + "\n  ");
    result.append("index handle: " + index_handle_.ToString() + "\n  ");
    result.append("footer version: " + rocksdb::ToString(version_) + "\n  ");
    result.append("table_magic_number: " +
                  rocksdb::ToString(table_magic_number_) + "\n  ");
  }
  return result;
}

Status ReadFooterFromFile(RandomAccessFileReader* file,
                          FilePrefetchBuffer* prefetch_buffer,
                          uint64_t file_size, Footer* footer,
                          uint64_t enforce_table_magic_number) {
  if (file_size < Footer::kMinEncodedLength) {
    return Status::Corruption(
      "file is too short (" + ToString(file_size) + " bytes) to be an "
      "sstable: " + file->file_name());
  }

  char footer_space[Footer::kMaxEncodedLength];
  Slice footer_input;
  size_t read_offset =
      (file_size > Footer::kMaxEncodedLength)
          ? static_cast<size_t>(file_size - Footer::kMaxEncodedLength)
          : 0;
  Status s;
  if (prefetch_buffer == nullptr ||
      !prefetch_buffer->TryReadFromCache(read_offset, Footer::kMaxEncodedLength,
                                         &footer_input)) {
    s = file->Read(read_offset, Footer::kMaxEncodedLength, &footer_input,
                   footer_space);
    if (!s.ok()) return s;
  }

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    return Status::Corruption(
      "file is too short (" + ToString(file_size) + " bytes) to be an "
      "sstable" + file->file_name());
  }

  s = footer->DecodeFrom(&footer_input);
  if (!s.ok()) {
    return s;
  }
  if (enforce_table_magic_number != 0 &&
      enforce_table_magic_number != footer->table_magic_number()) {
    return Status::Corruption(
      "Bad table magic number: expected "
      + ToString(enforce_table_magic_number) + ", found "
      + ToString(footer->table_magic_number())
      + " in " + file->file_name());
  }
  return Status::OK();
}

Status UncompressBlockContentsForCompressionType(
    const UncompressionInfo& uncompression_info, const char* data, size_t n,
    BlockContents* contents, uint32_t format_version,
    const ImmutableCFOptions& ioptions, MemoryAllocator* allocator) {
  CacheAllocationPtr ubuf;

  assert(uncompression_info.type() != kNoCompression &&
         "Invalid compression type");

  StopWatchNano timer(ioptions.env,
    ShouldReportDetailedTime(ioptions.env, ioptions.statistics));
  int decompress_size = 0;
  switch (uncompression_info.type()) {
    case kSnappyCompression: {
      size_t ulength = 0;
      static char snappy_corrupt_msg[] =
        "Snappy not supported or corrupted Snappy compressed block contents";
      if (!Snappy_GetUncompressedLength(data, n, &ulength)) {
        return Status::Corruption(snappy_corrupt_msg);
      }
      ubuf = AllocateBlock(ulength, allocator);
      if (!Snappy_Uncompress(data, n, ubuf.get())) {
        return Status::Corruption(snappy_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), ulength);
      break;
    }
    case kZlibCompression:
      ubuf = Zlib_Uncompress(
          uncompression_info, data, n, &decompress_size,
          GetCompressFormatForVersion(kZlibCompression, format_version),
          allocator);
      if (!ubuf) {
        static char zlib_corrupt_msg[] =
          "Zlib not supported or corrupted Zlib compressed block contents";
        return Status::Corruption(zlib_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), decompress_size);
      break;
    case kBZip2Compression:
      ubuf = BZip2_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kBZip2Compression, format_version),
          allocator);
      if (!ubuf) {
        static char bzip2_corrupt_msg[] =
          "Bzip2 not supported or corrupted Bzip2 compressed block contents";
        return Status::Corruption(bzip2_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), decompress_size);
      break;
    case kLZ4Compression:
      ubuf = LZ4_Uncompress(
          uncompression_info, data, n, &decompress_size,
          GetCompressFormatForVersion(kLZ4Compression, format_version),
          allocator);
      if (!ubuf) {
        static char lz4_corrupt_msg[] =
          "LZ4 not supported or corrupted LZ4 compressed block contents";
        return Status::Corruption(lz4_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), decompress_size);
      break;
    case kLZ4HCCompression:
      ubuf = LZ4_Uncompress(
          uncompression_info, data, n, &decompress_size,
          GetCompressFormatForVersion(kLZ4HCCompression, format_version),
          allocator);
      if (!ubuf) {
        static char lz4hc_corrupt_msg[] =
          "LZ4HC not supported or corrupted LZ4HC compressed block contents";
        return Status::Corruption(lz4hc_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), decompress_size);
      break;
    case kXpressCompression:
      // XPRESS allocates memory internally, thus no support for custom
      // allocator.
      ubuf.reset(XPRESS_Uncompress(data, n, &decompress_size));
      if (!ubuf) {
        static char xpress_corrupt_msg[] =
          "XPRESS not supported or corrupted XPRESS compressed block contents";
        return Status::Corruption(xpress_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), decompress_size);
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      ubuf = ZSTD_Uncompress(uncompression_info, data, n, &decompress_size,
                             allocator);
      if (!ubuf) {
        static char zstd_corrupt_msg[] =
            "ZSTD not supported or corrupted ZSTD compressed block contents";
        return Status::Corruption(zstd_corrupt_msg);
      }
      *contents = BlockContents(std::move(ubuf), decompress_size);
      break;
    default:
      return Status::Corruption("bad block type");
  }

  if(ShouldReportDetailedTime(ioptions.env, ioptions.statistics)){
    MeasureTime(ioptions.statistics, DECOMPRESSION_TIMES_NANOS,
      timer.ElapsedNanos());
  }
  MeasureTime(ioptions.statistics, BYTES_DECOMPRESSED, contents->data.size());
  RecordTick(ioptions.statistics, NUMBER_BLOCK_DECOMPRESSED);

  return Status::OK();
}

//
// The 'data' points to the raw block contents that was read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This
// buffer is returned via 'result' and it is upto the caller to
// free this buffer.
// format_version is the block format as defined in include/rocksdb/table.h
Status UncompressBlockContents(const UncompressionInfo& uncompression_info,
                               const char* data, size_t n,
                               BlockContents* contents, uint32_t format_version,
                               const ImmutableCFOptions& ioptions,
                               MemoryAllocator* allocator) {
  assert(data[n] != kNoCompression);
  assert(data[n] == uncompression_info.type());
  return UncompressBlockContentsForCompressionType(uncompression_info, data, n,
                                                   contents, format_version,
                                                   ioptions, allocator);
}

}  // namespace rocksdb
