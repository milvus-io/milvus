//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_format.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace rocksdb {
namespace blob_db {

void BlobLogHeader::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(BlobLogHeader::kSize);
  PutFixed32(dst, kMagicNumber);
  PutFixed32(dst, version);
  PutFixed32(dst, column_family_id);
  unsigned char flags = (has_ttl ? 1 : 0);
  dst->push_back(flags);
  dst->push_back(compression);
  PutFixed64(dst, expiration_range.first);
  PutFixed64(dst, expiration_range.second);
}

Status BlobLogHeader::DecodeFrom(Slice src) {
  static const std::string kErrorMessage =
      "Error while decoding blob log header";
  if (src.size() != BlobLogHeader::kSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected blob file header size");
  }
  uint32_t magic_number;
  unsigned char flags;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed32(&src, &column_family_id)) {
    return Status::Corruption(
        kErrorMessage,
        "Error decoding magic number, version and column family id");
  }
  if (magic_number != kMagicNumber) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (version != kVersion1) {
    return Status::Corruption(kErrorMessage, "Unknown header version");
  }
  flags = src.data()[0];
  compression = static_cast<CompressionType>(src.data()[1]);
  has_ttl = (flags & 1) == 1;
  src.remove_prefix(2);
  if (!GetFixed64(&src, &expiration_range.first) ||
      !GetFixed64(&src, &expiration_range.second)) {
    return Status::Corruption(kErrorMessage, "Error decoding expiration range");
  }
  return Status::OK();
}

void BlobLogFooter::EncodeTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(BlobLogFooter::kSize);
  PutFixed32(dst, kMagicNumber);
  PutFixed64(dst, blob_count);
  PutFixed64(dst, expiration_range.first);
  PutFixed64(dst, expiration_range.second);
  crc = crc32c::Value(dst->c_str(), dst->size());
  crc = crc32c::Mask(crc);
  PutFixed32(dst, crc);
}

Status BlobLogFooter::DecodeFrom(Slice src) {
  static const std::string kErrorMessage =
      "Error while decoding blob log footer";
  if (src.size() != BlobLogFooter::kSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected blob file footer size");
  }
  uint32_t src_crc = 0;
  src_crc = crc32c::Value(src.data(), BlobLogFooter::kSize - sizeof(uint32_t));
  src_crc = crc32c::Mask(src_crc);
  uint32_t magic_number;
  if (!GetFixed32(&src, &magic_number) || !GetFixed64(&src, &blob_count) ||
      !GetFixed64(&src, &expiration_range.first) ||
      !GetFixed64(&src, &expiration_range.second) || !GetFixed32(&src, &crc)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  if (magic_number != kMagicNumber) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (src_crc != crc) {
    return Status::Corruption(kErrorMessage, "CRC mismatch");
  }
  return Status::OK();
}

void BlobLogRecord::EncodeHeaderTo(std::string* dst) {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(BlobLogRecord::kHeaderSize + key.size() + value.size());
  PutFixed64(dst, key.size());
  PutFixed64(dst, value.size());
  PutFixed64(dst, expiration);
  header_crc = crc32c::Value(dst->c_str(), dst->size());
  header_crc = crc32c::Mask(header_crc);
  PutFixed32(dst, header_crc);
  blob_crc = crc32c::Value(key.data(), key.size());
  blob_crc = crc32c::Extend(blob_crc, value.data(), value.size());
  blob_crc = crc32c::Mask(blob_crc);
  PutFixed32(dst, blob_crc);
}

Status BlobLogRecord::DecodeHeaderFrom(Slice src) {
  static const std::string kErrorMessage = "Error while decoding blob record";
  if (src.size() != BlobLogRecord::kHeaderSize) {
    return Status::Corruption(kErrorMessage,
                              "Unexpected blob record header size");
  }
  uint32_t src_crc = 0;
  src_crc = crc32c::Value(src.data(), BlobLogRecord::kHeaderSize - 8);
  src_crc = crc32c::Mask(src_crc);
  if (!GetFixed64(&src, &key_size) || !GetFixed64(&src, &value_size) ||
      !GetFixed64(&src, &expiration) || !GetFixed32(&src, &header_crc) ||
      !GetFixed32(&src, &blob_crc)) {
    return Status::Corruption(kErrorMessage, "Error decoding content");
  }
  if (src_crc != header_crc) {
    return Status::Corruption(kErrorMessage, "Header CRC mismatch");
  }
  return Status::OK();
}

Status BlobLogRecord::CheckBlobCRC() const {
  uint32_t expected_crc = 0;
  expected_crc = crc32c::Value(key.data(), key.size());
  expected_crc = crc32c::Extend(expected_crc, value.data(), value.size());
  expected_crc = crc32c::Mask(expected_crc);
  if (expected_crc != blob_crc) {
    return Status::Corruption("Blob CRC mismatch");
  }
  return Status::OK();
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
