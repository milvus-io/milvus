//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/options.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {
namespace blob_db {

// BlobIndex is a pointer to the blob and metadata of the blob. The index is
// stored in base DB as ValueType::kTypeBlobIndex.
// There are three types of blob index:
//
//    kInlinedTTL:
//      +------+------------+---------------+
//      | type | expiration | value         |
//      +------+------------+---------------+
//      | char | varint64   | variable size |
//      +------+------------+---------------+
//
//    kBlob:
//      +------+-------------+----------+----------+-------------+
//      | type | file number | offset   | size     | compression |
//      +------+-------------+----------+----------+-------------+
//      | char | varint64    | varint64 | varint64 | char        |
//      +------+-------------+----------+----------+-------------+
//
//    kBlobTTL:
//      +------+------------+-------------+----------+----------+-------------+
//      | type | expiration | file number | offset   | size     | compression |
//      +------+------------+-------------+----------+----------+-------------+
//      | char | varint64   | varint64    | varint64 | varint64 | char        |
//      +------+------------+-------------+----------+----------+-------------+
//
// There isn't a kInlined (without TTL) type since we can store it as a plain
// value (i.e. ValueType::kTypeValue).
class BlobIndex {
 public:
  enum class Type : unsigned char {
    kInlinedTTL = 0,
    kBlob = 1,
    kBlobTTL = 2,
    kUnknown = 3,
  };

  BlobIndex() : type_(Type::kUnknown) {}

  bool IsInlined() const { return type_ == Type::kInlinedTTL; }

  bool HasTTL() const {
    return type_ == Type::kInlinedTTL || type_ == Type::kBlobTTL;
  }

  uint64_t expiration() const {
    assert(HasTTL());
    return expiration_;
  }

  const Slice& value() const {
    assert(IsInlined());
    return value_;
  }

  uint64_t file_number() const {
    assert(!IsInlined());
    return file_number_;
  }

  uint64_t offset() const {
    assert(!IsInlined());
    return offset_;
  }

  uint64_t size() const {
    assert(!IsInlined());
    return size_;
  }

  Status DecodeFrom(Slice slice) {
    static const std::string kErrorMessage = "Error while decoding blob index";
    assert(slice.size() > 0);
    type_ = static_cast<Type>(*slice.data());
    if (type_ >= Type::kUnknown) {
      return Status::Corruption(
          kErrorMessage,
          "Unknown blob index type: " + ToString(static_cast<char>(type_)));
    }
    slice = Slice(slice.data() + 1, slice.size() - 1);
    if (HasTTL()) {
      if (!GetVarint64(&slice, &expiration_)) {
        return Status::Corruption(kErrorMessage, "Corrupted expiration");
      }
    }
    if (IsInlined()) {
      value_ = slice;
    } else {
      if (GetVarint64(&slice, &file_number_) && GetVarint64(&slice, &offset_) &&
          GetVarint64(&slice, &size_) && slice.size() == 1) {
        compression_ = static_cast<CompressionType>(*slice.data());
      } else {
        return Status::Corruption(kErrorMessage, "Corrupted blob offset");
      }
    }
    return Status::OK();
  }

  static void EncodeInlinedTTL(std::string* dst, uint64_t expiration,
                               const Slice& value) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(1 + kMaxVarint64Length + value.size());
    dst->push_back(static_cast<char>(Type::kInlinedTTL));
    PutVarint64(dst, expiration);
    dst->append(value.data(), value.size());
  }

  static void EncodeBlob(std::string* dst, uint64_t file_number,
                         uint64_t offset, uint64_t size,
                         CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 3 + 2);
    dst->push_back(static_cast<char>(Type::kBlob));
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

  static void EncodeBlobTTL(std::string* dst, uint64_t expiration,
                            uint64_t file_number, uint64_t offset,
                            uint64_t size, CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 4 + 2);
    dst->push_back(static_cast<char>(Type::kBlobTTL));
    PutVarint64(dst, expiration);
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

 private:
  Type type_ = Type::kUnknown;
  uint64_t expiration_ = 0;
  Slice value_;
  uint64_t file_number_ = 0;
  uint64_t offset_ = 0;
  uint64_t size_ = 0;
  CompressionType compression_ = kNoCompression;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
