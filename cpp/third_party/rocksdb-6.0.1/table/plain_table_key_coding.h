//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <array>
#include "rocksdb/slice.h"
#include "db/dbformat.h"
#include "table/plain_table_reader.h"

namespace rocksdb {

class WritableFile;
struct ParsedInternalKey;
struct PlainTableReaderFileInfo;
enum PlainTableEntryType : unsigned char;

// Helper class to write out a key to an output file
// Actual data format of the key is documented in plain_table_factory.h
class PlainTableKeyEncoder {
 public:
  explicit PlainTableKeyEncoder(EncodingType encoding_type,
                                uint32_t user_key_len,
                                const SliceTransform* prefix_extractor,
                                size_t index_sparseness)
      : encoding_type_((prefix_extractor != nullptr) ? encoding_type : kPlain),
        fixed_user_key_len_(user_key_len),
        prefix_extractor_(prefix_extractor),
        index_sparseness_((index_sparseness > 1) ? index_sparseness : 1),
        key_count_for_prefix_(0) {}
  // key: the key to write out, in the format of internal key.
  // file: the output file to write out
  // offset: offset in the file. Needs to be updated after appending bytes
  //         for the key
  // meta_bytes_buf: buffer for extra meta bytes
  // meta_bytes_buf_size: offset to append extra meta bytes. Will be updated
  //                      if meta_bytes_buf is updated.
  Status AppendKey(const Slice& key, WritableFileWriter* file, uint64_t* offset,
                   char* meta_bytes_buf, size_t* meta_bytes_buf_size);

  // Return actual encoding type to be picked
  EncodingType GetEncodingType() { return encoding_type_; }

 private:
  EncodingType encoding_type_;
  uint32_t fixed_user_key_len_;
  const SliceTransform* prefix_extractor_;
  const size_t index_sparseness_;
  size_t key_count_for_prefix_;
  IterKey pre_prefix_;
};

class PlainTableFileReader {
 public:
  explicit PlainTableFileReader(const PlainTableReaderFileInfo* _file_info)
      : file_info_(_file_info), num_buf_(0) {}
  // In mmaped mode, the results point to mmaped area of the file, which
  // means it is always valid before closing the file.
  // In non-mmap mode, the results point to an internal buffer. If the caller
  // makes another read call, the results may not be valid. So callers should
  // make a copy when needed.
  // In order to save read calls to files, we keep two internal buffers:
  // the first read and the most recent read. This is efficient because it
  // columns these two common use cases:
  // (1) hash index only identify one location, we read the key to verify
  //     the location, and read key and value if it is the right location.
  // (2) after hash index checking, we identify two locations (because of
  //     hash bucket conflicts), we binary search the two location to see
  //     which one is what we need and start to read from the location.
  // These two most common use cases will be covered by the two buffers
  // so that we don't need to re-read the same location.
  // Currently we keep a fixed size buffer. If a read doesn't exactly fit
  // the buffer, we replace the second buffer with the location user reads.
  //
  // If return false, status code is stored in status_.
  bool Read(uint32_t file_offset, uint32_t len, Slice* out) {
    if (file_info_->is_mmap_mode) {
      assert(file_offset + len <= file_info_->data_end_offset);
      *out = Slice(file_info_->file_data.data() + file_offset, len);
      return true;
    } else {
      return ReadNonMmap(file_offset, len, out);
    }
  }

  // If return false, status code is stored in status_.
  bool ReadNonMmap(uint32_t file_offset, uint32_t len, Slice* output);

  // *bytes_read = 0 means eof. false means failure and status is saved
  // in status_. Not directly returning Status to save copying status
  // object to map previous performance of mmap mode.
  inline bool ReadVarint32(uint32_t offset, uint32_t* output,
                           uint32_t* bytes_read);

  bool ReadVarint32NonMmap(uint32_t offset, uint32_t* output,
                           uint32_t* bytes_read);

  Status status() const { return status_; }

  const PlainTableReaderFileInfo* file_info() { return file_info_; }

 private:
  const PlainTableReaderFileInfo* file_info_;

  struct Buffer {
    Buffer() : buf_start_offset(0), buf_len(0), buf_capacity(0) {}
    std::unique_ptr<char[]> buf;
    uint32_t buf_start_offset;
    uint32_t buf_len;
    uint32_t buf_capacity;
  };

  // Keep buffers for two recent reads.
  std::array<std::unique_ptr<Buffer>, 2> buffers_;
  uint32_t num_buf_;
  Status status_;

  Slice GetFromBuffer(Buffer* buf, uint32_t file_offset, uint32_t len);
};

// A helper class to decode keys from input buffer
// Actual data format of the key is documented in plain_table_factory.h
class PlainTableKeyDecoder {
 public:
  explicit PlainTableKeyDecoder(const PlainTableReaderFileInfo* file_info,
                                EncodingType encoding_type,
                                uint32_t user_key_len,
                                const SliceTransform* prefix_extractor)
      : file_reader_(file_info),
        encoding_type_(encoding_type),
        prefix_len_(0),
        fixed_user_key_len_(user_key_len),
        prefix_extractor_(prefix_extractor),
        in_prefix_(false) {}
  // Find the next key.
  // start: char array where the key starts.
  // limit: boundary of the char array
  // parsed_key: the output of the result key
  // internal_key: if not null, fill with the output of the result key in
  //               un-parsed format
  // bytes_read: how many bytes read from start. Output
  // seekable: whether key can be read from this place. Used when building
  //           indexes. Output.
  Status NextKey(uint32_t start_offset, ParsedInternalKey* parsed_key,
                 Slice* internal_key, Slice* value, uint32_t* bytes_read,
                 bool* seekable = nullptr);

  Status NextKeyNoValue(uint32_t start_offset, ParsedInternalKey* parsed_key,
                        Slice* internal_key, uint32_t* bytes_read,
                        bool* seekable = nullptr);

  PlainTableFileReader file_reader_;
  EncodingType encoding_type_;
  uint32_t prefix_len_;
  uint32_t fixed_user_key_len_;
  Slice saved_user_key_;
  IterKey cur_key_;
  const SliceTransform* prefix_extractor_;
  bool in_prefix_;

 private:
  Status NextPlainEncodingKey(uint32_t start_offset,
                              ParsedInternalKey* parsed_key,
                              Slice* internal_key, uint32_t* bytes_read,
                              bool* seekable = nullptr);
  Status NextPrefixEncodingKey(uint32_t start_offset,
                               ParsedInternalKey* parsed_key,
                               Slice* internal_key, uint32_t* bytes_read,
                               bool* seekable = nullptr);
  Status ReadInternalKey(uint32_t file_offset, uint32_t user_key_size,
                         ParsedInternalKey* parsed_key, uint32_t* bytes_read,
                         bool* internal_key_valid, Slice* internal_key);
  inline Status DecodeSize(uint32_t start_offset,
                           PlainTableEntryType* entry_type, uint32_t* key_size,
                           uint32_t* bytes_read);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
