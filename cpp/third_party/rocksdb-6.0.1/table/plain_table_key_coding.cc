//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "table/plain_table_key_coding.h"

#include <algorithm>
#include <string>
#include "db/dbformat.h"
#include "table/plain_table_reader.h"
#include "table/plain_table_factory.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

enum PlainTableEntryType : unsigned char {
  kFullKey = 0,
  kPrefixFromPreviousKey = 1,
  kKeySuffix = 2,
};

namespace {

// Control byte:
// First two bits indicate type of entry
// Other bytes are inlined sizes. If all bits are 1 (0x03F), overflow bytes
// are used. key_size-0x3F will be encoded as a variint32 after this bytes.

const unsigned char kSizeInlineLimit = 0x3F;

// Return 0 for error
size_t EncodeSize(PlainTableEntryType type, uint32_t key_size,
                  char* out_buffer) {
  out_buffer[0] = type << 6;

  if (key_size < static_cast<uint32_t>(kSizeInlineLimit)) {
    // size inlined
    out_buffer[0] |= static_cast<char>(key_size);
    return 1;
  } else {
    out_buffer[0] |= kSizeInlineLimit;
    char* ptr = EncodeVarint32(out_buffer + 1, key_size - kSizeInlineLimit);
    return ptr - out_buffer;
  }
}
}  // namespace

// Fill bytes_read with number of bytes read.
inline Status PlainTableKeyDecoder::DecodeSize(uint32_t start_offset,
                                               PlainTableEntryType* entry_type,
                                               uint32_t* key_size,
                                               uint32_t* bytes_read) {
  Slice next_byte_slice;
  bool success = file_reader_.Read(start_offset, 1, &next_byte_slice);
  if (!success) {
    return file_reader_.status();
  }
  *entry_type = static_cast<PlainTableEntryType>(
      (static_cast<unsigned char>(next_byte_slice[0]) & ~kSizeInlineLimit) >>
      6);
  char inline_key_size = next_byte_slice[0] & kSizeInlineLimit;
  if (inline_key_size < kSizeInlineLimit) {
    *key_size = inline_key_size;
    *bytes_read = 1;
    return Status::OK();
  } else {
    uint32_t extra_size;
    uint32_t tmp_bytes_read;
    success = file_reader_.ReadVarint32(start_offset + 1, &extra_size,
                                        &tmp_bytes_read);
    if (!success) {
      return file_reader_.status();
    }
    assert(tmp_bytes_read > 0);
    *key_size = kSizeInlineLimit + extra_size;
    *bytes_read = tmp_bytes_read + 1;
    return Status::OK();
  }
}

Status PlainTableKeyEncoder::AppendKey(const Slice& key,
                                       WritableFileWriter* file,
                                       uint64_t* offset, char* meta_bytes_buf,
                                       size_t* meta_bytes_buf_size) {
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(key, &parsed_key)) {
    return Status::Corruption(Slice());
  }

  Slice key_to_write = key;  // Portion of internal key to write out.

  uint32_t user_key_size = static_cast<uint32_t>(key.size() - 8);
  if (encoding_type_ == kPlain) {
    if (fixed_user_key_len_ == kPlainTableVariableLength) {
      // Write key length
      char key_size_buf[5];  // tmp buffer for key size as varint32
      char* ptr = EncodeVarint32(key_size_buf, user_key_size);
      assert(ptr <= key_size_buf + sizeof(key_size_buf));
      auto len = ptr - key_size_buf;
      Status s = file->Append(Slice(key_size_buf, len));
      if (!s.ok()) {
        return s;
      }
      *offset += len;
    }
  } else {
    assert(encoding_type_ == kPrefix);
    char size_bytes[12];
    size_t size_bytes_pos = 0;

    Slice prefix =
        prefix_extractor_->Transform(Slice(key.data(), user_key_size));
    if (key_count_for_prefix_ == 0 || prefix != pre_prefix_.GetUserKey() ||
        key_count_for_prefix_ % index_sparseness_ == 0) {
      key_count_for_prefix_ = 1;
      pre_prefix_.SetUserKey(prefix);
      size_bytes_pos += EncodeSize(kFullKey, user_key_size, size_bytes);
      Status s = file->Append(Slice(size_bytes, size_bytes_pos));
      if (!s.ok()) {
        return s;
      }
      *offset += size_bytes_pos;
    } else {
      key_count_for_prefix_++;
      if (key_count_for_prefix_ == 2) {
        // For second key within a prefix, need to encode prefix length
        size_bytes_pos +=
            EncodeSize(kPrefixFromPreviousKey,
                       static_cast<uint32_t>(pre_prefix_.GetUserKey().size()),
                       size_bytes + size_bytes_pos);
      }
      uint32_t prefix_len =
          static_cast<uint32_t>(pre_prefix_.GetUserKey().size());
      size_bytes_pos += EncodeSize(kKeySuffix, user_key_size - prefix_len,
                                   size_bytes + size_bytes_pos);
      Status s = file->Append(Slice(size_bytes, size_bytes_pos));
      if (!s.ok()) {
        return s;
      }
      *offset += size_bytes_pos;
      key_to_write = Slice(key.data() + prefix_len, key.size() - prefix_len);
    }
  }

  // Encode full key
  // For value size as varint32 (up to 5 bytes).
  // If the row is of value type with seqId 0, flush the special flag together
  // in this buffer to safe one file append call, which takes 1 byte.
  if (parsed_key.sequence == 0 && parsed_key.type == kTypeValue) {
    Status s =
        file->Append(Slice(key_to_write.data(), key_to_write.size() - 8));
    if (!s.ok()) {
      return s;
    }
    *offset += key_to_write.size() - 8;
    meta_bytes_buf[*meta_bytes_buf_size] = PlainTableFactory::kValueTypeSeqId0;
    *meta_bytes_buf_size += 1;
  } else {
    file->Append(key_to_write);
    *offset += key_to_write.size();
  }

  return Status::OK();
}

Slice PlainTableFileReader::GetFromBuffer(Buffer* buffer, uint32_t file_offset,
                                          uint32_t len) {
  assert(file_offset + len <= file_info_->data_end_offset);
  return Slice(buffer->buf.get() + (file_offset - buffer->buf_start_offset),
               len);
}

bool PlainTableFileReader::ReadNonMmap(uint32_t file_offset, uint32_t len,
                                       Slice* out) {
  const uint32_t kPrefetchSize = 256u;

  // Try to read from buffers.
  for (uint32_t i = 0; i < num_buf_; i++) {
    Buffer* buffer = buffers_[num_buf_ - 1 - i].get();
    if (file_offset >= buffer->buf_start_offset &&
        file_offset + len <= buffer->buf_start_offset + buffer->buf_len) {
      *out = GetFromBuffer(buffer, file_offset, len);
      return true;
    }
  }

  Buffer* new_buffer;
  // Data needed is not in any of the buffer. Allocate a new buffer.
  if (num_buf_ < buffers_.size()) {
    // Add a new buffer
    new_buffer = new Buffer();
    buffers_[num_buf_++].reset(new_buffer);
  } else {
    // Now simply replace the last buffer. Can improve the placement policy
    // if needed.
    new_buffer = buffers_[num_buf_ - 1].get();
  }

  assert(file_offset + len <= file_info_->data_end_offset);
  uint32_t size_to_read = std::min(file_info_->data_end_offset - file_offset,
                                   std::max(kPrefetchSize, len));
  if (size_to_read > new_buffer->buf_capacity) {
    new_buffer->buf.reset(new char[size_to_read]);
    new_buffer->buf_capacity = size_to_read;
    new_buffer->buf_len = 0;
  }
  Slice read_result;
  Status s = file_info_->file->Read(file_offset, size_to_read, &read_result,
                                    new_buffer->buf.get());
  if (!s.ok()) {
    status_ = s;
    return false;
  }
  new_buffer->buf_start_offset = file_offset;
  new_buffer->buf_len = size_to_read;
  *out = GetFromBuffer(new_buffer, file_offset, len);
  return true;
}

inline bool PlainTableFileReader::ReadVarint32(uint32_t offset, uint32_t* out,
                                               uint32_t* bytes_read) {
  if (file_info_->is_mmap_mode) {
    const char* start = file_info_->file_data.data() + offset;
    const char* limit =
        file_info_->file_data.data() + file_info_->data_end_offset;
    const char* key_ptr = GetVarint32Ptr(start, limit, out);
    assert(key_ptr != nullptr);
    *bytes_read = static_cast<uint32_t>(key_ptr - start);
    return true;
  } else {
    return ReadVarint32NonMmap(offset, out, bytes_read);
  }
}

bool PlainTableFileReader::ReadVarint32NonMmap(uint32_t offset, uint32_t* out,
                                               uint32_t* bytes_read) {
  const char* start;
  const char* limit;
  const uint32_t kMaxVarInt32Size = 6u;
  uint32_t bytes_to_read =
      std::min(file_info_->data_end_offset - offset, kMaxVarInt32Size);
  Slice bytes;
  if (!Read(offset, bytes_to_read, &bytes)) {
    return false;
  }
  start = bytes.data();
  limit = bytes.data() + bytes.size();

  const char* key_ptr = GetVarint32Ptr(start, limit, out);
  *bytes_read =
      (key_ptr != nullptr) ? static_cast<uint32_t>(key_ptr - start) : 0;
  return true;
}

Status PlainTableKeyDecoder::ReadInternalKey(
    uint32_t file_offset, uint32_t user_key_size, ParsedInternalKey* parsed_key,
    uint32_t* bytes_read, bool* internal_key_valid, Slice* internal_key) {
  Slice tmp_slice;
  bool success = file_reader_.Read(file_offset, user_key_size + 1, &tmp_slice);
  if (!success) {
    return file_reader_.status();
  }
  if (tmp_slice[user_key_size] == PlainTableFactory::kValueTypeSeqId0) {
    // Special encoding for the row with seqID=0
    parsed_key->user_key = Slice(tmp_slice.data(), user_key_size);
    parsed_key->sequence = 0;
    parsed_key->type = kTypeValue;
    *bytes_read += user_key_size + 1;
    *internal_key_valid = false;
  } else {
    success = file_reader_.Read(file_offset, user_key_size + 8, internal_key);
    if (!success) {
      return file_reader_.status();
    }
    *internal_key_valid = true;
    if (!ParseInternalKey(*internal_key, parsed_key)) {
      return Status::Corruption(
          Slice("Incorrect value type found when reading the next key"));
    }
    *bytes_read += user_key_size + 8;
  }
  return Status::OK();
}

Status PlainTableKeyDecoder::NextPlainEncodingKey(uint32_t start_offset,
                                                  ParsedInternalKey* parsed_key,
                                                  Slice* internal_key,
                                                  uint32_t* bytes_read,
                                                  bool* /*seekable*/) {
  uint32_t user_key_size = 0;
  Status s;
  if (fixed_user_key_len_ != kPlainTableVariableLength) {
    user_key_size = fixed_user_key_len_;
  } else {
    uint32_t tmp_size = 0;
    uint32_t tmp_read;
    bool success =
        file_reader_.ReadVarint32(start_offset, &tmp_size, &tmp_read);
    if (!success) {
      return file_reader_.status();
    }
    assert(tmp_read > 0);
    user_key_size = tmp_size;
    *bytes_read = tmp_read;
  }
  // dummy initial value to avoid compiler complain
  bool decoded_internal_key_valid = true;
  Slice decoded_internal_key;
  s = ReadInternalKey(start_offset + *bytes_read, user_key_size, parsed_key,
                      bytes_read, &decoded_internal_key_valid,
                      &decoded_internal_key);
  if (!s.ok()) {
    return s;
  }
  if (!file_reader_.file_info()->is_mmap_mode) {
    cur_key_.SetInternalKey(*parsed_key);
    parsed_key->user_key =
        Slice(cur_key_.GetInternalKey().data(), user_key_size);
    if (internal_key != nullptr) {
      *internal_key = cur_key_.GetInternalKey();
    }
  } else if (internal_key != nullptr) {
    if (decoded_internal_key_valid) {
      *internal_key = decoded_internal_key;
    } else {
      // Need to copy out the internal key
      cur_key_.SetInternalKey(*parsed_key);
      *internal_key = cur_key_.GetInternalKey();
    }
  }
  return Status::OK();
}

Status PlainTableKeyDecoder::NextPrefixEncodingKey(
    uint32_t start_offset, ParsedInternalKey* parsed_key, Slice* internal_key,
    uint32_t* bytes_read, bool* seekable) {
  PlainTableEntryType entry_type;

  bool expect_suffix = false;
  Status s;
  do {
    uint32_t size = 0;
    // dummy initial value to avoid compiler complain
    bool decoded_internal_key_valid = true;
    uint32_t my_bytes_read = 0;
    s = DecodeSize(start_offset + *bytes_read, &entry_type, &size,
                   &my_bytes_read);
    if (!s.ok()) {
      return s;
    }
    if (my_bytes_read == 0) {
      return Status::Corruption("Unexpected EOF when reading size of the key");
    }
    *bytes_read += my_bytes_read;

    switch (entry_type) {
      case kFullKey: {
        expect_suffix = false;
        Slice decoded_internal_key;
        s = ReadInternalKey(start_offset + *bytes_read, size, parsed_key,
                            bytes_read, &decoded_internal_key_valid,
                            &decoded_internal_key);
        if (!s.ok()) {
          return s;
        }
        if (!file_reader_.file_info()->is_mmap_mode ||
            (internal_key != nullptr && !decoded_internal_key_valid)) {
          // In non-mmap mode, always need to make a copy of keys returned to
          // users, because after reading value for the key, the key might
          // be invalid.
          cur_key_.SetInternalKey(*parsed_key);
          saved_user_key_ = cur_key_.GetUserKey();
          if (!file_reader_.file_info()->is_mmap_mode) {
            parsed_key->user_key =
                Slice(cur_key_.GetInternalKey().data(), size);
          }
          if (internal_key != nullptr) {
            *internal_key = cur_key_.GetInternalKey();
          }
        } else {
          if (internal_key != nullptr) {
            *internal_key = decoded_internal_key;
          }
          saved_user_key_ = parsed_key->user_key;
        }
        break;
      }
      case kPrefixFromPreviousKey: {
        if (seekable != nullptr) {
          *seekable = false;
        }
        prefix_len_ = size;
        assert(prefix_extractor_ == nullptr ||
               prefix_extractor_->Transform(saved_user_key_).size() ==
                   prefix_len_);
        // Need read another size flag for suffix
        expect_suffix = true;
        break;
      }
      case kKeySuffix: {
        expect_suffix = false;
        if (seekable != nullptr) {
          *seekable = false;
        }

        Slice tmp_slice;
        s = ReadInternalKey(start_offset + *bytes_read, size, parsed_key,
                            bytes_read, &decoded_internal_key_valid,
                            &tmp_slice);
        if (!s.ok()) {
          return s;
        }
        if (!file_reader_.file_info()->is_mmap_mode) {
          // In non-mmap mode, we need to make a copy of keys returned to
          // users, because after reading value for the key, the key might
          // be invalid.
          // saved_user_key_ points to cur_key_. We are making a copy of
          // the prefix part to another string, and construct the current
          // key from the prefix part and the suffix part back to cur_key_.
          std::string tmp =
              Slice(saved_user_key_.data(), prefix_len_).ToString();
          cur_key_.Reserve(prefix_len_ + size);
          cur_key_.SetInternalKey(tmp, *parsed_key);
          parsed_key->user_key =
              Slice(cur_key_.GetInternalKey().data(), prefix_len_ + size);
          saved_user_key_ = cur_key_.GetUserKey();
        } else {
          cur_key_.Reserve(prefix_len_ + size);
          cur_key_.SetInternalKey(Slice(saved_user_key_.data(), prefix_len_),
                                  *parsed_key);
        }
        parsed_key->user_key = cur_key_.GetUserKey();
        if (internal_key != nullptr) {
          *internal_key = cur_key_.GetInternalKey();
        }
        break;
      }
      default:
        return Status::Corruption("Un-identified size flag.");
    }
  } while (expect_suffix);  // Another round if suffix is expected.
  return Status::OK();
}

Status PlainTableKeyDecoder::NextKey(uint32_t start_offset,
                                     ParsedInternalKey* parsed_key,
                                     Slice* internal_key, Slice* value,
                                     uint32_t* bytes_read, bool* seekable) {
  assert(value != nullptr);
  Status s = NextKeyNoValue(start_offset, parsed_key, internal_key, bytes_read,
                            seekable);
  if (s.ok()) {
    assert(bytes_read != nullptr);
    uint32_t value_size;
    uint32_t value_size_bytes;
    bool success = file_reader_.ReadVarint32(start_offset + *bytes_read,
                                             &value_size, &value_size_bytes);
    if (!success) {
      return file_reader_.status();
    }
    if (value_size_bytes == 0) {
      return Status::Corruption(
          "Unexpected EOF when reading the next value's size.");
    }
    *bytes_read += value_size_bytes;
    success = file_reader_.Read(start_offset + *bytes_read, value_size, value);
    if (!success) {
      return file_reader_.status();
    }
    *bytes_read += value_size;
  }
  return s;
}

Status PlainTableKeyDecoder::NextKeyNoValue(uint32_t start_offset,
                                            ParsedInternalKey* parsed_key,
                                            Slice* internal_key,
                                            uint32_t* bytes_read,
                                            bool* seekable) {
  *bytes_read = 0;
  if (seekable != nullptr) {
    *seekable = true;
  }
  Status s;
  if (encoding_type_ == kPlain) {
    return NextPlainEncodingKey(start_offset, parsed_key, internal_key,
                                bytes_read, seekable);
  } else {
    assert(encoding_type_ == kPrefix);
    return NextPrefixEncodingKey(start_offset, parsed_key, internal_key,
                                 bytes_read, seekable);
  }
}

}  // namespace rocksdb
#endif  // ROCKSDB_LIT
