//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <algorithm>
#include "port/port.h"

namespace rocksdb {

inline size_t TruncateToPageBoundary(size_t page_size, size_t s) {
  s -= (s & (page_size - 1));
  assert((s % page_size) == 0);
  return s;
}

inline size_t Roundup(size_t x, size_t y) {
  return ((x + y - 1) / y) * y;
}

inline size_t Rounddown(size_t x, size_t y) { return (x / y) * y; }

// This class is to manage an aligned user
// allocated buffer for direct I/O purposes
// though can be used for any purpose.
class AlignedBuffer {
  size_t alignment_;
  std::unique_ptr<char[]> buf_;
  size_t capacity_;
  size_t cursize_;
  char* bufstart_;

public:
  AlignedBuffer()
    : alignment_(),
      capacity_(0),
      cursize_(0),
      bufstart_(nullptr) {
  }

  AlignedBuffer(AlignedBuffer&& o) ROCKSDB_NOEXCEPT {
    *this = std::move(o);
  }

  AlignedBuffer& operator=(AlignedBuffer&& o) ROCKSDB_NOEXCEPT {
    alignment_ = std::move(o.alignment_);
    buf_ = std::move(o.buf_);
    capacity_ = std::move(o.capacity_);
    cursize_ = std::move(o.cursize_);
    bufstart_ = std::move(o.bufstart_);
    return *this;
  }

  AlignedBuffer(const AlignedBuffer&) = delete;

  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  static bool isAligned(const void* ptr, size_t alignment) {
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
  }

  static bool isAligned(size_t n, size_t alignment) {
    return n % alignment == 0;
  }

  size_t Alignment() const {
    return alignment_;
  }

  size_t Capacity() const {
    return capacity_;
  }

  size_t CurrentSize() const {
    return cursize_;
  }

  const char* BufferStart() const {
    return bufstart_;
  }

  char* BufferStart() { return bufstart_; }

  void Clear() {
    cursize_ = 0;
  }

  void Alignment(size_t alignment) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0);
    alignment_ = alignment;
  }

  // Allocates a new buffer and sets bufstart_ to the aligned first byte.
  // requested_capacity: requested new buffer capacity. This capacity will be
  //     rounded up based on alignment.
  // copy_data: Copy data from old buffer to new buffer.
  // copy_offset: Copy data from this offset in old buffer.
  // copy_len: Number of bytes to copy.
  void AllocateNewBuffer(size_t requested_capacity, bool copy_data = false,
                         uint64_t copy_offset = 0, size_t copy_len = 0) {
    assert(alignment_ > 0);
    assert((alignment_ & (alignment_ - 1)) == 0);

    copy_len = copy_len > 0 ? copy_len : cursize_;
    if (copy_data && requested_capacity < copy_len) {
      // If we are downsizing to a capacity that is smaller than the current
      // data in the buffer. Ignore the request.
      return;
    }

    size_t new_capacity = Roundup(requested_capacity, alignment_);
    char* new_buf = new char[new_capacity + alignment_];
    char* new_bufstart = reinterpret_cast<char*>(
        (reinterpret_cast<uintptr_t>(new_buf) + (alignment_ - 1)) &
        ~static_cast<uintptr_t>(alignment_ - 1));

    if (copy_data) {
      assert(bufstart_ + copy_offset + copy_len <= bufstart_ + cursize_);
      memcpy(new_bufstart, bufstart_ + copy_offset, copy_len);
      cursize_ = copy_len;
    } else {
      cursize_ = 0;
    }

    bufstart_ = new_bufstart;
    capacity_ = new_capacity;
    buf_.reset(new_buf);
  }
  // Used for write
  // Returns the number of bytes appended
  size_t Append(const char* src, size_t append_size) {
    size_t buffer_remaining = capacity_ - cursize_;
    size_t to_copy = std::min(append_size, buffer_remaining);

    if (to_copy > 0) {
      memcpy(bufstart_ + cursize_, src, to_copy);
      cursize_ += to_copy;
    }
    return to_copy;
  }

  size_t Read(char* dest, size_t offset, size_t read_size) const {
    assert(offset < cursize_);

    size_t to_read = 0;
    if(offset < cursize_) {
      to_read = std::min(cursize_ - offset, read_size);
    }
    if (to_read > 0) {
      memcpy(dest, bufstart_ + offset, to_read);
    }
    return to_read;
  }

  /// Pad to alignment
  void PadToAlignmentWith(int padding) {
    size_t total_size = Roundup(cursize_, alignment_);
    size_t pad_size = total_size - cursize_;

    if (pad_size > 0) {
      assert((pad_size + cursize_) <= capacity_);
      memset(bufstart_ + cursize_, padding, pad_size);
      cursize_ += pad_size;
    }
  }

  void PadWith(size_t pad_size, int padding) {
    assert((pad_size + cursize_) <= capacity_);
    memset(bufstart_ + cursize_, padding, pad_size);
    cursize_ += pad_size;
  }

  // After a partial flush move the tail to the beginning of the buffer
  void RefitTail(size_t tail_offset, size_t tail_size) {
    if (tail_size > 0) {
      memmove(bufstart_, bufstart_ + tail_offset, tail_size);
    }
    cursize_ = tail_size;
  }

  // Returns place to start writing
  char* Destination() {
    return bufstart_ + cursize_;
  }

  void Size(size_t cursize) {
    cursize_ = cursize;
  }
};
}
