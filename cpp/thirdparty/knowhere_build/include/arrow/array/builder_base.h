// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>  // IWYU pragma: keep
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/buffer-builder.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
struct ArrayData;
class MemoryPool;

constexpr int64_t kMinBuilderCapacity = 1 << 5;
constexpr int64_t kListMaximumElements = std::numeric_limits<int32_t>::max() - 1;

/// Base class for all data array builders.
///
/// This class provides a facilities for incrementally building the null bitmap
/// (see Append methods) and as a side effect the current number of slots and
/// the null count.
///
/// \note Users are expected to use builders as one of the concrete types below.
/// For example, ArrayBuilder* pointing to BinaryBuilder should be downcast before use.
class ARROW_EXPORT ArrayBuilder {
 public:
  explicit ArrayBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type), pool_(pool), null_bitmap_builder_(pool) {}

  virtual ~ArrayBuilder() = default;

  /// For nested types. Since the objects are owned by this class instance, we
  /// skip shared pointers and just return a raw pointer
  ArrayBuilder* child(int i) { return children_[i].get(); }

  int num_children() const { return static_cast<int>(children_.size()); }

  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }
  int64_t capacity() const { return capacity_; }

  /// \brief Ensure that enough memory has been allocated to fit the indicated
  /// number of total elements in the builder, including any that have already
  /// been appended. Does not account for reallocations that may be due to
  /// variable size data, like binary values. To make space for incremental
  /// appends, use Reserve instead.
  ///
  /// \param[in] capacity the minimum number of total array values to
  ///            accommodate. Must be greater than the current capacity.
  /// \return Status
  virtual Status Resize(int64_t capacity);

  /// \brief Ensure that there is enough space allocated to add the indicated
  /// number of elements without any further calls to Resize. Overallocation is
  /// used in order to minimize the impact of incremental Reserve() calls.
  ///
  /// \param[in] additional_capacity the number of additional array values
  /// \return Status
  Status Reserve(int64_t additional_capacity) {
    auto current_capacity = capacity();
    auto min_capacity = length() + additional_capacity;
    if (min_capacity <= current_capacity) return Status::OK();

    // leave growth factor up to BufferBuilder
    auto new_capacity = BufferBuilder::GrowByFactor(current_capacity, min_capacity);
    return Resize(new_capacity);
  }

  /// Reset the builder.
  virtual void Reset();

  virtual Status AppendNull() = 0;
  virtual Status AppendNulls(int64_t length) = 0;

  /// For cases where raw data was memcpy'd into the internal buffers, allows us
  /// to advance the length of the builder. It is your responsibility to use
  /// this function responsibly.
  Status Advance(int64_t elements);

  /// \brief Return result of builder as an internal generic ArrayData
  /// object. Resets builder except for dictionary builder
  ///
  /// \param[out] out the finalized ArrayData object
  /// \return Status
  virtual Status FinishInternal(std::shared_ptr<ArrayData>* out) = 0;

  /// \brief Return result of builder as an Array object.
  ///
  /// The builder is reset except for DictionaryBuilder.
  ///
  /// \param[out] out the finalized Array object
  /// \return Status
  Status Finish(std::shared_ptr<Array>* out);

  std::shared_ptr<DataType> type() const { return type_; }

 protected:
  /// Append to null bitmap
  Status AppendToBitmap(bool is_valid);

  /// Vector append. Treat each zero byte as a null.   If valid_bytes is null
  /// assume all of length bits are valid.
  Status AppendToBitmap(const uint8_t* valid_bytes, int64_t length);

  /// Uniform append.  Append N times the same validity bit.
  Status AppendToBitmap(int64_t num_bits, bool value);

  /// Set the next length bits to not null (i.e. valid).
  Status SetNotNull(int64_t length);

  // Unsafe operations (don't check capacity/don't resize)

  void UnsafeAppendNull() { UnsafeAppendToBitmap(false); }

  // Append to null bitmap, update the length
  void UnsafeAppendToBitmap(bool is_valid) {
    null_bitmap_builder_.UnsafeAppend(is_valid);
    ++length_;
    if (!is_valid) ++null_count_;
  }

  // Vector append. Treat each zero byte as a nullzero. If valid_bytes is null
  // assume all of length bits are valid.
  void UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
    if (valid_bytes == NULLPTR) {
      return UnsafeSetNotNull(length);
    }
    null_bitmap_builder_.UnsafeAppend(valid_bytes, length);
    length_ += length;
    null_count_ = null_bitmap_builder_.false_count();
  }

  // Append the same validity value a given number of times.
  void UnsafeAppendToBitmap(const int64_t num_bits, bool value) {
    if (value) {
      UnsafeSetNotNull(num_bits);
    } else {
      UnsafeSetNull(num_bits);
    }
  }

  void UnsafeAppendToBitmap(const std::vector<bool>& is_valid);

  // Set the next validity bits to not null (i.e. valid).
  void UnsafeSetNotNull(int64_t length);

  // Set the next validity bits to null (i.e. invalid).
  void UnsafeSetNull(int64_t length);

  static Status TrimBuffer(const int64_t bytes_filled, ResizableBuffer* buffer);

  /// \brief Finish to an array of the specified ArrayType
  template <typename ArrayType>
  Status FinishTyped(std::shared_ptr<ArrayType>* out) {
    std::shared_ptr<Array> out_untyped;
    ARROW_RETURN_NOT_OK(Finish(&out_untyped));
    *out = std::static_pointer_cast<ArrayType>(std::move(out_untyped));
    return Status::OK();
  }

  static Status CheckCapacity(int64_t new_capacity, int64_t old_capacity) {
    if (new_capacity < 0) {
      return Status::Invalid("Resize capacity must be positive");
    }

    if (new_capacity < old_capacity) {
      return Status::Invalid("Resize cannot downsize");
    }

    return Status::OK();
  }

  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;

  TypedBufferBuilder<bool> null_bitmap_builder_;
  int64_t null_count_ = 0;

  // Array length, so far. Also, the index of the next element to be added
  int64_t length_ = 0;
  int64_t capacity_ = 0;

  // Child value array builders. These are owned by this class
  std::vector<std::shared_ptr<ArrayBuilder>> children_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ArrayBuilder);
};

}  // namespace arrow
