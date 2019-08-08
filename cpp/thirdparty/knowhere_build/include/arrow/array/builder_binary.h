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

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/buffer-builder.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"  // IWYU pragma: export

namespace arrow {

constexpr int64_t kBinaryMemoryLimit = std::numeric_limits<int32_t>::max() - 1;

// ----------------------------------------------------------------------
// Binary and String

/// \class BinaryBuilder
/// \brief Builder class for variable-length binary data
class ARROW_EXPORT BinaryBuilder : public ArrayBuilder {
 public:
  explicit BinaryBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  BinaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool);

  Status Append(const uint8_t* value, int32_t length) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    ARROW_RETURN_NOT_OK(AppendNextOffset());
    // Safety check for UBSAN.
    if (ARROW_PREDICT_TRUE(length > 0)) {
      ARROW_RETURN_NOT_OK(value_data_builder_.Append(value, length));
    }

    UnsafeAppendToBitmap(true);
    return Status::OK();
  }

  Status AppendNulls(int64_t length) final {
    const int64_t num_bytes = value_data_builder_.length();
    if (ARROW_PREDICT_FALSE(num_bytes > kBinaryMemoryLimit)) {
      return AppendOverflow(num_bytes);
    }
    ARROW_RETURN_NOT_OK(Reserve(length));
    for (int64_t i = 0; i < length; ++i) {
      offsets_builder_.UnsafeAppend(static_cast<int32_t>(num_bytes));
    }
    UnsafeAppendToBitmap(length, false);
    return Status::OK();
  }

  Status AppendNull() final {
    ARROW_RETURN_NOT_OK(AppendNextOffset());
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  Status Append(const char* value, int32_t length) {
    return Append(reinterpret_cast<const uint8_t*>(value), length);
  }

  Status Append(util::string_view value) {
    return Append(value.data(), static_cast<int32_t>(value.size()));
  }

  /// \brief Append without checking capacity
  ///
  /// Offsets and data should have been presized using Reserve() and
  /// ReserveData(), respectively.
  void UnsafeAppend(const uint8_t* value, int32_t length) {
    UnsafeAppendNextOffset();
    value_data_builder_.UnsafeAppend(value, length);
    UnsafeAppendToBitmap(true);
  }

  void UnsafeAppend(const char* value, int32_t length) {
    UnsafeAppend(reinterpret_cast<const uint8_t*>(value), length);
  }

  void UnsafeAppend(const std::string& value) {
    UnsafeAppend(value.c_str(), static_cast<int32_t>(value.size()));
  }

  void UnsafeAppend(util::string_view value) {
    UnsafeAppend(value.data(), static_cast<int32_t>(value.size()));
  }

  void UnsafeAppendNull() {
    const int64_t num_bytes = value_data_builder_.length();
    offsets_builder_.UnsafeAppend(static_cast<int32_t>(num_bytes));
    UnsafeAppendToBitmap(false);
  }

  void Reset() override;
  Status Resize(int64_t capacity) override;

  /// \brief Ensures there is enough allocated capacity to append the indicated
  /// number of bytes to the value data buffer without additional allocations
  Status ReserveData(int64_t elements);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<BinaryArray>* out) { return FinishTyped(out); }

  /// \return size of values buffer so far
  int64_t value_data_length() const { return value_data_builder_.length(); }
  /// \return capacity of values buffer
  int64_t value_data_capacity() const { return value_data_builder_.capacity(); }

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  const uint8_t* GetValue(int64_t i, int32_t* out_length) const;

  /// Temporary access to a value.
  ///
  /// This view becomes invalid on the next modifying operation.
  util::string_view GetView(int64_t i) const;

 protected:
  TypedBufferBuilder<int32_t> offsets_builder_;
  TypedBufferBuilder<uint8_t> value_data_builder_;

  Status AppendOverflow(int64_t num_bytes);

  Status AppendNextOffset() {
    const int64_t num_bytes = value_data_builder_.length();
    if (ARROW_PREDICT_FALSE(num_bytes > kBinaryMemoryLimit)) {
      return AppendOverflow(num_bytes);
    }
    return offsets_builder_.Append(static_cast<int32_t>(num_bytes));
  }

  void UnsafeAppendNextOffset() {
    const int64_t num_bytes = value_data_builder_.length();
    offsets_builder_.UnsafeAppend(static_cast<int32_t>(num_bytes));
  }
};

/// \class StringBuilder
/// \brief Builder class for UTF8 strings
class ARROW_EXPORT StringBuilder : public BinaryBuilder {
 public:
  using BinaryBuilder::BinaryBuilder;
  explicit StringBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using BinaryBuilder::Append;
  using BinaryBuilder::Reset;
  using BinaryBuilder::UnsafeAppend;

  /// \brief Append a sequence of strings in one shot.
  ///
  /// \param[in] values a vector of strings
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const std::vector<std::string>& values,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Append a sequence of nul-terminated strings in one shot.
  ///        If one of the values is NULL, it is processed as a null
  ///        value even if the corresponding valid_bytes entry is 1.
  ///
  /// \param[in] values a contiguous C array of nul-terminated char *
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const char** values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<StringArray>* out) { return FinishTyped(out); }
};

// ----------------------------------------------------------------------
// FixedSizeBinaryBuilder

class ARROW_EXPORT FixedSizeBinaryBuilder : public ArrayBuilder {
 public:
  FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                         MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  Status Append(const uint8_t* value) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(value);
    return Status::OK();
  }

  Status Append(const char* value) {
    return Append(reinterpret_cast<const uint8_t*>(value));
  }

  Status Append(const util::string_view& view) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(view);
    return Status::OK();
  }

  Status Append(const std::string& s) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(s);
    return Status::OK();
  }

  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& value) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(
        util::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
    return Status::OK();
  }

  Status AppendValues(const uint8_t* data, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  Status AppendNull() final;

  Status AppendNulls(int64_t length) final;

  void UnsafeAppend(const uint8_t* value) {
    UnsafeAppendToBitmap(true);
    if (ARROW_PREDICT_TRUE(byte_width_ > 0)) {
      byte_builder_.UnsafeAppend(value, byte_width_);
    }
  }

  void UnsafeAppend(util::string_view value) {
#ifndef NDEBUG
    CheckValueSize(static_cast<size_t>(value.size()));
#endif
    UnsafeAppend(reinterpret_cast<const uint8_t*>(value.data()));
  }

  void UnsafeAppendNull() {
    UnsafeAppendToBitmap(false);
    byte_builder_.UnsafeAdvance(byte_width_);
  }

  void Reset() override;
  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<FixedSizeBinaryArray>* out) { return FinishTyped(out); }

  /// \return size of values buffer so far
  int64_t value_data_length() const { return byte_builder_.length(); }

  int32_t byte_width() const { return byte_width_; }

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  const uint8_t* GetValue(int64_t i) const;

  /// Temporary access to a value.
  ///
  /// This view becomes invalid on the next modifying operation.
  util::string_view GetView(int64_t i) const;

 protected:
  int32_t byte_width_;
  BufferBuilder byte_builder_;

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  uint8_t* GetMutableValue(int64_t i) {
    uint8_t* data_ptr = byte_builder_.mutable_data();
    return data_ptr + i * byte_width_;
  }

#ifndef NDEBUG
  void CheckValueSize(int64_t size);
#endif
};

// ----------------------------------------------------------------------
// Chunked builders: build a sequence of BinaryArray or StringArray that are
// limited to a particular size (to the upper limit of 2GB)

namespace internal {

class ARROW_EXPORT ChunkedBinaryBuilder {
 public:
  ChunkedBinaryBuilder(int32_t max_chunk_value_length,
                       MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  ChunkedBinaryBuilder(int32_t max_chunk_value_length, int32_t max_chunk_length,
                       MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  virtual ~ChunkedBinaryBuilder() = default;

  Status Append(const uint8_t* value, int32_t length) {
    if (ARROW_PREDICT_FALSE(length + builder_->value_data_length() >
                            max_chunk_value_length_)) {
      if (builder_->value_data_length() == 0) {
        // The current item is larger than max_chunk_size_;
        // this chunk will be oversize and hold *only* this item
        ARROW_RETURN_NOT_OK(builder_->Append(value, length));
        return NextChunk();
      }
      // The current item would cause builder_->value_data_length() to exceed
      // max_chunk_size_, so finish this chunk and append the current item to the next
      // chunk
      ARROW_RETURN_NOT_OK(NextChunk());
      return Append(value, length);
    }

    if (ARROW_PREDICT_FALSE(builder_->length() == max_chunk_length_)) {
      // The current item would cause builder_->value_data_length() to exceed
      // max_chunk_size_, so finish this chunk and append the current item to the next
      // chunk
      ARROW_RETURN_NOT_OK(NextChunk());
    }

    return builder_->Append(value, length);
  }

  Status Append(const util::string_view& value) {
    return Append(reinterpret_cast<const uint8_t*>(value.data()),
                  static_cast<int32_t>(value.size()));
  }

  Status AppendNull() {
    if (ARROW_PREDICT_FALSE(builder_->length() == max_chunk_length_)) {
      ARROW_RETURN_NOT_OK(NextChunk());
    }
    return builder_->AppendNull();
  }

  Status Reserve(int64_t values);

  virtual Status Finish(ArrayVector* out);

 protected:
  Status NextChunk();

  // maximum total character data size per chunk
  int64_t max_chunk_value_length_;

  // maximum elements allowed per chunk
  int64_t max_chunk_length_ = kListMaximumElements;

  // when Reserve() would cause builder_ to exceed its max_chunk_length_,
  // add to extra_capacity_ instead and wait to reserve until the next chunk
  int64_t extra_capacity_ = 0;

  std::unique_ptr<BinaryBuilder> builder_;
  std::vector<std::shared_ptr<Array>> chunks_;
};

class ARROW_EXPORT ChunkedStringBuilder : public ChunkedBinaryBuilder {
 public:
  using ChunkedBinaryBuilder::ChunkedBinaryBuilder;

  Status Finish(ArrayVector* out) override;
};

}  // namespace internal

}  // namespace arrow
