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

#include <memory>

#include "arrow/array/builder_base.h"

namespace arrow {

namespace internal {

class ARROW_EXPORT AdaptiveIntBuilderBase : public ArrayBuilder {
 public:
  explicit AdaptiveIntBuilderBase(MemoryPool* pool);

  /// \brief Append multiple nulls
  /// \param[in] length the number of nulls to append
  Status AppendNulls(int64_t length) final {
    ARROW_RETURN_NOT_OK(CommitPendingData());
    ARROW_RETURN_NOT_OK(Reserve(length));
    memset(data_->mutable_data() + length_ * int_size_, 0, int_size_ * length);
    UnsafeSetNull(length);
    return Status::OK();
  }

  Status AppendNull() final {
    pending_data_[pending_pos_] = 0;
    pending_valid_[pending_pos_] = 0;
    pending_has_nulls_ = true;
    ++pending_pos_;

    if (ARROW_PREDICT_FALSE(pending_pos_ >= pending_size_)) {
      return CommitPendingData();
    }
    return Status::OK();
  }

  void Reset() override;
  Status Resize(int64_t capacity) override;

 protected:
  virtual Status CommitPendingData() = 0;

  std::shared_ptr<ResizableBuffer> data_;
  uint8_t* raw_data_;
  uint8_t int_size_;

  static constexpr int32_t pending_size_ = 1024;
  uint8_t pending_valid_[pending_size_];
  uint64_t pending_data_[pending_size_];
  int32_t pending_pos_;
  bool pending_has_nulls_;
};

}  // namespace internal

class ARROW_EXPORT AdaptiveUIntBuilder : public internal::AdaptiveIntBuilderBase {
 public:
  explicit AdaptiveUIntBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using ArrayBuilder::Advance;
  using internal::AdaptiveIntBuilderBase::Reset;

  /// Scalar append
  Status Append(const uint64_t val) {
    pending_data_[pending_pos_] = val;
    pending_valid_[pending_pos_] = 1;
    ++pending_pos_;

    if (ARROW_PREDICT_FALSE(pending_pos_ >= pending_size_)) {
      return CommitPendingData();
    }
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const uint64_t* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

 protected:
  Status CommitPendingData() override;
  Status ExpandIntSize(uint8_t new_int_size);

  Status AppendValuesInternal(const uint64_t* values, int64_t length,
                              const uint8_t* valid_bytes);

  template <typename new_type, typename old_type>
  typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
  ExpandIntSizeInternal();
#define __LESS(a, b) (a) < (b)
  template <typename new_type, typename old_type>
  typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
  ExpandIntSizeInternal();
#undef __LESS

  template <typename new_type>
  Status ExpandIntSizeN();
};

class ARROW_EXPORT AdaptiveIntBuilder : public internal::AdaptiveIntBuilderBase {
 public:
  explicit AdaptiveIntBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using ArrayBuilder::Advance;
  using internal::AdaptiveIntBuilderBase::Reset;

  /// Scalar append
  Status Append(const int64_t val) {
    auto v = static_cast<uint64_t>(val);

    pending_data_[pending_pos_] = v;
    pending_valid_[pending_pos_] = 1;
    ++pending_pos_;

    if (ARROW_PREDICT_FALSE(pending_pos_ >= pending_size_)) {
      return CommitPendingData();
    }
    return Status::OK();
  }

  /// \brief Append a sequence of elements in one shot
  /// \param[in] values a contiguous C array of values
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const int64_t* values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

 protected:
  Status CommitPendingData() override;
  Status ExpandIntSize(uint8_t new_int_size);

  Status AppendValuesInternal(const int64_t* values, int64_t length,
                              const uint8_t* valid_bytes);

  template <typename new_type, typename old_type>
  typename std::enable_if<sizeof(old_type) >= sizeof(new_type), Status>::type
  ExpandIntSizeInternal();
#define __LESS(a, b) (a) < (b)
  template <typename new_type, typename old_type>
  typename std::enable_if<__LESS(sizeof(old_type), sizeof(new_type)), Status>::type
  ExpandIntSizeInternal();
#undef __LESS

  template <typename new_type>
  Status ExpandIntSizeN();
};

}  // namespace arrow
