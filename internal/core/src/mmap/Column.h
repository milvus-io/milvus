// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <folly/io/IOBuf.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <queue>
#include <string>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/File.h"
#include "common/FieldMeta.h"
#include "common/FieldData.h"
#include "common/Span.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "mmap/Utils.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Array.h"
#include "knowhere/dataset.h"
#include "storage/prometheus_client.h"

namespace milvus {

/*
* If string field's value all empty, need a string padding to avoid
* mmap failing because size_ is zero which causing invalid arguement
* array has the same problem
* TODO: remove it when support NULL value
*/
constexpr size_t STRING_PADDING = 1;
constexpr size_t ARRAY_PADDING = 1;

class ColumnBase {
 public:
    // memory mode ctor
    ColumnBase(size_t reserve, const FieldMeta& field_meta)
        : type_size_(IsSparseFloatVectorDataType(field_meta.get_data_type())
                         ? 1
                         : field_meta.get_sizeof()),
          is_map_anonymous_(true) {
        SetPaddingSize(field_meta.get_data_type());

        if (IsVariableDataType(field_meta.get_data_type())) {
            return;
        }

        cap_size_ = type_size_ * reserve;

        // use anon mapping so we are able to free these memory with munmap only
        size_t mapped_size = cap_size_ + padding_;
        data_ = static_cast<char*>(mmap(nullptr,
                                        mapped_size,
                                        PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANON,
                                        -1,
                                        0));
        AssertInfo(data_ != MAP_FAILED,
                   "failed to create anon map: {}, map_size={}",
                   strerror(errno),
                   mapped_size);

        UpdateMetricWhenMmap(mapped_size);
    }

    // mmap mode ctor
    // User must call Seal to build the view for variable length column.
    ColumnBase(const File& file, size_t size, const FieldMeta& field_meta)
        : type_size_(IsSparseFloatVectorDataType(field_meta.get_data_type())
                         ? 1
                         : field_meta.get_sizeof()),
          is_map_anonymous_(false),
          num_rows_(size / type_size_) {
        SetPaddingSize(field_meta.get_data_type());

        size_ = size;
        cap_size_ = size;
        size_t mapped_size = cap_size_ + padding_;
        data_ = static_cast<char*>(mmap(
            nullptr, mapped_size, PROT_READ, MAP_SHARED, file.Descriptor(), 0));
        AssertInfo(data_ != MAP_FAILED,
                   "failed to create file-backed map, err: {}",
                   strerror(errno));
        madvise(data_, mapped_size, MADV_WILLNEED);

        UpdateMetricWhenMmap(mapped_size);
    }

    // mmap mode ctor
    // User must call Seal to build the view for variable length column.
    ColumnBase(const File& file,
               size_t size,
               int dim,
               const DataType& data_type)
        : type_size_(IsSparseFloatVectorDataType(data_type)
                         ? 1
                         : GetDataTypeSize(data_type, dim)),
          num_rows_(
              IsSparseFloatVectorDataType(data_type) ? 1 : (size / type_size_)),
          size_(size),
          cap_size_(size),
          is_map_anonymous_(false) {
        SetPaddingSize(data_type);

        size_t mapped_size = cap_size_ + padding_;
        data_ = static_cast<char*>(mmap(
            nullptr, mapped_size, PROT_READ, MAP_SHARED, file.Descriptor(), 0));
        AssertInfo(data_ != MAP_FAILED,
                   "failed to create file-backed map, err: {}",
                   strerror(errno));

        UpdateMetricWhenMmap(mapped_size);
    }

    virtual ~ColumnBase() {
        if (data_ != nullptr) {
            size_t mapped_size = cap_size_ + padding_;
            if (munmap(data_, mapped_size)) {
                AssertInfo(true,
                           "failed to unmap variable field, err={}",
                           strerror(errno));
            }
            UpdateMetricWhenMunmap(mapped_size);
        }
    }

    ColumnBase(ColumnBase&& column) noexcept
        : data_(column.data_),
          cap_size_(column.cap_size_),
          padding_(column.padding_),
          type_size_(column.type_size_),
          num_rows_(column.num_rows_),
          size_(column.size_) {
        column.data_ = nullptr;
        column.cap_size_ = 0;
        column.padding_ = 0;
        column.num_rows_ = 0;
        column.size_ = 0;
    }

    // Data() points at an addr that contains the elements
    virtual const char*
    Data() const {
        return data_;
    }

    // MmappedData() returns the mmaped address
    const char*
    MmappedData() const {
        return data_;
    }

    size_t
    NumRows() const {
        return num_rows_;
    };

    virtual size_t
    ByteSize() const {
        return cap_size_ + padding_;
    }

    // The capacity of the column,
    // DO NOT call this for variable length column(including SparseFloatColumn).
    virtual size_t
    Capacity() const {
        return cap_size_ / type_size_;
    }

    virtual SpanBase
    Span() const = 0;

    virtual void
    AppendBatch(const FieldDataPtr data) {
        size_t required_size = size_ + data->Size();
        if (required_size > cap_size_) {
            Expand(required_size * 2 + padding_);
        }

        std::copy_n(static_cast<const char*>(data->Data()),
                    data->Size(),
                    data_ + size_);
        size_ = required_size;
        num_rows_ += data->Length();
    }

    // Append one row
    virtual void
    Append(const char* data, size_t size) {
        size_t required_size = size_ + size;
        if (required_size > cap_size_) {
            Expand(required_size * 2);
        }

        std::copy_n(data, size, data_ + size_);
        size_ = required_size;
        num_rows_++;
    }

    void
    SetPaddingSize(const DataType& type) {
        switch (type) {
            case DataType::JSON:
                // simdjson requires a padding following the json data
                padding_ = simdjson::SIMDJSON_PADDING;
                break;
            case DataType::VARCHAR:
            case DataType::STRING:
                padding_ = STRING_PADDING;
                break;
            case DataType::ARRAY:
                padding_ = ARRAY_PADDING;
                break;
            default:
                padding_ = 0;
                break;
        }
    }

 protected:
    // only for memory mode, not mmap
    void
    Expand(size_t new_size) {
        if (new_size == 0) {
            return;
        }

        size_t new_mapped_size = new_size + padding_;
        auto data = static_cast<char*>(mmap(nullptr,
                                            new_mapped_size,
                                            PROT_READ | PROT_WRITE,
                                            MAP_PRIVATE | MAP_ANON,
                                            -1,
                                            0));
        UpdateMetricWhenMmap(true, new_mapped_size);

        AssertInfo(data != MAP_FAILED,
                   "failed to expand map: {}, new_map_size={}",
                   strerror(errno),
                   new_size + padding_);

        if (data_ != nullptr) {
            std::memcpy(data, data_, size_);
            if (munmap(data_, cap_size_ + padding_)) {
                auto err = errno;
                size_t mapped_size = new_size + padding_;
                munmap(data, mapped_size);
                UpdateMetricWhenMunmap(mapped_size);

                AssertInfo(
                    false,
                    "failed to unmap while expanding: {}, old_map_size={}",
                    strerror(err),
                    cap_size_ + padding_);
            }
            UpdateMetricWhenMunmap(cap_size_ + padding_);
        }

        data_ = data;
        cap_size_ = new_size;
        is_map_anonymous_ = true;
    }

    char* data_{nullptr};
    // capacity in bytes
    size_t cap_size_{0};
    size_t padding_{0};
    // type_size_ is not used for sparse float vector column.
    const size_t type_size_{1};
    size_t num_rows_{0};

    // length in bytes
    size_t size_{0};

 private:
    void
    UpdateMetricWhenMmap(size_t mmaped_size) {
        UpdateMetricWhenMmap(is_map_anonymous_, mmaped_size);
    }

    void
    UpdateMetricWhenMmap(bool is_map_anonymous, size_t mapped_size) {
        if (is_map_anonymous) {
            milvus::storage::internal_mmap_allocated_space_bytes_anon.Observe(
                mapped_size);
            milvus::storage::internal_mmap_in_used_space_bytes_anon.Increment(
                mapped_size);
        } else {
            milvus::storage::internal_mmap_allocated_space_bytes_file.Observe(
                mapped_size);
            milvus::storage::internal_mmap_in_used_space_bytes_file.Increment(
                mapped_size);
        }
    }

    void
    UpdateMetricWhenMunmap(size_t mapped_size) {
        if (is_map_anonymous_) {
            milvus::storage::internal_mmap_in_used_space_bytes_anon.Decrement(
                mapped_size);
        } else {
            milvus::storage::internal_mmap_in_used_space_bytes_file.Decrement(
                mapped_size);
        }
    }

 private:
    // is MAP_ANONYMOUS
    bool is_map_anonymous_;
};

class Column : public ColumnBase {
 public:
    // memory mode ctor
    Column(size_t cap, const FieldMeta& field_meta)
        : ColumnBase(cap, field_meta) {
    }

    // mmap mode ctor
    Column(const File& file, size_t size, const FieldMeta& field_meta)
        : ColumnBase(file, size, field_meta) {
    }

    // mmap mode ctor
    Column(const File& file, size_t size, int dim, DataType data_type)
        : ColumnBase(file, size, dim, data_type) {
    }

    Column(Column&& column) noexcept : ColumnBase(std::move(column)) {
    }

    ~Column() override = default;

    SpanBase
    Span() const override {
        return SpanBase(data_, num_rows_, cap_size_ / num_rows_);
    }
};

// when mmap is used, size_, data_ and num_rows_ of ColumnBase are used.
class SparseFloatColumn : public ColumnBase {
 public:
    // memory mode ctor
    SparseFloatColumn(const FieldMeta& field_meta) : ColumnBase(0, field_meta) {
    }
    // mmap mode ctor
    SparseFloatColumn(const File& file,
                      size_t size,
                      const FieldMeta& field_meta)
        : ColumnBase(file, size, field_meta) {
    }
    // mmap mode ctor
    SparseFloatColumn(const File& file,
                      size_t size,
                      int dim,
                      const DataType& data_type)
        : ColumnBase(file, size, dim, data_type) {
    }

    SparseFloatColumn(SparseFloatColumn&& column) noexcept
        : ColumnBase(std::move(column)),
          dim_(column.dim_),
          vec_(std::move(column.vec_)) {
    }

    ~SparseFloatColumn() override = default;

    const char*
    Data() const override {
        return static_cast<const char*>(static_cast<const void*>(vec_.data()));
    }

    size_t
    Capacity() const override {
        throw std::runtime_error(
            "Capacity not supported for sparse float column");
    }

    SpanBase
    Span() const override {
        throw std::runtime_error("Span not supported for sparse float column");
    }

    void
    AppendBatch(const FieldDataPtr data) override {
        auto ptr = static_cast<const knowhere::sparse::SparseRow<float>*>(
            data->Data());
        vec_.insert(vec_.end(), ptr, ptr + data->Length());
        for (size_t i = 0; i < data->Length(); ++i) {
            dim_ = std::max(dim_, ptr[i].dim());
        }
        num_rows_ += data->Length();
    }

    void
    Append(const char* data, size_t size) override {
        throw std::runtime_error(
            "Append not supported for sparse float column");
    }

    int64_t
    Dim() const {
        return dim_;
    }

    void
    Seal(std::vector<uint64_t> indices) {
        AssertInfo(!indices.empty(),
                   "indices should not be empty, Seal() of "
                   "SparseFloatColumn must be called only "
                   "at mmap mode");
        AssertInfo(data_,
                   "data_ should not be nullptr, Seal() of "
                   "SparseFloatColumn must be called only "
                   "at mmap mode");
        num_rows_ = indices.size();
        // so that indices[num_rows_] - indices[num_rows_ - 1] is the size of
        // the last row.
        indices.push_back(size_);
        for (size_t i = 0; i < num_rows_; i++) {
            auto vec_size = indices[i + 1] - indices[i];
            AssertInfo(
                vec_size % knowhere::sparse::SparseRow<float>::element_size() ==
                    0,
                "Incorrect sparse vector size: {}",
                vec_size);
            vec_.emplace_back(
                vec_size / knowhere::sparse::SparseRow<float>::element_size(),
                (uint8_t*)(data_) + indices[i],
                false);
        }
    }

 private:
    int64_t dim_ = 0;
    std::vector<knowhere::sparse::SparseRow<float>> vec_;
};

template <typename T>
class VariableColumn : public ColumnBase {
 public:
    using ViewType =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    // memory mode ctor
    VariableColumn(size_t cap, const FieldMeta& field_meta)
        : ColumnBase(cap, field_meta) {
    }

    // mmap mode ctor
    VariableColumn(const File& file, size_t size, const FieldMeta& field_meta)
        : ColumnBase(file, size, field_meta) {
    }

    VariableColumn(VariableColumn&& column) noexcept
        : ColumnBase(std::move(column)),
          indices_(std::move(column.indices_)),
          views_(std::move(column.views_)) {
    }

    ~VariableColumn() override = default;

    SpanBase
    Span() const override {
        return SpanBase(views_.data(), views_.size(), sizeof(ViewType));
    }

    [[nodiscard]] const std::vector<ViewType>&
    Views() const {
        return views_;
    }

    ViewType
    operator[](const int i) const {
        return views_[i];
    }

    std::string_view
    RawAt(const int i) const {
        return std::string_view(views_[i]);
    }

    void
    Append(FieldDataPtr chunk) {
        for (auto i = 0; i < chunk->get_num_rows(); i++) {
            auto data = static_cast<const T*>(chunk->RawValue(i));

            indices_.emplace_back(size_);
            size_ += data->size();
        }
        load_buf_.emplace(std::move(chunk));
    }

    void
    Seal(std::vector<uint64_t> indices = {}) {
        if (!indices.empty()) {
            indices_ = std::move(indices);
        }

        num_rows_ = indices_.size();

        // for variable length column in memory mode only
        if (data_ == nullptr) {
            size_t total_size = size_;
            size_ = 0;
            Expand(total_size);

            while (!load_buf_.empty()) {
                auto chunk = std::move(load_buf_.front());
                load_buf_.pop();

                for (auto i = 0; i < chunk->get_num_rows(); i++) {
                    auto data = static_cast<const T*>(chunk->RawValue(i));
                    std::copy_n(data->c_str(), data->size(), data_ + size_);
                    size_ += data->size();
                }
            }
        }

        ConstructViews();

        // Not need indices_ after
        indices_.clear();
        std::vector<uint64_t>().swap(indices_);
    }

 protected:
    void
    ConstructViews() {
        views_.reserve(indices_.size());
        for (size_t i = 0; i < indices_.size() - 1; i++) {
            views_.emplace_back(data_ + indices_[i],
                                indices_[i + 1] - indices_[i]);
        }
        views_.emplace_back(data_ + indices_.back(), size_ - indices_.back());
    }

 private:
    // loading states
    std::queue<FieldDataPtr> load_buf_{};

    std::vector<uint64_t> indices_{};

    // Compatible with current Span type
    std::vector<ViewType> views_{};
};

class ArrayColumn : public ColumnBase {
 public:
    // memory mode ctor
    ArrayColumn(size_t num_rows, const FieldMeta& field_meta)
        : ColumnBase(num_rows, field_meta),
          element_type_(field_meta.get_element_type()) {
    }

    // mmap mode ctor
    ArrayColumn(const File& file, size_t size, const FieldMeta& field_meta)
        : ColumnBase(file, size, field_meta),
          element_type_(field_meta.get_element_type()) {
    }

    ArrayColumn(ArrayColumn&& column) noexcept
        : ColumnBase(std::move(column)),
          indices_(std::move(column.indices_)),
          views_(std::move(column.views_)),
          element_type_(column.element_type_) {
    }

    ~ArrayColumn() override = default;

    SpanBase
    Span() const override {
        return SpanBase(views_.data(), views_.size(), sizeof(ArrayView));
    }

    [[nodiscard]] const std::vector<ArrayView>&
    Views() const {
        return views_;
    }

    ArrayView
    operator[](const int i) const {
        return views_[i];
    }

    ScalarArray
    RawAt(const int i) const {
        return views_[i].output_data();
    }

    void
    Append(const Array& array) {
        indices_.emplace_back(size_);
        element_indices_.emplace_back(array.get_offsets());
        ColumnBase::Append(static_cast<const char*>(array.data()),
                           array.byte_size());
    }

    void
    Seal(std::vector<uint64_t>&& indices = {},
         std::vector<std::vector<uint64_t>>&& element_indices = {}) {
        if (!indices.empty()) {
            indices_ = std::move(indices);
            element_indices_ = std::move(element_indices);
        }
        ConstructViews();
    }

 protected:
    void
    ConstructViews() {
        views_.reserve(indices_.size());
        for (size_t i = 0; i < indices_.size() - 1; i++) {
            views_.emplace_back(data_ + indices_[i],
                                indices_[i + 1] - indices_[i],
                                element_type_,
                                std::move(element_indices_[i]));
        }
        views_.emplace_back(data_ + indices_.back(),
                            size_ - indices_.back(),
                            element_type_,
                            std::move(element_indices_[indices_.size() - 1]));
        element_indices_.clear();
    }

 private:
    std::vector<uint64_t> indices_{};
    std::vector<std::vector<uint64_t>> element_indices_{};
    // Compatible with current Span type
    std::vector<ArrayView> views_{};
    DataType element_type_;
};
}  // namespace milvus
