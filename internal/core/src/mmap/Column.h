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

#include <sys/mman.h>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <filesystem>

#include "common/FieldMeta.h"
#include "common/Span.h"
#include "common/EasyAssert.h"
#include "common/File.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "mmap/Utils.h"
#include "storage/FieldData.h"
#include "common/Array.h"

namespace milvus {

static int mmap_flags = MAP_SHARED;

class ColumnBase {
 public:
    // memory mode ctor
    ColumnBase(size_t reserve, const FieldMeta& field_meta)
        : type_size_(field_meta.get_sizeof()) {
        // simdjson requires a padding following the json data
        padding_ = field_meta.get_data_type() == DataType::JSON
                       ? simdjson::SIMDJSON_PADDING
                       : 0;

        if (datatype_is_variable(field_meta.get_data_type())) {
            return;
        }

        cap_size_ = field_meta.get_sizeof() * reserve;
        auto data_type = field_meta.get_data_type();

        // use anon mapping so we are able to free these memory with munmap only
        data_ = static_cast<char*>(mmap(nullptr,
                                        cap_size_ + padding_,
                                        PROT_READ | PROT_WRITE,
                                        mmap_flags | MAP_ANON,
                                        -1,
                                        0));
        AssertInfo(
            data_ != MAP_FAILED,
            fmt::format("failed to create anon map, err: {}", strerror(errno)));
    }

    // mmap mode ctor
    ColumnBase(const File& file, size_t size, const FieldMeta& field_meta)
        : type_size_(field_meta.get_sizeof()),
          num_rows_(size / field_meta.get_sizeof()) {
        padding_ = field_meta.get_data_type() == DataType::JSON
                       ? simdjson::SIMDJSON_PADDING
                       : 0;

        size_ = size;
        cap_size_ = size;
        data_ = static_cast<char*>(mmap(nullptr,
                                        cap_size_ + padding_,
                                        PROT_READ,
                                        mmap_flags,
                                        file.Descriptor(),
                                        0));
        AssertInfo(data_ != MAP_FAILED,
                   fmt::format("failed to create file-backed map, err: {}",
                               strerror(errno)));
        madvise(data_, cap_size_ + padding_, MADV_WILLNEED);
    }

    // mmap mode ctor
    ColumnBase(const File& file,
               size_t size,
               int dim,
               const DataType& data_type)
        : type_size_(datatype_sizeof(data_type, dim)),
          num_rows_(size / datatype_sizeof(data_type, dim)),
          size_(size),
          cap_size_(size) {
        padding_ = data_type == DataType::JSON ? simdjson::SIMDJSON_PADDING : 0;

        data_ = static_cast<char*>(mmap(nullptr,
                                        cap_size_ + padding_,
                                        PROT_READ,
                                        mmap_flags,
                                        file.Descriptor(),
                                        0));
        AssertInfo(data_ != MAP_FAILED,
                   fmt::format("failed to create file-backed map, err: {}",
                               strerror(errno)));
    }

    virtual ~ColumnBase() {
        if (data_ != nullptr) {
            if (munmap(data_, cap_size_ + padding_)) {
                AssertInfo(true,
                           fmt::format("failed to unmap variable field, err={}",
                                       strerror(errno)));
            }
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

    const char*
    Data() const {
        return data_;
    }

    size_t
    NumRows() const {
        return num_rows_;
    };

    const size_t
    ByteSize() const {
        return cap_size_ + padding_;
    }

    // The capacity of the column,
    // DO NOT call this for variable length column.
    size_t
    Capacity() const {
        return cap_size_ / type_size_;
    }

    virtual SpanBase
    Span() const = 0;

    void
    AppendBatch(const storage::FieldDataPtr& data) {
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
    void
    Append(const char* data, size_t size) {
        size_t required_size = size_ + size;
        if (required_size > cap_size_) {
            Expand(required_size * 2);
        }

        std::copy_n(data, size, data_ + size_);
        size_ = required_size;
        num_rows_++;
    }

 protected:
    // only for memory mode, not mmap
    void
    Expand(size_t new_size) {
        auto data = static_cast<char*>(mmap(nullptr,
                                            new_size + padding_,
                                            PROT_READ | PROT_WRITE,
                                            mmap_flags | MAP_ANON,
                                            -1,
                                            0));

        AssertInfo(data != MAP_FAILED,
                   fmt::format("failed to create map: {}", strerror(errno)));

        if (data_ != nullptr) {
            std::memcpy(data, data_, size_);
            if (munmap(data_, cap_size_ + padding_)) {
                AssertInfo(
                    false,
                    fmt::format("failed to unmap while expanding, err={}",
                                strerror(errno)));
            }
        }

        data_ = data;
        cap_size_ = new_size;
    }

    char* data_{nullptr};
    // capacity in bytes
    size_t cap_size_{0};
    size_t padding_{0};
    const size_t type_size_{1};
    size_t num_rows_{0};

    // length in bytes
    size_t size_{0};
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
        size_t len = (i == indices_.size() - 1) ? size_ - indices_.back()
                                                : indices_[i + 1] - indices_[i];
        return std::string_view(data_ + indices_[i], len);
    }

    void
    Append(const char* data, size_t size) {
        indices_.emplace_back(size_);
        ColumnBase::Append(data, size);
    }

    void
    Seal(std::vector<uint64_t> indices = {}) {
        if (!indices.empty()) {
            indices_ = std::move(indices);
        }
        num_rows_ = indices_.size();
        ConstructViews();
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
