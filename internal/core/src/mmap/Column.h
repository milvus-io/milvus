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
#include "exceptions/EasyAssert.h"
#include "fmt/format.h"
#include "mmap/Utils.h"

namespace milvus {

#ifdef MAP_POPULATE
static int mmap_flags = MAP_PRIVATE | MAP_POPULATE;
#else
static int mmap_flags = MAP_PRIVATE;
#endif

class ColumnBase {
 public:
    // memory mode ctor
    ColumnBase(size_t num_rows, const FieldMeta& field_meta) {
        // simdjson requires a padding following the json data
        padding_ = field_meta.get_data_type() == DataType::JSON
                       ? simdjson::SIMDJSON_PADDING
                       : 0;

        if (datatype_is_variable(field_meta.get_data_type())) {
            return;
        }

        size_ = field_meta.get_sizeof() * num_rows + padding_;
        auto data_type = field_meta.get_data_type();

        // use anon mapping so we are able to free these memory with munmap only
        data_ = static_cast<char*>(mmap(nullptr,
                                        size_,
                                        PROT_READ | PROT_WRITE,
                                        mmap_flags | MAP_ANON,
                                        -1,
                                        0));
        AssertInfo(
            data_ != MAP_FAILED,
            fmt::format("failed to create anon map, err: {}", strerror(errno)));
    }

    // mmap mode ctor
    ColumnBase(int fd, size_t size, const FieldMeta& field_meta) {
        padding_ = field_meta.get_data_type() == DataType::JSON
                       ? simdjson::SIMDJSON_PADDING
                       : 0;

        len_ = size;
        size_ = size + padding_;
        data_ = static_cast<char*>(
            mmap(nullptr, size_, PROT_READ, mmap_flags, fd, 0));
#ifndef MAP_POPULATE
        // Manually access the mapping to populate it
        const size_t page_size = getpagesize();
        char* begin = (char*)data_;
        char* end = begin + len_;
        for (char* page = begin; page < end; page += page_size) {
            char value = page[0];
        }
#endif
    }

    virtual ~ColumnBase() {
        if (data_ != nullptr) {
            if (munmap(data_, size_)) {
                AssertInfo(true,
                           fmt::format("failed to unmap variable field, err={}",
                                       strerror(errno)));
            }
        }
    }

    ColumnBase(ColumnBase&& column) noexcept
        : data_(column.data_), size_(column.size_) {
        column.data_ = nullptr;
        column.size_ = 0;
    }

    const char*
    Data() const {
        return data_;
    }

    virtual SpanBase
    Span() const = 0;

    // build only
    void
    Append(const char* data, size_t size) {
        size_t required_size = len_ + size;
        if (required_size + padding_ > size_) {
            Expand(required_size * 2 + padding_);
        }

        std::copy_n(data, size, data_ + len_);
        len_ += size;
    }

 protected:
    // only for memory mode, not mmap
    void
    Expand(size_t size) {
        auto data = static_cast<char*>(mmap(nullptr,
                                            size,
                                            PROT_READ | PROT_WRITE,
                                            mmap_flags | MAP_ANON,
                                            -1,
                                            0));

        AssertInfo(data != MAP_FAILED,
                   fmt::format("failed to create map: {}", strerror(errno)));

        if (data_ != nullptr) {
            std::memcpy(data, data_, len_);
            if (munmap(data_, size_)) {
                AssertInfo(
                    false,
                    fmt::format("failed to unmap while expanding, err={}",
                                strerror(errno)));
            }
        }

        data_ = data;
        size_ = size;
    }

    char* data_{nullptr};
    size_t size_{0};
    size_t padding_{0};

    // build only
    size_t len_{0};
};

class Column : public ColumnBase {
 public:
    // memory mode ctor
    Column(size_t num_rows, const FieldMeta& field_meta)
        : ColumnBase(num_rows, field_meta), num_rows_(num_rows) {
    }

    // mmap mode ctor
    Column(int fd, size_t size, const FieldMeta& field_meta)
        : ColumnBase(fd, size, field_meta),
          num_rows_(size / field_meta.get_sizeof()) {
    }

    Column(Column&& column) noexcept
        : ColumnBase(std::move(column)), num_rows_(column.num_rows_) {
        column.num_rows_ = 0;
    }

    ~Column() override = default;

    size_t
    NumRows() const {
        return num_rows_;
    }

    SpanBase
    Span() const override {
        return SpanBase(data_, num_rows_, size_ / num_rows_);
    }

 private:
    size_t num_rows_{};
};

template <typename T>
class VariableColumn : public ColumnBase {
 public:
    using ViewType =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    // memory mode ctor
    VariableColumn(size_t num_rows, const FieldMeta& field_meta)
        : ColumnBase(num_rows, field_meta) {
    }

    // mmap mode ctor
    VariableColumn(int fd, size_t size, const FieldMeta& field_meta)
        : ColumnBase(fd, size, field_meta) {
    }

    VariableColumn(VariableColumn&& column) noexcept
        : ColumnBase(std::move(column)),
          indices_(std::move(column.indices_)),
          views_(std::move(column.views_)) {
    }

    ~VariableColumn() override = default;

    size_t
    NumRows() const {
        return indices_.size();
    }

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
        size_t len = (i == indices_.size() - 1) ? len_ - indices_.back()
                                                : indices_[i + 1] - indices_[i];
        return std::string_view(data_ + indices_[i], len);
    }

    void
    Append(const char* data, size_t size) {
        indices_.emplace_back(len_);
        ColumnBase::Append(data, size);
    }

    void
    Seal(std::vector<uint64_t> indices = {}) {
        if (!indices.empty()) {
            indices_ = std::move(indices);
        }
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
        views_.emplace_back(data_ + indices_.back(), len_ - indices_.back());
    }

 private:
    std::vector<uint64_t> indices_{};

    // Compatible with current Span type
    std::vector<ViewType> views_{};
};
}  // namespace milvus
