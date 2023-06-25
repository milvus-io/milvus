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

#include <cstddef>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <vector>
#include <string>
#include <utility>

#include "mmap/Utils.h"

namespace milvus {

struct Entry {
    char* data;
    uint32_t length;
};

class ColumnBase {
 public:
    ColumnBase() = default;
    virtual ~ColumnBase() {
        if (data_ != nullptr && data_ != MAP_FAILED) {
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
    data() const {
        return data_;
    }

    [[nodiscard]] size_t
    size() const {
        return size_;
    }

    virtual SpanBase
    span() const = 0;

 protected:
    char* data_{nullptr};
    uint64_t size_{0};
};

class Column : public ColumnBase {
 public:
    Column(int64_t segment_id,
           const FieldMeta& field_meta,
           const FieldDataInfo& info) {
        data_ = static_cast<char*>(CreateMap(segment_id, field_meta, info));
        size_ = field_meta.get_sizeof() * info.row_count;
        row_count_ = info.row_count;
    }

    Column(Column&& column) noexcept
        : ColumnBase(std::move(column)), row_count_(column.row_count_) {
        column.row_count_ = 0;
    }

    ~Column() override = default;

    SpanBase
    span() const override {
        return SpanBase(data_, row_count_, size_ / row_count_);
    }

 private:
    int64_t row_count_{};
};

template <typename T>
class VariableColumn : public ColumnBase {
 public:
    using ViewType =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    VariableColumn(int64_t segment_id,
                   const FieldMeta& field_meta,
                   const FieldDataInfo& info) {
        indices_.reserve(info.row_count);
        for (auto data : info.datas) {
            for (ssize_t idx = 0; idx < data->get_num_rows(); ++idx) {
                indices_.emplace_back(size_);
                size_ += data->Size(idx);
            }
        }

        data_ = static_cast<char*>(CreateMap(segment_id, field_meta, info));
        construct_views();
    }

    VariableColumn(VariableColumn&& field) noexcept
        : indices_(std::move(field.indices_)), views_(std::move(field.views_)) {
        data_ = field.data();
        size_ = field.size();
        field.data_ = nullptr;
    }

    ~VariableColumn() override = default;

    SpanBase
    span() const override {
        return SpanBase(views_.data(), views_.size(), sizeof(ViewType));
    }

    [[nodiscard]] const std::vector<ViewType>&
    views() const {
        return views_;
    }

    ViewType
    operator[](const int i) const {
        return views_[i];
    }

    std::string_view
    raw_at(const int i) const {
        size_t len = (i == indices_.size() - 1) ? size_ - indices_.back()
                                                : indices_[i + 1] - indices_[i];
        return std::string_view(data_ + indices_[i], len);
    }

 protected:
    void
    construct_views() {
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
}  // namespace milvus
