// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <sys/mman.h>

#include <string_view>
#include <vector>
#include <string>
#include <utility>

#include "common/FieldMeta.h"
#include "common/LoadInfo.h"
#include "common/Span.h"
#include "common/Utils.h"
#include "exceptions/EasyAssert.h"
#include "fmt/core.h"

namespace milvus::segcore {

struct Entry {
    char* data;
    uint32_t length;
};

// Used for string/varchar field only,
// TODO(yah01): make this generic
class VariableField {
 public:
    explicit VariableField(int64_t segment_id,
                           const FieldMeta& field_meta,
                           const LoadFieldDataInfo& info) {
        auto begin = info.field_data->scalars().string_data().data().begin();
        auto end = info.field_data->scalars().string_data().data().end();

        indices_.reserve(info.row_count);
        while (begin != end) {
            indices_.push_back(size_);
            size_ += begin->size();
            begin++;
        }

        data_ = static_cast<char*>(CreateMap(segment_id, field_meta, info));
        construct_views();
    }

    VariableField(VariableField&& field) noexcept
        : indices_(std::move(field.indices_)),
          size_(field.size_),
          data_(field.data_),
          views_(std::move(field.views_)) {
        field.data_ = nullptr;
    }

    ~VariableField() {
        if (data_ != MAP_FAILED && data_ != nullptr) {
            if (munmap(data_, size_)) {
                AssertInfo(true,
                           fmt::format("failed to unmap variable field, err={}",
                                       strerror(errno)));
            }
        }
    }

    char*
    data() {
        return data_;
    }

    [[nodiscard]] const std::vector<std::string_view>&
    views() const {
        return views_;
    }

    [[nodiscard]] size_t
    size() const {
        return size_;
    }

    Span<char>
    operator[](const int i) const {
        uint64_t next = (i + 1 == indices_.size()) ? size_ : indices_[i + 1];
        uint64_t offset = indices_[i];
        return Span<char>(data_ + offset, uint32_t(next - offset));
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
    uint64_t size_{0};
    char* data_{nullptr};

    // Compatible with current Span type
    std::vector<std::string_view> views_{};
};
}  // namespace milvus::segcore
