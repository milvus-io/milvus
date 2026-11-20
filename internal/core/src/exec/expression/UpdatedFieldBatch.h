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

#include "common/Array.h"
#include "common/ArrayValue.h"
#include "common/ValidityView.h"
#include "segcore/UpdatesRecordReaders.h"

namespace milvus::exec {

template <typename T>
inline constexpr bool CanOverlay =
    std::is_same_v<T, bool> || std::is_same_v<T, int8_t> ||
    std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
    std::is_same_v<T, int64_t> || std::is_same_v<T, std::string> ||
    std::is_same_v<T, std::string_view> || std::is_same_v<T, Array> ||
    std::is_same_v<T, ArrayView> || std::is_same_v<T, ArrayValueView>;

template <typename T>
T
UpdateScalar(const segcore::UpdateValue& value) {
    if constexpr (std::is_same_v<T, std::string> ||
                  std::is_same_v<T, std::string_view>)
        return *std::get<segcore::UpdateString>(value);
    else
        return std::get<T>(value);
}

// The native batch remains pinned for the duration of evaluation. This private
// copy owns every converted array backing the views used by the evaluator.
template <typename T>
class UpdatedFieldBatch {
 public:
    UpdatedFieldBatch(const T* base,
                      ValidityView validity,
                      size_t count,
                      const std::vector<segcore::UpdatesRecord::Patch>& patches,
                      const FieldMeta& meta,
                      const TargetBitmapView* mask = nullptr,
                      size_t mask_start = 0)
        : values_(count), valid_(count) {
        for (size_t i = 0; i < count; ++i) {
            if (mask && !mask->empty() && !(*mask)[mask_start + i])
                continue;
            valid_[i] = !validity || validity[i];
            if (valid_[i])
                values_[i] = base[i];
        }
        for (const auto& [i, value] : patches) {
            valid_[i] = !std::holds_alternative<std::monostate>(value);
            if (!valid_[i])
                continue;
            if constexpr (std::is_same_v<T, ArrayView>) {
                auto& a =
                    arrays_.emplace_back(*std::get<segcore::UpdateArray>(value),
                                         meta.is_element_nullable());
                values_[i] = ArrayView(const_cast<char*>(a.data()),
                                       a.length(),
                                       a.byte_size(),
                                       a.get_element_type(),
                                       a.get_offsets_data(),
                                       a.get_element_valid_data(),
                                       a.is_element_nullable(),
                                       a.has_invalid_element());
            } else if constexpr (std::is_same_v<T, ArrayValueView>) {
                auto& a = native_arrays_.emplace_back(
                    *std::get<segcore::UpdateArray>(value),
                    meta.get_array_type_schema());
                values_[i] = a.View();
            } else if constexpr (std::is_same_v<T, Array>) {
                values_[i] = Array(*std::get<segcore::UpdateArray>(value),
                                   meta.is_element_nullable());
            } else {
                values_[i] = UpdateScalar<T>(value);
            }
        }
    }
    const T*
    Data() const {
        return values_.data();
    }
    ValidityView
    Validity() const {
        return ValidityView::FromExpanded(valid_.data());
    }

 private:
    FixedVector<T> values_;
    FixedVector<bool> valid_;
    std::vector<Array> arrays_;
    std::vector<ArrayValue> native_arrays_;
};
}  // namespace milvus::exec
