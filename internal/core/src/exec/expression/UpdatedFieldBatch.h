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

#include <type_traits>

#include "common/Types.h"
#include "common/ValidityView.h"
#include "segcore/UpdatesRecordReaders.h"

namespace milvus::exec {

// Request-owned typed values and validity. Shared immutable payloads retain
// VARCHAR bytes; no base column or version record is modified by evaluators.
template <typename T>
class UpdatedFieldBatch {
 public:
    explicit UpdatedFieldBatch(
        std::vector<std::shared_ptr<const segcore::experimental::UpdateValue>>
            versions)
        : versions_(std::move(versions)),
          values_(versions_.size()),
          valid_(versions_.size()) {
        for (size_t i = 0; i < versions_.size(); ++i) {
            const auto& version = versions_[i];
            valid_[i] =
                version && !std::holds_alternative<std::monostate>(*version);
            if (valid_[i]) {
                if constexpr (std::is_same_v<T, int64_t>) {
                    values_[i] = std::get<int64_t>(*version);
                } else {
                    values_[i] = std::get<std::string>(*version);
                }
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
    std::vector<std::shared_ptr<const segcore::experimental::UpdateValue>>
        versions_;
    std::vector<T> values_;
    FixedVector<bool> valid_;
};

// Materialize only batches containing visible updates. Base views remain
// immutable; owning patch references keep updated string views alive.
template <typename T>
class UpdatedFieldOverlay {
 public:
    UpdatedFieldOverlay(
        const T* base,
        ValidityView validity,
        size_t count,
        std::vector<segcore::experimental::UpdatesRecord::RangeUpdate> patches)
        : patches_(std::move(patches)), values_(count), valid_(count) {
        for (size_t i = 0; i < count; ++i) {
            valid_[i] = !validity || validity[i];
            if (valid_[i])
                values_[i] = base[i];
        }
        for (const auto& patch : patches_) {
            const auto& value = *patch.value;
            valid_[patch.position] =
                !std::holds_alternative<std::monostate>(value);
            if (valid_[patch.position]) {
                if constexpr (std::is_same_v<T, int64_t>)
                    values_[patch.position] = std::get<int64_t>(value);
                else
                    values_[patch.position] = std::get<std::string>(value);
            } else {
                values_[patch.position] = T{};
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
    std::vector<segcore::experimental::UpdatesRecord::RangeUpdate> patches_;
    std::vector<T> values_;
    FixedVector<bool> valid_;
};

}  // namespace milvus::exec
