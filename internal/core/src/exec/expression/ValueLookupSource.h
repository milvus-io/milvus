// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <utility>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/expression/IndexPathSelection.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {

// One expression-local value source. Selection is metadata-only until the
// exact entry is pinned; an empty pin means the caller scans raw column data.
class PinnedValueLookup final {
 public:
    PinnedValueLookup() = default;

    PinnedValueLookup(const segcore::SegmentInternalInterface* segment,
                      OpContext* op_ctx,
                      FieldId field_id,
                      DataType data_type,
                      int64_t active_count) {
        const auto capabilities = segment->IndexCapability(field_id);
        const auto decision = DetermineExecPath(
            {.field_id = field_id,
             .reader = RequiredReader::ValueLookup,
             .value_type = data_type},
            capabilities);
        if (!decision.key.has_value()) {
            return;
        }

        const auto* entry = capabilities.Find(*decision.key);
        AssertInfo(
            entry != nullptr,
            "selected value index for field {} disappeared from metadata",
            field_id.get());
        const index::IIndexReaderBase* reader = nullptr;
        if (segment->type() == SegmentType::Growing) {
            growing_pin_ = segment->PinGrowingIndex(field_id);
            if (!growing_pin_) {
                return;
            }
            reader = &growing_pin_.Reader();
            covered_end_ =
                std::min(active_count, growing_pin_.CoveredRowEnd());
        } else {
            root_pin_ = segment->PinIndex(op_ctx, *decision.key);
            if (!root_pin_) {
                return;
            }
            reader = root_pin_.get();
            covered_end_ = active_count;
        }
        AssertInfo(segcore::SameCaps(entry->caps, reader->Caps()),
                   "value index metadata for field {} does not match reader "
                   "capabilities",
                   field_id.get());
        ValidateReader(field_id, data_type, reader);
        reader_ = reader;
    }

    PinnedValueLookup(const PinnedValueLookup&) = delete;
    PinnedValueLookup&
    operator=(const PinnedValueLookup&) = delete;
    PinnedValueLookup(PinnedValueLookup&& other) noexcept
        : root_pin_(std::move(other.root_pin_)),
          growing_pin_(std::move(other.growing_pin_)),
          reader_(std::exchange(other.reader_, nullptr)),
          covered_end_(std::exchange(other.covered_end_, 0)) {
    }
    PinnedValueLookup&
    operator=(PinnedValueLookup&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        reader_ = nullptr;
        covered_end_ = 0;
        root_pin_ = std::move(other.root_pin_);
        growing_pin_ = std::move(other.growing_pin_);
        reader_ = std::exchange(other.reader_, nullptr);
        covered_end_ = std::exchange(other.covered_end_, 0);
        return *this;
    }

    bool
    HasReader() const {
        return reader_ != nullptr;
    }

    bool
    Covers(const int64_t* offsets, int64_t count) const {
        if (!HasReader()) {
            return false;
        }
        for (int64_t i = 0; i < count; ++i) {
            if (offsets[i] < 0 || offsets[i] >= covered_end_) {
                return false;
            }
        }
        return true;
    }

    template <typename T>
    const index::IScalarValueReader<T>*
    Reader() const {
        return dynamic_cast<const index::IScalarValueReader<T>*>(reader_);
    }

 private:
    void
    ValidateReader(FieldId field_id,
                   DataType data_type,
                   const index::IIndexReaderBase* reader) {
        const auto validate = [&]<typename T>() {
            auto* selected =
                dynamic_cast<const index::IScalarValueReader<T>*>(reader);
            AssertInfo(
                selected != nullptr,
                "value index for field {} does not expose lookup type {}",
                field_id.get(),
                data_type);
        };
        switch (data_type) {
            case DataType::BOOL:
                validate.template operator()<bool>();
                break;
            case DataType::INT8:
                validate.template operator()<int8_t>();
                break;
            case DataType::INT16:
                validate.template operator()<int16_t>();
                break;
            case DataType::INT32:
                validate.template operator()<int32_t>();
                break;
            case DataType::INT64:
            // #52689: TIMESTAMPTZ is stored and indexed as int64.
            case DataType::TIMESTAMPTZ:
                validate.template operator()<int64_t>();
                break;
            case DataType::FLOAT:
                validate.template operator()<float>();
                break;
            case DataType::DOUBLE:
                validate.template operator()<double>();
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
                validate.template operator()<std::string_view>();
                break;
            default:
                ThrowInfo(UnexpectedError,
                          "unsupported value lookup data type {}",
                          data_type);
        }
    }

    // Reader pointers are non-owning and are destroyed before both pins.
    segcore::IndexPin root_pin_;
    index::GrowingIndexSnapshotPin growing_pin_;
    const index::IIndexReaderBase* reader_{nullptr};
    int64_t covered_end_{0};
};

}  // namespace milvus::exec
