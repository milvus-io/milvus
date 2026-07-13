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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "pb/plan.pb.h"

namespace milvus {

enum class ScanValueKind {
    Default,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
};

enum class ValueEncoding {
    Empty,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
};

enum class ValidityEncoding {
    AllValid,
    BoolArray,
    Bitmap,
};

struct ValueView {
    ValueEncoding encoding = ValueEncoding::Empty;
    ScanValueKind kind = ScanValueKind::Default;
    DataType physical_type = DataType::NONE;
    DataType logical_type = DataType::NONE;
    const void* data = nullptr;
    int64_t offset = 0;
    int64_t size = 0;
    int32_t byte_width = 0;

    bool
    empty() const {
        return encoding == ValueEncoding::Empty || data == nullptr;
    }

    template <typename T>
    const T*
    data_as() const {
        AssertInfo(encoding != ValueEncoding::Empty && data != nullptr,
                   "scan value view is empty");
        return static_cast<const T*>(data) + offset;
    }
};

struct ValidityView {
    ValidityEncoding encoding = ValidityEncoding::AllValid;
    const void* data = nullptr;
    int64_t offset = 0;
    int64_t size = 0;
    bool nullable = false;
    bool all_valid = true;

    bool
    IsValid(int64_t i) const {
        AssertInfo(i >= 0 && (size == 0 || i < size),
                   "validity offset {} out of range {}",
                   i,
                   size);
        if (encoding == ValidityEncoding::AllValid || all_valid ||
            data == nullptr) {
            return true;
        }
        const auto pos = offset + i;
        switch (encoding) {
            case ValidityEncoding::BoolArray:
                return static_cast<const bool*>(data)[pos];
            case ValidityEncoding::Bitmap: {
                const auto* bitmap = static_cast<const uint8_t*>(data);
                return (bitmap[pos >> 3] >> (pos & 0x07)) & 1;
            }
            case ValidityEncoding::AllValid:
                return true;
        }
        return true;
    }
};

struct ScanBatch {
    // Every batch represents the dense row range
    // [row_id_start, row_id_start + size) for data scans. Payload fields are
    // optional based on ScanOptions. For data scans, values and validity are
    // dense over this range. For filter pushdown scans, row_ids is sparse and
    // validity, when present, is aligned with row_ids.
    ValueView values;
    ValidityView validity;
    std::vector<int64_t> row_ids;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
};

class ScanCursor {
 public:
    virtual ~ScanCursor() = default;

    // Return the next natural batch from the underlying source. ScanOptions
    // does not carry an upper-layer batch-size hint; callers should consume
    // the returned batch directly or keep their own position inside it.
    virtual bool
    Next(ScanBatch* out) = 0;
};

enum class ScanOutput {
    // Filter pushdown payload: ScanBatch::row_ids contains predicate true or
    // unknown rows; ScanBatch::validity is aligned with row_ids.
    RowIds,
    // Dense data payload: ScanBatch::values contains values over the batch
    // range unless projection asks to omit data.
    Data,
};

enum class ScanProjection {
    // Return ScanBatch::values for dense data scans.
    Data,
    // Omit ScanBatch::values. Dense data scans still return validity over the
    // batch range; filter pushdown scans return row_ids plus row-aligned
    // validity.
    NoData,
};

enum class ScanPredicate {
    None,
    Unary,
    BinaryRange,
};

struct ScanOptions {
    ScanOptions() = default;

    ScanOptions(ScanOutput output,
                ScanPredicate predicate,
                int64_t start_offset,
                int64_t length,
                ScanProjection projection = ScanProjection::Data,
                ScanValueKind value_kind = ScanValueKind::Default,
                proto::plan::OpType op_type = proto::plan::OpType::Invalid,
                proto::plan::GenericValue value = {},
                proto::plan::GenericValue lower_value = {},
                proto::plan::GenericValue upper_value = {},
                bool lower_inclusive = false,
                bool upper_inclusive = false)
        : output(output),
          predicate(predicate),
          start_offset(start_offset),
          length(length),
          projection(projection),
          value_kind(value_kind),
          op_type(op_type),
          value(std::move(value)),
          lower_value(std::move(lower_value)),
          upper_value(std::move(upper_value)),
          lower_inclusive(lower_inclusive),
          upper_inclusive(upper_inclusive) {
    }

    static ScanOptions
    ForData(int64_t start_offset,
            int64_t length,
            ScanProjection projection = ScanProjection::Data,
            ScanValueKind value_kind = ScanValueKind::Default) {
        return ScanOptions(ScanOutput::Data,
                           ScanPredicate::None,
                           start_offset,
                           length,
                           projection,
                           value_kind);
    }

    static ScanOptions
    ForNoData(int64_t start_offset,
              int64_t length,
              ScanValueKind value_kind = ScanValueKind::Default) {
        return ForData(
            start_offset, length, ScanProjection::NoData, value_kind);
    }

    static ScanOptions
    ForUnary(int64_t start_offset,
             int64_t length,
             proto::plan::OpType op_type,
             const proto::plan::GenericValue& value) {
        return ScanOptions(ScanOutput::RowIds,
                           ScanPredicate::Unary,
                           start_offset,
                           length,
                           ScanProjection::NoData,
                           ScanValueKind::Default,
                           op_type,
                           value);
    }

    static ScanOptions
    ForBinaryRange(int64_t start_offset,
                   int64_t length,
                   const proto::plan::GenericValue& lower_value,
                   bool lower_inclusive,
                   const proto::plan::GenericValue& upper_value,
                   bool upper_inclusive) {
        return ScanOptions(ScanOutput::RowIds,
                           ScanPredicate::BinaryRange,
                           start_offset,
                           length,
                           ScanProjection::NoData,
                           ScanValueKind::Default,
                           proto::plan::OpType::Invalid,
                           {},
                           lower_value,
                           upper_value,
                           lower_inclusive,
                           upper_inclusive);
    }

    ScanOutput output = ScanOutput::Data;
    ScanPredicate predicate = ScanPredicate::None;
    int64_t start_offset = 0;
    int64_t length = 0;
    ScanProjection projection = ScanProjection::Data;
    ScanValueKind value_kind = ScanValueKind::Default;
    proto::plan::OpType op_type = proto::plan::OpType::Invalid;
    proto::plan::GenericValue value;
    proto::plan::GenericValue lower_value;
    proto::plan::GenericValue upper_value;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
};

using ScanResult = std::unique_ptr<ScanCursor>;

}  // namespace milvus
