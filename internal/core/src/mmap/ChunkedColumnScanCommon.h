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

#include "common/EasyAssert.h"
#include "common/Types.h"

namespace milvus {

enum class ScanValueKind {
    Default,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
    VectorArrayView,
};

enum class ValueEncoding {
    Empty,
    FixedWidth,
    StringView,
    JsonView,
    ArrayView,
    VectorArrayView,
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

    bool
    IsValid(int64_t i) const {
        AssertInfo(
            i >= 0 && i < size, "validity offset {} out of range {}", i, size);
        if (encoding == ValidityEncoding::AllValid) {
            return true;
        }
        AssertInfo(data != nullptr,
                   "validity data is null for encoding {}",
                   static_cast<int>(encoding));
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

    // Expression evaluators use a legacy bool-array contract where nullptr
    // means every row is valid. Keep storage-native encodings at the Scan
    // boundary and normalize them exactly once before invoking an evaluator.
    const bool*
    bool_data(FixedVector<bool>& scratch) const {
        scratch.clear();
        if (encoding == ValidityEncoding::AllValid || size == 0) {
            return nullptr;
        }
        AssertInfo(data != nullptr,
                   "validity data is null for encoding {}",
                   static_cast<int>(encoding));
        if (encoding == ValidityEncoding::BoolArray) {
            return static_cast<const bool*>(data) + offset;
        }
        AssertInfo(encoding == ValidityEncoding::Bitmap,
                   "cannot materialize validity encoding {} as bool data",
                   static_cast<int>(encoding));

        scratch.resize(size);
        for (int64_t i = 0; i < size; ++i) {
            scratch[i] = IsValid(i);
        }
        return scratch.data();
    }
};

struct ScanBatch {
    // Every batch represents the dense row range
    // [row_id_start, row_id_start + size). Values are optional based on the
    // requested projection, while validity remains aligned with this range.
    ValueView values;
    ValidityView validity;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
};

class ScanCursor {
 public:
    virtual ~ScanCursor() = default;

    // The next dense row that has not yet been returned to the caller.
    virtual int64_t
    Position() const = 0;

    // Return the next dense batch from the underlying source. A successful
    // call starts at Position(), returns at most max_rows without crossing a
    // column chunk boundary, and advances Position() by the returned size.
    // Callers consume the returned batch as a whole; physical reader and
    // buffered-batch positions remain private to the cursor.
    virtual bool
    Next(int64_t max_rows, ScanBatch* out) = 0;
};

enum class ScanProjection {
    // Return ScanBatch::values.
    Data,
    // Omit ScanBatch::values while still returning validity.
    NoData,
};

struct ScanOptions {
    ScanOptions() = default;

    ScanOptions(int64_t start_offset,
                int64_t length,
                ScanProjection projection = ScanProjection::Data,
                ScanValueKind value_kind = ScanValueKind::Default)
        : start_offset(start_offset),
          length(length),
          projection(projection),
          value_kind(value_kind) {
    }

    static ScanOptions
    ForData(int64_t start_offset,
            int64_t length,
            ScanProjection projection = ScanProjection::Data,
            ScanValueKind value_kind = ScanValueKind::Default) {
        return ScanOptions(start_offset, length, projection, value_kind);
    }

    static ScanOptions
    ForNoData(int64_t start_offset,
              int64_t length,
              ScanValueKind value_kind = ScanValueKind::Default) {
        return ForData(
            start_offset, length, ScanProjection::NoData, value_kind);
    }

    int64_t start_offset = 0;
    int64_t length = 0;
    ScanProjection projection = ScanProjection::Data;
    ScanValueKind value_kind = ScanValueKind::Default;
};

using ScanResult = std::unique_ptr<ScanCursor>;

}  // namespace milvus
