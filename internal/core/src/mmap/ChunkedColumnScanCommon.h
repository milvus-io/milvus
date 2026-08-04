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

enum class OffsetElementType {
    Int32,
    Int64,
};

// A non-owning view over positional row offsets. Callers must keep the
// underlying array alive for the lifetime of the TakeCursor. Supporting both
// widths lets expression evaluation use its existing int32 offsets without a
// widening copy while retrieve/requery can pass segment int64 offsets.
struct OffsetView {
    const void* data = nullptr;
    int64_t size = 0;
    OffsetElementType element_type = OffsetElementType::Int32;

    static OffsetView
    From(const int32_t* offsets, int64_t count) {
        return OffsetView{offsets, count, OffsetElementType::Int32};
    }

    static OffsetView
    From(const int64_t* offsets, int64_t count) {
        return OffsetView{offsets, count, OffsetElementType::Int64};
    }

    int64_t
    operator[](int64_t index) const {
        AssertInfo(index >= 0 && index < size,
                   "take offset index {} out of range {}",
                   index,
                   size);
        AssertInfo(data != nullptr,
                   "take offsets are null with non-empty size {}",
                   size);
        if (element_type == OffsetElementType::Int32) {
            return static_cast<const int32_t*>(data)[index];
        }
        return static_cast<const int64_t*>(data)[index];
    }
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

struct ScanBatch {
    // Every batch represents the dense row range
    // [row_id_start, row_id_start + size). Values are optional based on the
    // requested projection, while validity remains aligned with this range.
    ValueView values;
    // Evaluators consume validity as one bool per logical row. nullptr means
    // every row in this batch is valid. Storage-native bitmap encodings must
    // be normalized by the cursor before they cross this boundary.
    const bool* validity = nullptr;
    std::shared_ptr<void> owner;
    int64_t row_id_start = 0;
    int64_t size = 0;
};

struct TakeBatch {
    // Take preserves the caller's offset order. values is a backend-native
    // addressable collection and selection maps each logical output item to
    // one value in that collection. A null selection means identity indexing.
    //
    // Raw fixed-width batches expose a pinned Chunk span plus chunk-local
    // selection offsets. Reader-backed formats may return already ordered,
    // dense decoded values with a null selection.
    ValueView values;
    const int32_t* selection = nullptr;
    // Validity uses the same indexing as values: selection[i] when selection
    // is present, otherwise i. nullptr means every logical item is valid.
    const bool* validity = nullptr;
    std::shared_ptr<void> owner;
    // Logical position in the input OffsetView of the first returned item.
    int64_t position = 0;
    int64_t size = 0;
    // Raw batches identify their physical source chunk so expression skip
    // statistics can remain chunk-aware. Reader-backed ordered results may
    // combine sources and leave this as -1.
    int64_t source_chunk_id = -1;
};

class TakeCursor {
 public:
    virtual ~TakeCursor() = default;

    // Number of input offsets already returned.
    virtual int64_t
    Position() const = 0;

    // Return the next ordered positional batch. Concatenating all successful
    // batches must produce exactly the input offset order, including duplicate
    // offsets. max_rows limits the logical items exposed to the caller; a
    // backend may physically read/materialize more data once and slice it here.
    virtual bool
    Next(int64_t max_rows, TakeBatch* out) = 0;
};

struct TakeOptions {
    OffsetView offsets;
    ScanValueKind value_kind = ScanValueKind::Default;
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

// PreparedScan owns the storage resources selected for one logical scan,
// including cache pins. Cursors only own position and reader state, so callers
// may discard a cursor after short-circuiting and seek to a new position
// without planning or pinning the same cells again.
class PreparedScan {
 public:
    virtual ~PreparedScan() = default;

    virtual int64_t
    Start() const = 0;

    virtual int64_t
    End() const = 0;

    virtual std::unique_ptr<ScanCursor>
    Seek(int64_t position) const = 0;
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
using PreparedScanResult = std::shared_ptr<PreparedScan>;
using TakeResult = std::unique_ptr<TakeCursor>;

}  // namespace milvus
