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

#include "mmap/VirtualPKChunkedColumn.h"

#include <memory>
#include <utility>
#include <vector>

namespace milvus {
namespace {

inline int64_t
ComputeVirtualPKValue(int64_t shifted_segment_id, int64_t offset) {
    return shifted_segment_id | (offset & 0xFFFFFFFF);
}

void
ValidateVirtualPKValueKind(ScanValueKind requested) {
    const auto resolved = requested == ScanValueKind::Default
                              ? ScanValueKind::FixedWidth
                              : requested;
    AssertInfo(resolved == ScanValueKind::FixedWidth,
               "virtual PK value kind {} is not fixed-width",
               static_cast<int>(resolved));
}

struct VirtualPKValues {
    std::vector<int64_t> values;
};

ValueView
MakeVirtualPKValueView(const VirtualPKValues& owner) {
    ValueView values;
    values.kind = ScanValueKind::FixedWidth;
    values.data = owner.values.data();
    values.offset = 0;
    values.byte_width = sizeof(int64_t);
    return values;
}

class VirtualPKScanCursor final : public ScanCursor {
 public:
    VirtualPKScanCursor(int64_t shifted_segment_id,
                        int64_t num_rows,
                        int64_t start_offset)
        : shifted_segment_id_(shifted_segment_id),
          num_rows_(num_rows),
          scan_pos_(start_offset) {
        AssertInfo(start_offset >= 0 && start_offset <= num_rows_,
                   "virtual PK scan start {} out of rows {}",
                   start_offset,
                   num_rows_);
    }

    bool
    Next(int64_t position,
         int64_t length,
         ScanReadMode mode,
         ScanBatch* out) override {
        AssertInfo(out != nullptr, "virtual PK scan output batch is null");
        *out = ScanBatch{};
        AssertInfo(position >= scan_pos_,
                   "virtual PK scan cannot seek backward from {} to {}",
                   scan_pos_,
                   position);
        AssertInfo(position >= 0 && position <= num_rows_ && length >= 0 &&
                       length <= num_rows_ - position,
                   "virtual PK scan range [{}, {}) out of rows {}",
                   position,
                   position + length,
                   num_rows_);
        scan_pos_ = position;
        if (length == 0 || position == num_rows_) {
            return false;
        }
        AssertInfo(mode != ScanReadMode::ValidityOnly,
                   "validity-only scan requested for non-nullable virtual PK "
                   "column");

        out->row_id_start = position;
        out->size = length;
        AssertInfo(mode == ScanReadMode::DataAndValidity,
                   "unsupported virtual PK scan mode {}",
                   static_cast<int>(mode));
        auto owner = std::make_shared<VirtualPKValues>();
        owner->values.resize(length);
        for (int64_t i = 0; i < length; ++i) {
            owner->values[i] =
                ComputeVirtualPKValue(shifted_segment_id_, position + i);
        }
        out->values = MakeVirtualPKValueView(*owner);
        out->owner = std::move(owner);
        scan_pos_ += length;
        return true;
    }

 private:
    int64_t shifted_segment_id_;
    int64_t num_rows_;
    int64_t scan_pos_;
};

class VirtualPKTakeResult final : public TakeResult {
 public:
    explicit VirtualPKTakeResult(std::shared_ptr<VirtualPKValues> owner)
        : owner_(std::move(owner)) {
        AssertInfo(owner_ != nullptr, "virtual PK take owner is null");
    }

    int64_t
    Size() const override {
        return static_cast<int64_t>(owner_->values.size());
    }

    ScanValueKind
    Kind() const override {
        return ScanValueKind::FixedWidth;
    }

    DataType
    GetDataType() const override {
        return DataType::INT64;
    }

    bool
    IsValid(int64_t index) const override {
        CheckIndex(index);
        return true;
    }

    bool
    IsOwned() const override {
        return true;
    }

    OwnedTakeData
    GetOwn() const override {
        return OwnedTakeData{
            MakeVirtualPKValueView(*owner_), nullptr, owner_, Size()};
    }

 protected:
    const void*
    FixedValueAt(int64_t index) const override {
        CheckIndex(index);
        return owner_->values.data() + index;
    }

    std::string_view
    StringViewAt(int64_t) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "virtual PK Take does not contain string values");
    }

    Json
    JsonAt(int64_t) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "virtual PK Take does not contain JSON values");
    }

    ArrayView
    ArrayAt(int64_t) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "virtual PK Take does not contain array values");
    }

 private:
    void
    CheckIndex(int64_t index) const {
        AssertInfo(index >= 0 && index < Size(),
                   "virtual PK take index {} out of range {}",
                   index,
                   Size());
    }

    std::shared_ptr<VirtualPKValues> owner_;
};

}  // namespace

ChunkedColumnInterface::ScanResult
VirtualPKChunkedColumn::Scan(milvus::OpContext*,
                             const ScanOptions& options) const {
    ValidateVirtualPKValueKind(options.value_kind);
    return std::make_unique<VirtualPKScanCursor>(
        shifted_segment_id_, num_rows_, options.start_offset);
}

ChunkedColumnInterface::TakeResultPtr
VirtualPKChunkedColumn::Take(milvus::OpContext*,
                             TakePlan plan,
                             ScanValueKind requested_kind) const {
    ValidateVirtualPKValueKind(requested_kind);
    auto owner = std::make_shared<VirtualPKValues>();
    owner->values.resize(plan.Size());
    for (int64_t i = 0; i < plan.Size(); ++i) {
        const auto& location = plan.locations[i];
        AssertInfo(location.source_cell_id == 0,
                   "virtual PK Take source Cell {} is not 0",
                   location.source_cell_id);
        AssertInfo(location.cell_offset < static_cast<size_t>(num_rows_),
                   "virtual PK Take offset {} out of rows {}",
                   location.cell_offset,
                   num_rows_);
        owner->values[i] =
            ComputeVirtualPK(static_cast<int64_t>(location.cell_offset));
    }
    return std::make_unique<VirtualPKTakeResult>(std::move(owner));
}

}  // namespace milvus
