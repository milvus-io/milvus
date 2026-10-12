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

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumnFilter.h"

namespace milvus {
namespace {

inline int64_t
ComputeVirtualPKValue(int64_t shifted_segment_id, int64_t offset) {
    return shifted_segment_id | (offset & 0xFFFFFFFF);
}

void
ValidateVirtualPKScanTargetType(TargetType requested) {
    AssertInfo(requested == TargetType::None || requested == TargetType::Int64,
               "virtual PK scan target {} is neither None nor Int64",
               static_cast<int>(requested));
}

void
ValidateVirtualPKTakeTargetType(TargetType requested) {
    AssertInfo(requested == TargetType::Int64,
               "virtual PK target {} is not Int64",
               static_cast<int>(requested));
}

struct VirtualPKValues {
    std::vector<int64_t> values;
    FixedVector<bool> data_skipped;
};

ValueView
MakeVirtualPKValueView(const VirtualPKValues& owner) {
    ValueView values;
    values.target_type = TargetType::Int64;
    values.data = owner.values.empty() ? nullptr : owner.values.data();
    values.offset = 0;
    values.byte_width = sizeof(int64_t);
    return values;
}

class VirtualPKScanCursor final : public ScanCursor {
 public:
    VirtualPKScanCursor(int64_t shifted_segment_id,
                        int64_t num_rows,
                        int64_t start_offset,
                        TargetType target_type,
                        std::function<bool()> should_skip_data)
        : shifted_segment_id_(shifted_segment_id),
          num_rows_(num_rows),
          target_type_(target_type),
          should_skip_data_(std::move(should_skip_data)),
          scan_pos_(start_offset) {
        AssertInfo(start_offset >= 0 && start_offset <= num_rows_,
                   "virtual PK scan start {} out of rows {}",
                   start_offset,
                   num_rows_);
    }

    int64_t
    Position() const override {
        return scan_pos_;
    }

    void
    Seek(int64_t position) override {
        AssertInfo(position >= scan_pos_,
                   "virtual PK scan cannot seek backward from {} to {}",
                   scan_pos_,
                   position);
        AssertInfo(position <= num_rows_,
                   "virtual PK scan seek {} out of rows {}",
                   position,
                   num_rows_);
        scan_pos_ = position;
    }

    bool
    Next(int64_t length, ScanReadMode read_mode, ScanBatch* out) override {
        AssertInfo(out != nullptr, "virtual PK scan output batch is null");
        *out = ScanBatch{};
        AssertInfo(length >= 0,
                   "virtual PK scan length {} must be non-negative",
                   length);
        if (length == 0 || scan_pos_ == num_rows_) {
            return false;
        }
        length = std::min(length, num_rows_ - scan_pos_);
        AssertInfo(read_mode != ScanReadMode::ValidityOnly,
                   "validity-only scan requested for non-nullable virtual PK "
                   "column");
        AssertInfo(target_type_ == TargetType::Int64,
                   "virtual PK data scan target {} is not Int64",
                   static_cast<int>(target_type_));

        out->row_id_start = scan_pos_;
        out->size = length;
        AssertInfo(read_mode == ScanReadMode::DataAndValidity,
                   "unsupported virtual PK scan mode {}",
                   static_cast<int>(read_mode));
        if (!filter_evaluated_) {
            filter_evaluated_ = true;
            data_skipped_ = should_skip_data_ && should_skip_data_();
        }
        if (data_skipped_) {
            out->data_skipped = true;
            scan_pos_ += length;
            return true;
        }
        auto owner = std::make_shared<VirtualPKValues>();
        owner->values.reserve(length);
        for (int64_t i = 0; i < length; ++i) {
            owner->values.emplace_back(
                ComputeVirtualPKValue(shifted_segment_id_, scan_pos_ + i));
        }
        out->values = MakeVirtualPKValueView(*owner);
        out->owner = std::move(owner);
        scan_pos_ += length;
        return true;
    }

 private:
    int64_t shifted_segment_id_;
    int64_t num_rows_;
    TargetType target_type_;
    std::function<bool()> should_skip_data_;
    bool filter_evaluated_{false};
    bool data_skipped_{false};
    int64_t scan_pos_;
};

class VirtualPKTakeResult final : public TakeResult {
 public:
    VirtualPKTakeResult(std::shared_ptr<VirtualPKValues> owner, int64_t size)
        : owner_(std::move(owner)), size_(size) {
        AssertInfo(owner_ != nullptr, "virtual PK take owner is null");
        AssertInfo(
            size_ >= 0, "virtual PK take size {} must be non-negative", size_);
        AssertInfo(
            owner_->data_skipped.empty()
                ? static_cast<int64_t>(owner_->values.size()) == size_
                : (static_cast<int64_t>(owner_->data_skipped.size()) == size_ &&
                   owner_->values.empty()),
            "virtual PK take storage does not match logical size {}",
            size_);
    }

    int64_t
    Size() const override {
        return size_;
    }

    TargetType
    GetTargetType() const override {
        return TargetType::Int64;
    }

    DataType
    GetDataType() const override {
        return DataType::INT64;
    }

 protected:
    TakeItemState
    PrepareItem(int64_t index, bool read_data) const override {
        return {true,
                read_data && !owner_->data_skipped.empty() &&
                    owner_->data_skipped[index]};
    }

 public:
    bool
    IsOwned() const override {
        return true;
    }

    OwnedTakeData
    GetOwn() const override {
        const auto data_skipped =
            owner_->data_skipped.empty()
                ? ValidityView{}
                : ValidityView::FromExpanded(owner_->data_skipped.data());
        return OwnedTakeData{
            MakeVirtualPKValueView(*owner_), {}, owner_, Size(), data_skipped};
    }

 protected:
    const void*
    FixedValueAt(int64_t index) const override {
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
    std::shared_ptr<VirtualPKValues> owner_;
    int64_t size_;
};

}  // namespace

ChunkedColumnInterface::ScanResult
VirtualPKChunkedColumn::Scan(milvus::OpContext*,
                             const ScanOptions& options) const {
    ValidateVirtualPKScanTargetType(options.target_type);
    return std::make_unique<VirtualPKScanCursor>(
        shifted_segment_id_,
        num_rows_,
        options.start_offset,
        options.target_type,
        options.filter == nullptr
            ? std::function<bool()>{}
            : std::function<bool()>{[this, filter = options.filter]() {
                  return ShouldSkipData(filter);
              }});
}

ChunkedColumnInterface::TakeResultPtr
VirtualPKChunkedColumn::Take(milvus::OpContext*, TakeOptions options) const {
    ValidateVirtualPKTakeTargetType(options.target_type);
    const auto offsets = options.offsets;
    AssertInfo(offsets.size >= 0,
               "virtual PK Take offset count must be non-negative, got {}",
               offsets.size);
    AssertInfo(offsets.size == 0 || offsets.data != nullptr,
               "virtual PK Take offsets are null with count {}",
               offsets.size);

    auto owner = std::make_shared<VirtualPKValues>();
    const auto data_skipped =
        offsets.size > 0 && ShouldSkipData(options.filter);
    if (data_skipped) {
        owner->data_skipped.resize(offsets.size, true);
    } else {
        owner->values.reserve(offsets.size);
    }
    for (int64_t i = 0; i < offsets.size; ++i) {
        const auto offset = offsets[i];
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "virtual PK Take offset {} is out of rows {}",
                   offset,
                   num_rows_);
        if (!data_skipped) {
            owner->values.emplace_back(ComputeVirtualPK(offset));
        }
    }
    return std::make_unique<VirtualPKTakeResult>(std::move(owner),
                                                 offsets.size);
}

bool
VirtualPKChunkedColumn::ShouldSkipData(
    const detail::ColumnFilterPtr& filter) const {
    if (filter == nullptr) {
        return false;
    }
    if (filter->Source() ==
        detail::ColumnFilter::MetricsSource::LoadedPayload) {
        // Match the legacy Chunk path: payload-backed statistics are consulted
        // only after the generated data has been materialized.
        EnsureMaterialized();
    }
    return filter->CanSkipPhysicalCell(0);
}

}  // namespace milvus
