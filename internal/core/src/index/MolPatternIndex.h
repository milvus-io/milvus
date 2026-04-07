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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/File.h"
#include "index/ScalarIndex.h"
#include "index/Meta.h"
#include "storage/FileManager.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

// MolPatternIndex stores PatternFingerprints for mol field data.
// Used as a conservative pre-filter for mol_contains substructure/superstructure queries.
// Inherits ScalarIndex<string> for compatibility with the Growing segment's
// ScalarFieldIndexing<string> infrastructure.
//
// Thread safety (Growing segment): The fp buffer is pre-allocated to
// max_row_count_ * bytes_per_row_ at construction (zero-filled), so the
// data pointer is stable.  Writers write fp data at disjoint reserved
// offsets (from PreInsert), then CAS-advance published_row_count_ to
// max(current, offset+1).  published_row_count_ is a high-water mark,
// NOT a continuous-prefix guarantee; gaps may transiently contain zero
// FPs (conservative).  Query visibility is bounded by AckResponder,
// which only advances when all rows in [0, ack) are committed.
template <typename T>
class MolPatternIndex : public ScalarIndex<T> {
 public:
    using MemFileManager = storage::MemFileManagerImpl;
    using MemFileManagerPtr = std::shared_ptr<MemFileManager>;

    explicit MolPatternIndex(int64_t max_row_count = 0, int32_t n_bit = 0)
        : ScalarIndex<T>(MOL_PATTERN_INDEX_TYPE),
          max_row_count_(max_row_count) {
        if (n_bit > 0) {
            dim_ = n_bit;
        }
        bytes_per_row_ = (dim_ + 7) / 8;
        if (max_row_count_ > 0) {
            fp_data_owned_.resize(max_row_count_ * bytes_per_row_, 0);
        }
    }

    explicit MolPatternIndex(
        const storage::FileManagerContext& file_manager_context);

    ~MolPatternIndex() override;

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::MOL_PATTERN;
    }

    // IndexBase interface
    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override;

    void
    Load(tracer::TraceContext ctx, const Config& config = {}) override;

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

    void
    Build(const Config& config = {}) override;

    void
    Build(size_t n,
          const T* values,
          const bool* valid_data = nullptr) override;

    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) override;

    void
    BuildWithFieldData(
        const std::vector<FieldDataPtr>& field_datas) override;

    int64_t
    Count() override {
        return published_row_count_.load(std::memory_order_acquire);
    }

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        return false;
    }

    bool
    IsMmapSupported() const override {
        return true;
    }

    void
    ComputeByteSize() override {
        ScalarIndex<T>::ComputeByteSize();
        this->cached_byte_size_ = fp_data_owned_.size() +
                                  sizeof(dim_) + sizeof(row_count_) +
                                  sizeof(bytes_per_row_);
    }

    // ScalarIndex<T> pure virtual methods — all unsupported for MOL_PATTERN
    const TargetBitmap
    In(size_t n, const T* values) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not support In");
    }

    // MolPatternIndex does not track null bitmap.
    // NullExpr bypasses this index for MOL fields (see NullExpr.cpp),
    // so these should never be called. Throw to catch misuse.
    const TargetBitmap
    IsNull() override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not track null bitmap, "
                  "NullExpr should bypass this index");
    }

    TargetBitmap
    IsNotNull() override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not track null bitmap, "
                  "NullExpr should bypass this index");
    }

    const TargetBitmap
    NotIn(size_t n, const T* values) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not support NotIn");
    }

    const TargetBitmap
    Range(T value, OpType op) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not support Range");
    }

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not support Range");
    }

    std::optional<T>
    Reverse_Lookup(size_t offset) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "MolPatternIndex does not support Reverse_Lookup");
    }

    int64_t
    Size() override {
        return published_row_count_.load(std::memory_order_acquire);
    }

    // MOL-specific access for query executor (lock-free)
    int32_t
    Dim() const {
        return dim_;
    }

    // Return the high-water mark of written offsets (max offset + 1).
    // Under concurrent inserts, gaps may exist where fp data is still
    // zero-initialized.  These gaps are NOT query-visible because query
    // visibility is bounded by AckResponder::GetAck(), which only
    // advances when all rows in [0, ack) are fully committed.
    int64_t
    RowCount() const {
        return published_row_count_.load(std::memory_order_acquire);
    }

    const uint8_t*
    GetFPData() const {
        return fp_data_mmap_ ? fp_data_mmap_ : fp_data_owned_.data();
    }

    // Growing segment: write fingerprint for one mol row at a specific offset.
    // row_offset is the segment-level row offset (from PreInsert reserved_offset).
    // Lock-free, safe for concurrent callers with disjoint offsets.
    void
    AppendMolRow(int64_t row_offset,
                 const std::string& pickle_data,
                 bool is_valid);

 private:
    int32_t dim_ = 2048;       // bits
    int32_t bytes_per_row_ = 0;
    int64_t row_count_ = 0;        // total rows written (writer-only)
    int64_t max_row_count_ = 0;    // pre-allocated capacity

    // High-water mark of written offsets (max row_offset + 1, lock-free).
    // NOT a "continuous prefix" guarantee — under concurrent/out-of-order
    // inserts, some rows in [0, published_row_count_) may still be zero.
    // This is safe because query visibility is bounded by AckResponder
    // which IS a continuous prefix — zero-FP gaps are never query-visible.
    std::atomic<int64_t> published_row_count_{0};

    // Fingerprint data — owned (Build / non-mmap Load / Growing)
    // For Growing segment: pre-allocated to max_row_count_ * bytes_per_row_
    // at construction. DO NOT resize/insert/push_back after construction —
    // concurrent readers depend on a stable data pointer.
    std::vector<uint8_t> fp_data_owned_;

    // Fingerprint data — mmap pointer (mmap Load)
    const uint8_t* fp_data_mmap_ = nullptr;
    size_t fp_data_mmap_size_ = 0;
    std::unique_ptr<MmapFileRAII> mmap_file_raii_;

    MemFileManagerPtr mem_file_manager_;
};

}  // namespace milvus::index
