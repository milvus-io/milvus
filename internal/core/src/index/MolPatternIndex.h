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

#include <cstddef>
#include <cstdint>
#include <shared_mutex>
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
// Thread safety: AppendMolRow (writer) and GetFPData/RowCount (readers) are
// protected by a shared_mutex. Readers hold a shared lock, the writer holds
// an exclusive lock.
template <typename T>
class MolPatternIndex : public ScalarIndex<T> {
 public:
    using MemFileManager = storage::MemFileManagerImpl;
    using MemFileManagerPtr = std::shared_ptr<MemFileManager>;

    MolPatternIndex()
        : ScalarIndex<T>(MOL_PATTERN_INDEX_TYPE),
          bytes_per_row_((dim_ + 7) / 8) {
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
        return row_count_;
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
        this->cached_byte_size_ = fp_data_owned_.capacity() +
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
        return row_count_;
    }

    // MOL-specific access for query executor (thread-safe)
    int32_t
    Dim() const {
        return dim_;
    }

    // Snapshot current row count and fp data pointer.
    // The shared_lock is held for the lifetime of FPSnapshot, so the
    // data pointer remains valid until the snapshot is destroyed.
    struct FPSnapshot {
        int64_t row_count;
        const uint8_t* data;
        std::shared_lock<std::shared_mutex> lock;
    };

    FPSnapshot
    SnapshotFPData() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto rc = row_count_;
        auto ptr = fp_data_mmap_ ? fp_data_mmap_ : fp_data_owned_.data();
        return {rc, ptr, std::move(lock)};
    }

    int64_t
    RowCount() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return row_count_;
    }

    const uint8_t*
    GetFPData() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return fp_data_mmap_ ? fp_data_mmap_ : fp_data_owned_.data();
    }

    // Growing segment: append fingerprint for one mol row (exclusive lock)
    void
    AppendMolRow(const std::string& pickle_data, bool is_valid);

 private:
    int32_t dim_ = 2048;       // bits
    int32_t bytes_per_row_ = 0;
    int64_t row_count_ = 0;

    // Fingerprint data — owned (Build / non-mmap Load / Growing)
    std::vector<uint8_t> fp_data_owned_;

    // Fingerprint data — mmap pointer (mmap Load)
    const uint8_t* fp_data_mmap_ = nullptr;
    size_t fp_data_mmap_size_ = 0;
    std::unique_ptr<MmapFileRAII> mmap_file_raii_;

    MemFileManagerPtr mem_file_manager_;

    // Protects fp_data_owned_, fp_data_mmap_, row_count_ for concurrent
    // read (query) / write (growing append).
    mutable std::shared_mutex mutex_;
};

}  // namespace milvus::index
