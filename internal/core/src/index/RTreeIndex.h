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
#include <vector>
#include <folly/SharedMutex.h>
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"
#include "index/RTreeIndexWrapper.h"
#include "index/ScalarIndex.h"
#include "index/Meta.h"
#include "pb/plan.pb.h"

namespace milvus::index {

using RTreeIndexWrapper = milvus::index::RTreeIndexWrapper;

template <typename T>
class RTreeIndex : public ScalarIndex<T> {
 public:
    using MemFileManager = storage::MemFileManagerImpl;
    using MemFileManagerPtr = std::shared_ptr<MemFileManager>;
    using DiskFileManager = storage::DiskFileManagerImpl;
    using DiskFileManagerPtr = std::shared_ptr<DiskFileManager>;

    RTreeIndex() : ScalarIndex<T>(RTREE_INDEX_TYPE) {
    }

    explicit RTreeIndex(
        const storage::FileManagerContext& ctx = storage::FileManagerContext());

    ~RTreeIndex();

    void
    InitForBuildIndex();

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    // Load index from an already assembled BinarySet (not used by RTree yet)
    void
    Load(const BinarySet& binary_set, const Config& config = {}) override;

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::RTREE;
    }

    void
    Build(const Config& config = {}) override;

    // Build index directly from in-memory value array (required by ScalarIndex)
    void
    Build(size_t n, const T* values, const bool* valid_data = nullptr) override;

    int64_t
    Count() override {
        if (is_built_) {
            return total_num_rows_;
        }
        return wrapper_ ? wrapper_->count() +
                              static_cast<int64_t>(null_offset_.size())
                        : 0;
    }

    // BuildWithRawDataForUT should be only used in ut. Only string is supported.
    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) override;

    // Build index with string data (WKB format) for growing segment
    void
    BuildWithStrings(const std::vector<std::string>& geometries);

    // Add single geometry incrementally (for growing segment)
    void
    AddGeometry(const std::string& wkb_data, int64_t row_offset);

    BinarySet
    Serialize(const Config& config) override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    const TargetBitmap
    In(size_t n, const T* values) override;

    const TargetBitmap
    IsNull() override;

    TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    InApplyFilter(
        size_t n,
        const T* values,
        const std::function<bool(size_t /* offset */)>& filter) override;

    void
    InApplyCallback(
        size_t n,
        const T* values,
        const std::function<void(size_t /* offset */)>& callback) override;

    const TargetBitmap
    NotIn(size_t n, const T* values) override;

    const TargetBitmap
    Range(T value, OpType op) override;

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override;

    const bool
    HasRawData() const override {
        return false;
    }

    std::optional<T>
    Reverse_Lookup(size_t offset) const override {
        ThrowInfo(ErrorCode::NotImplemented,
                  "Reverse_Lookup should not be handled by R-Tree index");
    }

    int64_t
    Size() override {
        return Count();
    }

    // GIS-specific query methods
    /**
     * @brief Query candidates based on spatial operation
     * @param op Spatial operation type
     * @param query_geom Query geometry in WKB format
     * @param candidate_offsets Output vector of candidate row offsets
     */
    void
    QueryCandidates(proto::plan::GISFunctionFilterExpr_GISOp op,
                    const Geometry query_geometry,
                    std::vector<int64_t>& candidate_offsets);

    const TargetBitmap
    Query(const DatasetPtr& dataset) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

 protected:
    void
    finish();

 protected:
    std::shared_ptr<RTreeIndexWrapper> wrapper_;
    std::string path_;
    proto::schema::FieldSchema schema_;

    MemFileManagerPtr mem_file_manager_;
    DiskFileManagerPtr disk_file_manager_;

    // Index state
    bool is_built_ = false;
    int64_t total_num_rows_ = 0;

    // Track null rows to support IsNull/IsNotNull just like other scalar indexes
    folly::SharedMutexWritePriority mutex_{};
    std::vector<size_t> null_offset_;
};
}  // namespace milvus::index