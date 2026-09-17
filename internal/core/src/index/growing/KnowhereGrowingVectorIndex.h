// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <vector>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "common/GrowingOffsetMapping.h"
#include "common/Types.h"
#include "index/contracts/growing/IGrowingIndex.h"
#include "index/growing/GrowingVectorSource.h"
#include "index/vector/KnowhereEngine.h"
#include "knowhere/config.h"

namespace milvus::index {

// One writer owns one live concurrent Knowhere node. Published readers share
// that node, but bind a fixed physical count and a frozen nullable mapping;
// consumers must apply that physical prefix before invoking ANN.
template <typename T>
class KnowhereGrowingVectorIndex final
    : public IGrowingIndex,
      public IAppendable<VectorBatch<GrowingVectorStorageType<T>>> {
 public:
    using StorageType = GrowingVectorStorageType<T>;

    KnowhereGrowingVectorIndex(
        DataType value_type,
        IndexType index_type,
        MetricType metric_type,
        IndexVersion version,
        int64_t dim,
        int64_t build_threshold,
        knowhere::Json build_params,
        knowhere::Json search_defaults,
        std::shared_ptr<const GrowingVectorSource<StorageType>> source,
        bool retain_source_as_data_view,
        // Re-resolved before every knowhere Build/Add so a refreshed build
        // thread-num config takes effect on the next build of an existing
        // growing segment (#53030). Empty keeps whatever `build_params`
        // already carries.
        std::function<int64_t()> build_thread_num = {});

    ~KnowhereGrowingVectorIndex() override = default;

    void
    CommitIfNeeded() override;

    void
    Flush() override;

    void
    Append(int64_t row_begin, const VectorBatch<StorageType>& batch) override;

    DataType
    ValueType() const override;

    std::string
    Family() const override;

 private:
    KnowhereEngine
    CreateEngine() const;

    knowhere::Json
    BuildConfig() const;

    bool
    TryBuildAccepted();

    void
    BuildFromSource(int64_t physical_count, int64_t logical_count);

    void
    AddFromSource(int64_t physical_begin,
                  int64_t physical_count,
                  int64_t logical_begin,
                  int64_t logical_count);

    // `validity_bitmap` is the LSB-first public-row validity for
    // [logical_begin, logical_begin + logical_count). knowhere's IdMapData
    // only views it, so it must outlive this call. Empty means non-nullable.
    void
    AddBatch(const StorageType* values,
             int64_t physical_count,
             int64_t dim,
             const uint8_t* validity_bitmap,
             int64_t logical_count);

    // Rebuild the public-row validity bitmap for one logical range out of the
    // writer's append-only bookkeeping. Only the cold build needs this; an
    // incremental append already holds its batch's validity array.
    std::vector<uint8_t>
    MaterializeValidity(int64_t logical_begin, int64_t logical_count) const;

    void
    PublishAccepted();

    void
    RethrowIfPoisoned() const;

    const DataType value_type_;
    const IndexType index_type_;
    const MetricType metric_type_;
    const IndexVersion version_;
    const int64_t dim_;
    const int64_t build_threshold_;
    const knowhere::Json build_params_;
    const knowhere::Json search_defaults_;
    const bool retain_source_as_data_view_;
    const std::function<int64_t()> build_thread_num_;

    mutable std::mutex writer_mutex_;
    std::shared_ptr<const GrowingVectorSource<StorageType>> source_;
    std::optional<KnowhereEngine> engine_;
    GrowingOffsetMapping validity_;
    std::optional<bool> nullable_;
    int64_t accepted_row_end_{0};
    int64_t accepted_physical_count_{0};
    bool built_{false};
    bool publication_pending_{false};
    std::exception_ptr poison_;
};

}  // namespace milvus::index
