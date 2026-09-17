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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "index/contracts/query/IVectorReader.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorValidData.h"

namespace milvus::index {

class VectorIndexReader final : public IIndexReaderBase, public IVectorReader {
 public:
    VectorIndexReader(KnowhereEngine engine, VectorValidData valid);

    VectorIndexReader(uint32_t disk_ann_beamwidth,
                      KnowhereEngine engine,
                      VectorValidData valid);

    // A growing generation shares one live Add/Search engine but freezes the
    // physical prefix, nullable mapping and legacy growing search defaults at
    // publication. The query consumer may impose a shorter visible prefix.
    VectorIndexReader(KnowhereEngine engine,
                      VectorValidData valid,
                      int64_t physical_count,
                      knowhere::Json search_defaults);

    ~VectorIndexReader() override = default;

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    void
    Search(const DatasetPtr& dataset,
           const VectorSearchParams& params,
           const BitsetView& bitset,
           milvus::OpContext* op_ctx,
           SearchResult& result) const override;

    knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    Iterators(const DatasetPtr& dataset,
              const knowhere::Json& json,
              const BitsetView& bitset,
              milvus::OpContext* op_ctx) const override;

    bool
    RefineEnabled() const override;

    bool
    HasRawData() const override;

    std::vector<uint8_t>
    GetVector(const DatasetPtr& dataset) const override;

    std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr& dataset) const override;

    MetricType
    Metric() const override;

    IndexType
    KnowhereIndexType() const override;

    int64_t
    Dim() const override;

    knowhere::Json
    PrepareSearchParams(const VectorSearchParams& params) const override;

    bool
    HasValidData() const override;

    int64_t
    ValidCount() const override;

    bool
    IsRowValid(int64_t logical_offset) const override;

    int64_t
    PhysicalOffset(int64_t logical_offset) const override;

    int64_t
    LogicalOffset(int64_t physical_offset) const override;

    const milvus::OffsetMapping&
    OffsetMapping() const override;

    knowhere::expected<knowhere::DataSetPtr>
    CalcDistByIDs(const knowhere::DataSetPtr& query_dataset,
                  const BitsetView& bitset,
                  const int64_t* labels,
                  size_t labels_len,
                  bool is_cosine,
                  milvus::OpContext* op_ctx) const override;

    std::pair<std::vector<uint8_t>, std::vector<size_t>>
    GetEmbListByIds(const DatasetPtr& dataset,
                    const std::string& metric_type) const override;

 private:
    enum class Backend {
        Memory,
        Disk,
    };

    static constexpr std::array<const char*, 4> kGrowingQueryOverrides = {
        "radius", "range_filter", "drop_ratio_search", "dim_max_score_ratio"};

    bool
    IsGrowingGeneration() const;

    VectorSearchParams
    EffectiveSearchParams(const VectorSearchParams& params) const;

    void
    FillEmptySearchResult(const DatasetPtr& dataset,
                          const VectorSearchParams& params,
                          SearchResult& result) const;

    void
    SearchMemory(const DatasetPtr& dataset,
                 const VectorSearchParams& params,
                 const BitsetView& bitset,
                 milvus::OpContext* op_ctx,
                 SearchResult& result) const;

    void
    SearchDisk(const DatasetPtr& dataset,
               const VectorSearchParams& params,
               const BitsetView& bitset,
               milvus::OpContext* op_ctx,
               SearchResult& result) const;

    BitsetView
    BoundSearchBitset(const BitsetView& bitset, TargetBitmap& storage) const;

    BitsetView
    BoundIteratorBitset(const BitsetView& bitset) const;

    void
    ValidatePhysicalIds(const DatasetPtr& dataset, const char* operation) const;

    void
    ValidatePhysicalIds(const int64_t* ids,
                        size_t count,
                        const char* operation) const;

    bool
    IsEmptyEngine() const;

    KnowhereEngine engine_;
    VectorValidData valid_;
    Backend backend_{Backend::Memory};
    uint32_t disk_ann_beamwidth_{8};
    int64_t physical_count_;
    std::optional<knowhere::Json> growing_search_defaults_;
};

}  // namespace milvus::index
