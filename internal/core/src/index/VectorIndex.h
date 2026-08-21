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

#include <cstring>
#include "common/FastMem.h"
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <boost/dynamic_bitset.hpp>

#include "Utils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/id_map.h"
#include "knowhere/index/index_factory.h"
#include "index/Index.h"
#include "common/Types.h"
#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"
#include "common/OpContext.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"

namespace milvus::index {

// valid data keys for nullable vector index serialization
constexpr const char* VALID_DATA_KEY = "valid_data";
constexpr const char* VALID_DATA_COUNT_KEY = "valid_data_count";

class VectorIndex : public IndexBase {
 public:
    explicit VectorIndex(const IndexType& index_type,
                         const MetricType& metric_type)
        : IndexBase(index_type), metric_type_(metric_type) {
    }

 public:
    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) override {
        ThrowInfo(Unsupported,
                  "vector index don't support build index with raw data");
    };

    virtual void
    AddWithDataset(const DatasetPtr& dataset, const Config& config) {
        ThrowInfo(Unsupported, "vector index don't support add with dataset");
    }

    virtual void
    Query(const DatasetPtr dataset,
          const SearchInfo& search_info,
          const BitsetView& bitset,
          milvus::OpContext* op_context,
          SearchResult& search_result) const = 0;

    virtual knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
    VectorIterators(const DatasetPtr dataset,
                    const knowhere::Json& json,
                    const BitsetView& bitset,
                    milvus::OpContext* = nullptr) const {
        ThrowInfo(NotImplemented,
                  "VectorIndex:" + this->GetIndexType() +
                      " didn't implement VectorIterator interface, "
                      "there must be sth wrong in the code");
    }

    virtual const bool
    HasRawData() const override = 0;

    virtual bool
    IsIndexRefineEnabled() const = 0;

    virtual knowhere::IdMap&
    GetIdMap() = 0;

    virtual const knowhere::IdMap&
    GetIdMap() const = 0;

    virtual void
    SetIdMapType(knowhere::IdMap::Type type) {
        GetIdMap().SetType(type);
    }

    virtual knowhere::expected<knowhere::DataSetPtr>
    CalcDistByIDs(const knowhere::DataSetPtr query_dataset,
                  const BitsetView& bitset,
                  const int64_t* labels,
                  size_t labels_len,
                  bool is_cosine,
                  milvus::OpContext* op_context = nullptr) const {
        return knowhere::expected<knowhere::DataSetPtr>::Err(
            knowhere::Status::not_implemented,
            "CalcDistByIDs not supported for current index type");
    }

    virtual std::vector<uint8_t>
    GetVector(const DatasetPtr dataset) const = 0;

    /**
     * @brief Retrieve embedding lists by their IDs from the index.
     *
     * @param dataset Contains the embedding list IDs (rows = count, ids = el_ids)
     * @param metric_type The metric type (e.g., MAX_SIM, MAX_SIM_IP)
     * @return A pair of (raw_vector_data, offsets) where offsets has size count+1
     *         and raw_vector_data contains all vectors concatenated.
     */
    virtual std::pair<std::vector<uint8_t>, std::vector<size_t>>
    GetEmbListByIds(const DatasetPtr dataset,
                    const std::string& metric_type) const {
        ThrowInfo(NotImplemented,
                  "GetEmbListByIds not supported for current index type");
    }

    virtual std::unique_ptr<
        const knowhere::sparse::SparseRow<SparseValueType>[]>
    GetSparseVector(const DatasetPtr dataset) const = 0;

    IndexType
    GetIndexType() const {
        return index_type_;
    }

    MetricType
    GetMetricType() const {
        return metric_type_;
    }

    int64_t
    GetDim() const {
        return dim_;
    }

    void
    SetDim(int64_t dim) {
        dim_ = dim;
    }

    virtual void
    CleanLocalData() {
    }

    virtual void
    CheckCompatible(const IndexVersion& version) {
        std::string err_msg =
            "version not support : " + std::to_string(version) +
            " , knowhere current version " +
            std::to_string(
                knowhere::Version::GetCurrentVersion().VersionNumber());
        AssertInfo(
            knowhere::Version::VersionSupport(knowhere::Version(version)),
            err_msg);
    }

    virtual bool
    IsMmapSupported() const override {
        return knowhere::IndexFactory::Instance().FeatureCheck(
            index_type_, knowhere::feature::MMAP);
    }

    knowhere::Json
    PrepareSearchParams(const SearchInfo& search_info) const {
        knowhere::Json search_cfg = search_info.search_params_;

        search_cfg[knowhere::meta::METRIC_TYPE] = search_info.metric_type_;
        search_cfg[knowhere::meta::TOPK] = search_info.topk_;

        // save trace context into search conf
        if (search_info.trace_ctx_.traceID != nullptr &&
            search_info.trace_ctx_.spanID != nullptr) {
            search_cfg[knowhere::meta::TRACE_ID] =
                tracer::GetTraceIDAsHexStr(&search_info.trace_ctx_);
            search_cfg[knowhere::meta::SPAN_ID] =
                tracer::GetSpanIDAsHexStr(&search_info.trace_ctx_);
            search_cfg[knowhere::meta::TRACE_FLAGS] =
                search_info.trace_ctx_.traceFlags;
        }

        return search_cfg;
    }

    // Indexed nullable mapping is owned by Knowhere IdMap. Milvus only exposes
    // logical-row helpers here; raw column physical mapping stays in OffsetMapping.
    bool
    IsRowValid(int64_t logical_offset) const {
        const auto& id_map = GetIdMap();
        if (id_map.ValidBitmap().empty()) {
            return true;
        }
        return id_map.IsValidOutId(logical_offset);
    }

    bool
    HasValidData() const {
        return !GetIdMap().ValidBitmap().empty();
    }

    int64_t
    GetValidCount() const {
        return static_cast<int64_t>(GetIdMap().InCount());
    }

 protected:
    void
    CheckEmptyEmbListIds(const int64_t* ids,
                         int64_t rows,
                         int64_t fallback_list_count) const {
        AssertInfo(rows >= 0, "emb list ids row count is negative");
        if (rows == 0) {
            return;
        }
        AssertInfo(ids != nullptr, "emb list ids are null");

        const auto& id_map = GetIdMap();
        const bool has_valid_data = !id_map.ValidBitmap().empty();
        const auto logical_count = has_valid_data
                                       ? static_cast<int64_t>(id_map.OutCount())
                                       : fallback_list_count;
        for (int64_t i = 0; i < rows; ++i) {
            const auto id = ids[i];
            AssertInfo(id >= 0 && id < logical_count,
                       "emb list id {} out of range {}",
                       id,
                       logical_count);
            if (has_valid_data) {
                AssertInfo(
                    id_map.IsValidOutId(id), "emb list id {} is null", id);
            }
        }
    }

    template <typename T>
    static std::vector<uint8_t>
    DecodeVectorByIdsResult(const knowhere::DataSetPtr& result) {
        auto tensor = result->GetTensor();
        auto row_num = result->GetRows();
        auto dim = result->GetDim();
        size_t data_size =
            static_cast<size_t>(milvus::GetVecRowSize<T>(dim)) * row_num;
        std::vector<uint8_t> raw_data(data_size);
        milvus::fastmem::FastMemcpy(raw_data.data(), tensor, data_size);
        return raw_data;
    }

    template <typename T>
    static std::pair<std::vector<uint8_t>, std::vector<size_t>>
    DecodeEmbListByIdsResult(const knowhere::DataSetPtr& result) {
        auto tensor = result->GetTensor();
        auto dim = result->GetDim();
        auto num_el_ids = result->GetRows();
        const size_t* offsets_ptr =
            result->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
        AssertInfo(offsets_ptr != nullptr,
                   "EMB_LIST_OFFSET not found in result");

        size_t total_vecs = offsets_ptr[num_el_ids];
        size_t data_size =
            static_cast<size_t>(milvus::GetVecRowSize<T>(dim)) * total_vecs;
        std::vector<uint8_t> raw_data(data_size);
        milvus::fastmem::FastMemcpy(raw_data.data(), tensor, data_size);

        std::vector<size_t> offsets(offsets_ptr, offsets_ptr + num_el_ids + 1);
        return {std::move(raw_data), std::move(offsets)};
    }

 private:
    MetricType metric_type_;
    int64_t dim_;
};

}  // namespace milvus::index
