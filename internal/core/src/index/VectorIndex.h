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
#include "knowhere/index/index.h"
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

struct EmbListRetrieveResult {
    std::vector<uint8_t> raw_data;
    std::vector<size_t> offsets;
};

struct VectorRetrieveResult {
    std::vector<uint8_t> raw_data;
};

struct SparseVectorRetrieveResult {
    std::unique_ptr<const knowhere::sparse::SparseRow<SparseValueType>[]>
        sparse_data;
};

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
                    const BitsetView& bitset) const {
        ThrowInfo(NotImplemented,
                  "VectorIndex:" + this->GetIndexType() +
                      " didn't implement VectorIterator interface, "
                      "there must be sth wrong in the code");
    }

    virtual const bool
    HasRawData() const override = 0;

    virtual bool
    IsIndexRefineEnabled() const = 0;

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

    virtual VectorRetrieveResult
    GetVector(const DatasetPtr dataset) const = 0;

    /**
     * @brief Retrieve embedding lists by their IDs from the index.
     *
     * @param dataset Contains the embedding list IDs (rows = count, ids = el_ids)
     * @param metric_type The metric type (e.g., MAX_SIM, MAX_SIM_IP)
     * @return A pair of (raw_vector_data, offsets) where offsets has size count+1
     *         and raw_vector_data contains all vectors concatenated.
     */
    virtual EmbListRetrieveResult
    GetEmbListByIds(const DatasetPtr dataset,
                    const std::string& metric_type) const {
        ThrowInfo(NotImplemented,
                  "GetEmbListByIds not supported for current index type");
    }

    virtual SparseVectorRetrieveResult
    GetSparseVector(const DatasetPtr dataset) const = 0;

    virtual knowhere::IdMap&
    GetIdMap() = 0;

    virtual const knowhere::IdMap&
    GetIdMap() const = 0;

    virtual void
    SetIdMapUseLock(bool use_lock) {
        GetIdMap().SetUseLock(use_lock);
    }

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

 protected:
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

    template <typename T>
    VectorRetrieveResult
    GetVectorByIdsFromIndex(
        const DatasetPtr dataset,
        const knowhere::Index<knowhere::IndexNode>& index) const {
        VectorRetrieveResult result;
        if (IndexIsSparse(GetIndexType())) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to get vector, index is sparse");
        }
        if (dataset->GetRows() == 0) {
            return result;
        }

        auto res = index.GetVectorByIds(dataset);
        if (!res.has_value()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to get vector, {}: {}",
                      KnowhereStatusString(res.error()),
                      res.what());
        }
        result.raw_data = DecodeVectorByIdsResult<T>(res.value());
        return result;
    }

    SparseVectorRetrieveResult
    GetSparseVectorByIdsFromIndex(
        const DatasetPtr dataset,
        const knowhere::Index<knowhere::IndexNode>& index) const {
        SparseVectorRetrieveResult result;
        if (dataset->GetRows() == 0) {
            return result;
        }

        auto res = index.GetVectorByIds(dataset);
        if (!res.has_value()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to get vector, {}: {}",
                      KnowhereStatusString(res.error()),
                      res.what());
        }
        res.value()->SetIsOwner(false);
        result.sparse_data.reset(
            static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                res.value()->GetTensor()));
        return result;
    }

    template <typename T>
    EmbListRetrieveResult
    GetEmbListByIdsFromIndex(
        const DatasetPtr dataset,
        const std::string& metric_type,
        const knowhere::Index<knowhere::IndexNode>& index,
        const std::vector<size_t>& empty_emb_list_offsets) const {
        EmbListRetrieveResult result;
        if (dataset->GetRows() == 0) {
            result.offsets = {0};
            return result;
        }
        if (!empty_emb_list_offsets.empty()) {
            auto ids = dataset->GetIds();
            auto rows = dataset->GetRows();
            std::vector<int64_t> compact_ids;
            ids = index.GetIdMap().MapOutToIn(ids, rows, compact_ids);
            auto emb_list_count =
                static_cast<int64_t>(empty_emb_list_offsets.size()) - 1;
            for (int64_t i = 0; i < rows; ++i) {
                AssertInfo(ids[i] >= 0 && ids[i] < emb_list_count,
                           "emb list id {} out of range {}",
                           ids[i],
                           emb_list_count);
            }
            result.offsets.assign(rows + 1, 0);
            return result;
        }

        auto res = index.GetEmbListByIds(dataset, metric_type);
        if (!res.has_value()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to get emb list, {}: {}",
                      KnowhereStatusString(res.error()),
                      res.what());
        }
        auto [raw_data, offsets] = DecodeEmbListByIdsResult<T>(res.value());
        result.raw_data = std::move(raw_data);
        result.offsets = std::move(offsets);
        return result;
    }

    void
    FinalizeIdMapForEmptyIndex(knowhere::Index<knowhere::IndexNode>& index,
                               ErrorCode error_code,
                               const std::string& context) const {
        if (index.GetIdMap().GetSnapshot().GetCount() == 0) {
            return;
        }

        auto stat = index.Node()->FinalizeIdMap();
        if (stat != knowhere::Status::success) {
            ThrowInfo(error_code,
                      "failed to finalize id map for {}, {}",
                      context,
                      KnowhereStatusString(stat));
        }
    }

 private:
    MetricType metric_type_;
    int64_t dim_;
};

}  // namespace milvus::index
