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

#include <string>
#include <vector>

#include "SearchBruteForce.h"
#include "SubSearchResult.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/index/index_node.h"
#include "log/Log.h"

namespace milvus::query {

void
CheckBruteForceSearchParam(const FieldMeta& field,
                           const SearchInfo& search_info) {
    auto data_type = field.get_data_type();
    auto& metric_type = search_info.metric_type_;

    AssertInfo(IsVectorDataType(data_type),
               "[BruteForceSearch] Data type isn't vector type");
    if (IsBinaryVectorDataType(data_type)) {
        AssertInfo(IsBinaryVectorMetricType(metric_type),
                   "[BruteForceSearch] Binary vector, data type and metric "
                   "type miss-match");
    } else if (IsFloatVectorDataType(data_type)) {
        AssertInfo(IsFloatVectorMetricType(metric_type),
                   "[BruteForceSearch] Float vector, data type and metric type "
                   "miss-match");
    } else if (IsIntVectorDataType(data_type)) {
        AssertInfo(IsIntVectorMetricType(metric_type),
                   "[BruteForceSearch] Int vector, data type and metric type "
                   "miss-match");
    } else {
        AssertInfo(IsVectorDataType(data_type),
                   "[BruteForceSearch] Unsupported vector data type");
    }
}

knowhere::Json
PrepareBFSearchParams(const SearchInfo& search_info,
                      const std::map<std::string, std::string>& index_info) {
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

    if (search_info.metric_type_ == knowhere::metric::BM25) {
        search_cfg[knowhere::meta::BM25_AVGDL] =
            search_info.search_params_[knowhere::meta::BM25_AVGDL];
        search_cfg[knowhere::meta::BM25_K1] =
            std::stof(index_info.at(knowhere::meta::BM25_K1));
        search_cfg[knowhere::meta::BM25_B] =
            std::stof(index_info.at(knowhere::meta::BM25_B));
    }
    return search_cfg;
}

std::pair<knowhere::DataSetPtr, knowhere::DataSetPtr>
PrepareBFDataSet(const dataset::SearchDataset& query_ds,
                 const dataset::RawDataset& raw_ds,
                 DataType data_type) {
    auto base_dataset =
        knowhere::GenDataSet(raw_ds.num_raw_data, raw_ds.dim, raw_ds.raw_data);
    if (raw_ds.raw_data_lims != nullptr) {
        // knowhere::DataSet count vectors in a flattened manner where as the num_raw_data here is the number
        // of embedding lists where each embedding list contains multiple vectors. So we should use the last element
        // in lims which equals to the total number of vectors.
        base_dataset->SetLims(raw_ds.raw_data_lims);
        // the length of lims equals to the number of embedding lists + 1
        base_dataset->SetRows(raw_ds.raw_data_lims[raw_ds.num_raw_data]);
    }

    auto query_dataset = knowhere::GenDataSet(
        query_ds.num_queries, query_ds.dim, query_ds.query_data);
    if (query_ds.query_lims != nullptr) {
        // ditto
        query_dataset->SetLims(query_ds.query_lims);
        query_dataset->SetRows(query_ds.query_lims[query_ds.num_queries]);
    }

    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        base_dataset->SetIsSparse(true);
        query_dataset->SetIsSparse(true);
    }
    base_dataset->SetTensorBeginId(raw_ds.begin_id);
    return std::make_pair(query_dataset, base_dataset);
};

SubSearchResult
BruteForceSearch(const dataset::SearchDataset& query_ds,
                 const dataset::RawDataset& raw_ds,
                 const SearchInfo& search_info,
                 const std::map<std::string, std::string>& index_info,
                 const BitsetView& bitset,
                 DataType data_type,
                 DataType element_type,
                 milvus::OpContext* op_context) {
    SubSearchResult sub_result(query_ds.num_queries,
                               query_ds.topk,
                               query_ds.metric_type,
                               query_ds.round_decimal);
    auto topk = query_ds.topk;
    auto nq = query_ds.num_queries;
    auto [query_dataset, base_dataset] =
        PrepareBFDataSet(query_ds, raw_ds, data_type);
    auto search_cfg = PrepareBFSearchParams(search_info, index_info);
    // `range_search_k` is only used as one of the conditions for iterator early termination.
    // not gurantee to return exactly `range_search_k` results, which may be more or less.
    // set it to -1 will return all results in the range.
    search_cfg[knowhere::meta::RANGE_SEARCH_K] = topk;
    sub_result.mutable_seg_offsets().resize(nq * topk);
    sub_result.mutable_distances().resize(nq * topk);

    // For vector array (embedding list), element type is used to determine how to operate search.
    if (data_type == DataType::VECTOR_ARRAY) {
        AssertInfo(element_type != DataType::NONE,
                   "Element type is not specified for vector array");
        data_type = element_type;
    }

    if (search_cfg.contains(RADIUS)) {
        AssertInfo(data_type != DataType::VECTOR_ARRAY,
                   "Vector array(embedding list) is not supported for range "
                   "search");

        if (search_cfg.contains(RANGE_FILTER)) {
            CheckRangeSearchParam(search_cfg[RADIUS],
                                  search_cfg[RANGE_FILTER],
                                  search_info.metric_type_);
        }
        knowhere::expected<knowhere::DataSetPtr> res;
        if (data_type == DataType::VECTOR_FLOAT) {
            res = knowhere::BruteForce::RangeSearch<float>(
                base_dataset, query_dataset, search_cfg, bitset, op_context);
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            res = knowhere::BruteForce::RangeSearch<float16>(
                base_dataset, query_dataset, search_cfg, bitset, op_context);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            res = knowhere::BruteForce::RangeSearch<bfloat16>(
                base_dataset, query_dataset, search_cfg, bitset, op_context);
        } else if (data_type == DataType::VECTOR_BINARY) {
            res = knowhere::BruteForce::RangeSearch<bin1>(
                base_dataset, query_dataset, search_cfg, bitset, op_context);
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            res = knowhere::BruteForce::RangeSearch<
                knowhere::sparse::SparseRow<SparseValueType>>(
                base_dataset, query_dataset, search_cfg, bitset, op_context);
        } else if (data_type == DataType::VECTOR_INT8) {
            res = knowhere::BruteForce::RangeSearch<int8>(
                base_dataset, query_dataset, search_cfg, bitset, op_context);
        } else {
            ThrowInfo(
                ErrorCode::Unsupported,
                "Unsupported dataType for chunk brute force range search:{}",
                data_type);
        }
        milvus::tracer::AddEvent("knowhere_finish_BruteForce_RangeSearch");
        if (!res.has_value()) {
            ThrowInfo(KnowhereError,
                      "Brute force range search fail: {}, {}",
                      KnowhereStatusString(res.error()),
                      res.what());
        }
        auto result =
            ReGenRangeSearchResult(res.value(), topk, nq, query_ds.metric_type);
        milvus::tracer::AddEvent("ReGenRangeSearchResult");
        std::copy_n(
            GetDatasetIDs(result), nq * topk, sub_result.get_seg_offsets());
        std::copy_n(
            GetDatasetDistance(result), nq * topk, sub_result.get_distances());
    } else {
        knowhere::Status stat;
        if (data_type == DataType::VECTOR_FLOAT) {
            stat = knowhere::BruteForce::SearchWithBuf<float>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset,
                op_context);
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            stat = knowhere::BruteForce::SearchWithBuf<float16>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset,
                op_context);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            stat = knowhere::BruteForce::SearchWithBuf<bfloat16>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset,
                op_context);
        } else if (data_type == DataType::VECTOR_BINARY) {
            stat = knowhere::BruteForce::SearchWithBuf<bin1>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset,
                op_context);
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            stat = knowhere::BruteForce::SearchSparseWithBuf(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset,
                op_context);
        } else if (data_type == DataType::VECTOR_INT8) {
            stat = knowhere::BruteForce::SearchWithBuf<int8>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset,
                op_context);
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      "Unsupported dataType for chunk brute force search:{}",
                      data_type);
        }
        milvus::tracer::AddEvent("knowhere_finish_BruteForce_SearchWithBuf");
        if (stat != knowhere::Status::success) {
            ThrowInfo(KnowhereError,
                      "Brute force search fail: " + KnowhereStatusString(stat));
        }
    }
    sub_result.round_values();
    return sub_result;
}

knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
DispatchBruteForceIteratorByDataType(const knowhere::DataSetPtr& base_dataset,
                                     const knowhere::DataSetPtr& query_dataset,
                                     const knowhere::Json& config,
                                     const BitsetView& bitset,
                                     milvus::DataType data_type) {
    AssertInfo(data_type != DataType::VECTOR_ARRAY,
               "VECTOR_ARRAY is not supported for brute force iterator");

    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            return knowhere::BruteForce::AnnIterator<float>(
                base_dataset, query_dataset, config, bitset);
        case DataType::VECTOR_FLOAT16:
            return knowhere::BruteForce::AnnIterator<float16>(
                base_dataset, query_dataset, config, bitset);
        case DataType::VECTOR_BFLOAT16:
            return knowhere::BruteForce::AnnIterator<bfloat16>(
                base_dataset, query_dataset, config, bitset);
        case DataType::VECTOR_SPARSE_U32_F32:
            return knowhere::BruteForce::AnnIterator<
                knowhere::sparse::SparseRow<SparseValueType>>(
                base_dataset, query_dataset, config, bitset);
        case DataType::VECTOR_INT8:
            return knowhere::BruteForce::AnnIterator<int8>(
                base_dataset, query_dataset, config, bitset);
        default:
            ThrowInfo(ErrorCode::Unsupported,
                      "Unsupported dataType for chunk brute force iterator:{}",
                      data_type);
    }
}

knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
GetBruteForceSearchIterators(
    const dataset::SearchDataset& query_ds,
    const dataset::RawDataset& raw_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    DataType data_type) {
    auto nq = query_ds.num_queries;
    auto [query_dataset, base_dataset] =
        PrepareBFDataSet(query_ds, raw_ds, data_type);
    auto search_cfg = PrepareBFSearchParams(search_info, index_info);
    return DispatchBruteForceIteratorByDataType(
        base_dataset, query_dataset, search_cfg, bitset, data_type);
}

SubSearchResult
PackBruteForceSearchIteratorsIntoSubResult(
    const dataset::SearchDataset& query_ds,
    const dataset::RawDataset& raw_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    DataType data_type) {
    auto nq = query_ds.num_queries;
    auto iterators_val = GetBruteForceSearchIterators(
        query_ds, raw_ds, search_info, index_info, bitset, data_type);
    if (iterators_val.has_value()) {
        AssertInfo(
            iterators_val.value().size() == nq,
            "Wrong state, initialized knowhere_iterators count:{} is not "
            "equal to nq:{} for single chunk",
            iterators_val.value().size(),
            nq);
        return SubSearchResult(query_ds.num_queries,
                               query_ds.topk,
                               query_ds.metric_type,
                               query_ds.round_decimal,
                               iterators_val.value());
    } else {
        LOG_ERROR(
            "Failed to get valid knowhere brute-force-iterators from chunk, "
            "terminate search_group_by operation");
        ThrowInfo(ErrorCode::Unsupported,
                  "Returned knowhere brute-force-iterator has non-ready "
                  "iterators inside, terminate search_group_by operation");
    }
}

}  // namespace milvus::query
