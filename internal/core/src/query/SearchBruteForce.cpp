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
    bool is_float_vec_data_type = IsFloatVectorDataType(data_type);
    bool is_float_metric_type = IsFloatMetricType(metric_type);
    AssertInfo(is_float_vec_data_type == is_float_metric_type,
               "[BruteForceSearch] Data type and metric type miss-match");
}

knowhere::Json
PrepareBFSearchParams(const SearchInfo& search_info) {
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

SubSearchResult
BruteForceSearch(const dataset::SearchDataset& dataset,
                 const void* chunk_data_raw,
                 int64_t chunk_rows,
                 const SearchInfo& search_info,
                 const BitsetView& bitset,
                 DataType data_type) {
    SubSearchResult sub_result(dataset.num_queries,
                               dataset.topk,
                               dataset.metric_type,
                               dataset.round_decimal);
    auto nq = dataset.num_queries;
    auto dim = dataset.dim;
    auto topk = dataset.topk;

    auto base_dataset = knowhere::GenDataSet(chunk_rows, dim, chunk_data_raw);
    auto query_dataset = knowhere::GenDataSet(nq, dim, dataset.query_data);
    if (data_type == DataType::VECTOR_SPARSE_FLOAT) {
        base_dataset->SetIsSparse(true);
        query_dataset->SetIsSparse(true);
    }
    auto search_cfg = PrepareBFSearchParams(search_info);
    // `range_search_k` is only used as one of the conditions for iterator early termination.
    // not gurantee to return exactly `range_search_k` results, which may be more or less.
    // set it to -1 will return all results in the range.
    search_cfg[knowhere::meta::RANGE_SEARCH_K] = topk;

    sub_result.mutable_seg_offsets().resize(nq * topk);
    sub_result.mutable_distances().resize(nq * topk);

    if (search_cfg.contains(RADIUS)) {
        if (search_cfg.contains(RANGE_FILTER)) {
            CheckRangeSearchParam(search_cfg[RADIUS],
                                  search_cfg[RANGE_FILTER],
                                  search_info.metric_type_);
        }
        knowhere::expected<knowhere::DataSetPtr> res;
        if (data_type == DataType::VECTOR_FLOAT) {
            res = knowhere::BruteForce::RangeSearch<float>(
                base_dataset, query_dataset, search_cfg, bitset);
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            res = knowhere::BruteForce::RangeSearch<float16>(
                base_dataset, query_dataset, search_cfg, bitset);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            res = knowhere::BruteForce::RangeSearch<bfloat16>(
                base_dataset, query_dataset, search_cfg, bitset);
        } else if (data_type == DataType::VECTOR_BINARY) {
            res = knowhere::BruteForce::RangeSearch<uint8_t>(
                base_dataset, query_dataset, search_cfg, bitset);
        } else if (data_type == DataType::VECTOR_SPARSE_FLOAT) {
            res = knowhere::BruteForce::RangeSearch<
                knowhere::sparse::SparseRow<float>>(
                base_dataset, query_dataset, search_cfg, bitset);
        } else {
            PanicInfo(
                ErrorCode::Unsupported,
                "Unsupported dataType for chunk brute force range search:{}",
                data_type);
        }
        milvus::tracer::AddEvent("knowhere_finish_BruteForce_RangeSearch");
        if (!res.has_value()) {
            PanicInfo(KnowhereError,
                      "Brute force range search fail: {}, {}",
                      KnowhereStatusString(res.error()),
                      res.what());
        }
        auto result =
            ReGenRangeSearchResult(res.value(), topk, nq, dataset.metric_type);
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
                bitset);
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            stat = knowhere::BruteForce::SearchWithBuf<float16>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            stat = knowhere::BruteForce::SearchWithBuf<bfloat16>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset);
        } else if (data_type == DataType::VECTOR_BINARY) {
            stat = knowhere::BruteForce::SearchWithBuf<uint8_t>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset);
        } else if (data_type == DataType::VECTOR_SPARSE_FLOAT) {
            stat = knowhere::BruteForce::SearchSparseWithBuf(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset);
        } else {
            PanicInfo(ErrorCode::Unsupported,
                      "Unsupported dataType for chunk brute force search:{}",
                      data_type);
        }
        milvus::tracer::AddEvent("knowhere_finish_BruteForce_SearchWithBuf");
        if (stat != knowhere::Status::success) {
            PanicInfo(KnowhereError,
                      "Brute force search fail: " + KnowhereStatusString(stat));
        }
    }
    sub_result.round_values();
    return sub_result;
}

SubSearchResult
BruteForceSearchIterators(const dataset::SearchDataset& dataset,
                          const void* chunk_data_raw,
                          int64_t chunk_rows,
                          const SearchInfo& search_info,
                          const BitsetView& bitset,
                          DataType data_type) {
    auto nq = dataset.num_queries;
    auto dim = dataset.dim;
    auto base_dataset = knowhere::GenDataSet(chunk_rows, dim, chunk_data_raw);
    auto query_dataset = knowhere::GenDataSet(nq, dim, dataset.query_data);
    if (data_type == DataType::VECTOR_SPARSE_FLOAT) {
        base_dataset->SetIsSparse(true);
        query_dataset->SetIsSparse(true);
    }
    auto search_cfg = PrepareBFSearchParams(search_info);

    knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
        iterators_val;
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            iterators_val = knowhere::BruteForce::AnnIterator<float>(
                base_dataset, query_dataset, search_cfg, bitset);
            break;
        case DataType::VECTOR_FLOAT16:
            iterators_val = knowhere::BruteForce::AnnIterator<float16>(
                base_dataset, query_dataset, search_cfg, bitset);
            break;
        case DataType::VECTOR_BFLOAT16:
            iterators_val = knowhere::BruteForce::AnnIterator<bfloat16>(
                base_dataset, query_dataset, search_cfg, bitset);
            break;
        case DataType::VECTOR_SPARSE_FLOAT:
            iterators_val = knowhere::BruteForce::AnnIterator<
                knowhere::sparse::SparseRow<float>>(
                base_dataset, query_dataset, search_cfg, bitset);
            break;
        default:
            PanicInfo(ErrorCode::Unsupported,
                      "Unsupported dataType for chunk brute force iterator:{}",
                      data_type);
    }
    if (iterators_val.has_value()) {
        AssertInfo(
            iterators_val.value().size() == nq,
            "Wrong state, initialized knowhere_iterators count:{} is not "
            "equal to nq:{} for single chunk",
            iterators_val.value().size(),
            nq);
        SubSearchResult subSearchResult(dataset.num_queries,
                                        dataset.topk,
                                        dataset.metric_type,
                                        dataset.round_decimal,
                                        iterators_val.value());
        return std::move(subSearchResult);
    } else {
        LOG_ERROR(
            "Failed to get valid knowhere brute-force-iterators from chunk, "
            "terminate search_group_by operation");
        PanicInfo(ErrorCode::Unsupported,
                  "Returned knowhere brute-force-iterator has non-ready "
                  "iterators inside, terminate search_group_by operation");
    }
}

}  // namespace milvus::query
