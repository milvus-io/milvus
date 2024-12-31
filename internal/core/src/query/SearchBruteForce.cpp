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
    auto query_dataset = knowhere::GenDataSet(
        query_ds.num_queries, query_ds.dim, query_ds.query_data);
    if (data_type == DataType::VECTOR_SPARSE_FLOAT) {
        base_dataset->SetIsSparse(true);
        query_dataset->SetIsSparse(true);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        //todo: if knowhere support real fp16/bf16 bf, remove convert
        base_dataset =
            knowhere::ConvertFromDataTypeIfNeeded<bfloat16>(base_dataset);
        query_dataset =
            knowhere::ConvertFromDataTypeIfNeeded<bfloat16>(query_dataset);
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        //todo: if knowhere support real fp16/bf16 bf, remove convert
        base_dataset =
            knowhere::ConvertFromDataTypeIfNeeded<float16>(base_dataset);
        query_dataset =
            knowhere::ConvertFromDataTypeIfNeeded<float16>(query_dataset);
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
                 DataType data_type) {
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
            //todo: if knowhere support real fp16/bf16 bf, change it
            res = knowhere::BruteForce::RangeSearch<float>(
                base_dataset, query_dataset, search_cfg, bitset);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            //todo: if knowhere support real fp16/bf16 bf, change it
            res = knowhere::BruteForce::RangeSearch<float>(
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
                bitset);
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            //todo: if knowhere support real fp16/bf16 bf, change it
            stat = knowhere::BruteForce::SearchWithBuf<float>(
                base_dataset,
                query_dataset,
                sub_result.mutable_seg_offsets().data(),
                sub_result.mutable_distances().data(),
                search_cfg,
                bitset);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            //todo: if knowhere support real fp16/bf16 bf, change it
            stat = knowhere::BruteForce::SearchWithBuf<float>(
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

knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
DispatchBruteForceIteratorByDataType(const knowhere::DataSetPtr& base_dataset,
                                     const knowhere::DataSetPtr& query_dataset,
                                     const knowhere::Json& config,
                                     const BitsetView& bitset,
                                     const milvus::DataType& data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            return knowhere::BruteForce::AnnIterator<float>(
                base_dataset, query_dataset, config, bitset);
            break;
        case DataType::VECTOR_FLOAT16:
            //todo: if knowhere support real fp16/bf16 bf, change it
            return knowhere::BruteForce::AnnIterator<float>(
                base_dataset, query_dataset, config, bitset);
            break;
        case DataType::VECTOR_BFLOAT16:
            //todo: if knowhere support real fp16/bf16 bf, change it
            return knowhere::BruteForce::AnnIterator<float>(
                base_dataset, query_dataset, config, bitset);
            break;
        case DataType::VECTOR_SPARSE_FLOAT:
            return knowhere::BruteForce::AnnIterator<
                knowhere::sparse::SparseRow<float>>(
                base_dataset, query_dataset, config, bitset);
            break;
        default:
            PanicInfo(ErrorCode::Unsupported,
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
        PanicInfo(ErrorCode::Unsupported,
                  "Returned knowhere brute-force-iterator has non-ready "
                  "iterators inside, terminate search_group_by operation");
    }
}

}  // namespace milvus::query
