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

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "common/Tracer.h"
#include "SearchBruteForce.h"
#include "SubSearchResult.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
namespace milvus::query {

void
CheckBruteForceSearchParam(const FieldMeta& field,
                           const SearchInfo& search_info) {
    auto data_type = field.get_data_type();
    auto& metric_type = search_info.metric_type_;

    AssertInfo(datatype_is_vector(data_type),
               "[BruteForceSearch] Data type isn't vector type");
    bool is_float_data_type = (data_type == DataType::VECTOR_FLOAT ||
                               data_type == DataType::VECTOR_FLOAT16);
    bool is_float_metric_type = IsFloatMetricType(metric_type);
    AssertInfo(is_float_data_type == is_float_metric_type,
               "[BruteForceSearch] Data type and metric type miss-match");
}

SubSearchResult
BruteForceSearch(const dataset::SearchDataset& dataset,
                 const void* chunk_data_raw,
                 int64_t chunk_rows,
                 const knowhere::Json& conf,
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

    if (data_type == DataType::VECTOR_FLOAT16) {
        // Todo: Temporarily use cast to float32 to achieve, need to optimize
        // first, First, transfer the cast to knowhere part
        // second, knowhere partially supports float16 and removes the forced conversion to float32
        auto xb = base_dataset->GetTensor();
        std::vector<float> float_xb(base_dataset->GetRows() *
                                    base_dataset->GetDim());

        auto xq = query_dataset->GetTensor();
        std::vector<float> float_xq(query_dataset->GetRows() *
                                    query_dataset->GetDim());

        auto fp16_xb = static_cast<const float16*>(xb);
        for (int i = 0; i < base_dataset->GetRows() * base_dataset->GetDim();
             i++) {
            float_xb[i] = (float)fp16_xb[i];
        }

        auto fp16_xq = static_cast<const float16*>(xq);
        for (int i = 0; i < query_dataset->GetRows() * query_dataset->GetDim();
             i++) {
            float_xq[i] = (float)fp16_xq[i];
        }
        void* void_ptr_xb = static_cast<void*>(float_xb.data());
        void* void_ptr_xq = static_cast<void*>(float_xq.data());
        base_dataset = knowhere::GenDataSet(chunk_rows, dim, void_ptr_xb);
        query_dataset = knowhere::GenDataSet(nq, dim, void_ptr_xq);
    }

    auto config = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, dataset.metric_type},
        {knowhere::meta::DIM, dim},
        {knowhere::meta::TOPK, topk},
    };

    sub_result.mutable_seg_offsets().resize(nq * topk);
    sub_result.mutable_distances().resize(nq * topk);

    if (conf.contains(RADIUS)) {
        config[RADIUS] = conf[RADIUS].get<float>();
        if (conf.contains(RANGE_FILTER)) {
            config[RANGE_FILTER] = conf[RANGE_FILTER].get<float>();
            CheckRangeSearchParam(
                config[RADIUS], config[RANGE_FILTER], dataset.metric_type);
        }
        auto res = knowhere::BruteForce::RangeSearch(
            base_dataset, query_dataset, config, bitset);
        milvus::tracer::AddEvent("knowhere_finish_BruteForce_RangeSearch");
        if (!res.has_value()) {
            PanicInfo(KnowhereError,
                      fmt::format("failed to range search: {}: {}",
                                  KnowhereStatusString(res.error()),
                                  res.what()));
        }
        auto result =
            ReGenRangeSearchResult(res.value(), topk, nq, dataset.metric_type);
        milvus::tracer::AddEvent("ReGenRangeSearchResult");
        std::copy_n(
            GetDatasetIDs(result), nq * topk, sub_result.get_seg_offsets());
        std::copy_n(
            GetDatasetDistance(result), nq * topk, sub_result.get_distances());
    } else {
        auto stat = knowhere::BruteForce::SearchWithBuf(
            base_dataset,
            query_dataset,
            sub_result.mutable_seg_offsets().data(),
            sub_result.mutable_distances().data(),
            config,
            bitset);
        milvus::tracer::AddEvent("knowhere_finish_BruteForce_SearchWithBuf");
        if (stat != knowhere::Status::success) {
            throw SegcoreError(
                KnowhereError,
                "invalid metric type, " + KnowhereStatusString(stat));
        }
    }
    sub_result.round_values();
    return sub_result;
}

}  // namespace milvus::query
