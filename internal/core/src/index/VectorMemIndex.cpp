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

#include "index/VectorMemIndex.h"

#include <cmath>
#include "index/Meta.h"
#include "index/Utils.h"
#include "exceptions/EasyAssert.h"
#include "config/ConfigKnowhere.h"

#include "knowhere/factory.h"
#include "knowhere/comp/time_recorder.h"
#include "common/BitsetView.h"
#include "common/Slice.h"
#include "common/Consts.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"

namespace milvus::index {

VectorMemIndex::VectorMemIndex(const IndexType& index_type,
                               const MetricType& metric_type)
    : VectorIndex(index_type, metric_type) {
    AssertInfo(!is_unsupported(index_type, metric_type),
               index_type + " doesn't support metric: " + metric_type);

    index_ = knowhere::IndexFactory::Instance().Create(GetIndexType());
}

BinarySet
VectorMemIndex::Serialize(const Config& config) {
    knowhere::BinarySet ret;
    auto stat = index_.Serialize(ret);
    if (stat != knowhere::Status::success)
        PanicCodeInfo(ErrorCodeEnum::UnexpectedError,
                      "failed to serialize index, " + MatchKnowhereError(stat));
    milvus::Disassemble(ret);

    return ret;
}

void
VectorMemIndex::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    auto stat = index_.Deserialize(binary_set);
    if (stat != knowhere::Status::success)
        PanicCodeInfo(
            ErrorCodeEnum::UnexpectedError,
            "failed to Deserialize index, " + MatchKnowhereError(stat));
    SetDim(index_.Dim());
}

void
VectorMemIndex::BuildWithDataset(const DatasetPtr& dataset,
                                 const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    SetDim(dataset->GetDim());

    knowhere::TimeRecorder rc("BuildWithoutIds", 1);
    auto stat = index_.Build(*dataset, index_config);
    if (stat != knowhere::Status::success)
        PanicCodeInfo(ErrorCodeEnum::BuildIndexError,
                      "failed to build index, " + MatchKnowhereError(stat));
    rc.ElapseFromBegin("Done");
    SetDim(index_.Dim());
}

void
VectorMemIndex::AddWithDataset(const DatasetPtr& dataset,
                               const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    knowhere::TimeRecorder rc("AddWithDataset", 1);
    auto stat = index_.Add(*dataset, index_config);
    if (stat != knowhere::Status::success)
        PanicCodeInfo(ErrorCodeEnum::BuildIndexError,
                      "failed to append index, " + MatchKnowhereError(stat));
    rc.ElapseFromBegin("Done");
}

std::unique_ptr<SearchResult>
VectorMemIndex::Query(const DatasetPtr dataset,
                      const SearchInfo& search_info,
                      const BitsetView& bitset) {
    //    AssertInfo(GetMetricType() == search_info.metric_type_,
    //               "Metric type of field index isn't the same with search info");

    auto num_queries = dataset->GetRows();
    knowhere::Json search_conf = search_info.search_params_;
    auto topk = search_info.topk_;
    // TODO :: check dim of search data
    auto final = [&] {
        search_conf[knowhere::meta::TOPK] = topk;
        search_conf[knowhere::meta::METRIC_TYPE] = GetMetricType();
        auto index_type = GetIndexType();
        if (CheckKeyInConfig(search_conf, RADIUS)) {
            if (CheckKeyInConfig(search_conf, RANGE_FILTER)) {
                CheckRangeSearchParam(search_conf[RADIUS],
                                      search_conf[RANGE_FILTER],
                                      GetMetricType());
            }
            auto res = index_.RangeSearch(*dataset, search_conf, bitset);
            if (!res.has_value()) {
                PanicCodeInfo(ErrorCodeEnum::UnexpectedError,
                              "failed to range search, " +
                                  MatchKnowhereError(res.error()));
            }
            return ReGenRangeSearchResult(
                res.value(), topk, num_queries, GetMetricType());
        } else {
            auto res = index_.Search(*dataset, search_conf, bitset);
            if (!res.has_value()) {
                PanicCodeInfo(
                    ErrorCodeEnum::UnexpectedError,
                    "failed to search, " + MatchKnowhereError(res.error()));
            }
            return res.value();
        }
    }();

    auto ids = final->GetIds();
    float* distances = const_cast<float*>(final->GetDistance());
    final->SetIsOwner(true);
    auto round_decimal = search_info.round_decimal_;
    auto total_num = num_queries * topk;

    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = std::round(distances[i] * multiplier) / multiplier;
        }
    }
    auto result = std::make_unique<SearchResult>();
    result->seg_offsets_.resize(total_num);
    result->distances_.resize(total_num);
    result->total_nq_ = num_queries;
    result->unity_topK_ = topk;

    std::copy_n(ids, total_num, result->seg_offsets_.data());
    std::copy_n(distances, total_num, result->distances_.data());

    return result;
}

const bool
VectorMemIndex::HasRawData() const {
    return index_.HasRawData(GetMetricType());
}

const std::vector<uint8_t>
VectorMemIndex::GetVector(const DatasetPtr dataset) const {
    auto res = index_.GetVectorByIds(*dataset);
    if (!res.has_value()) {
        PanicCodeInfo(
            ErrorCodeEnum::UnexpectedError,
            "failed to get vector, " + MatchKnowhereError(res.error()));
    }
    auto index_type = GetIndexType();
    auto tensor = res.value()->GetTensor();
    auto row_num = res.value()->GetRows();
    auto dim = res.value()->GetDim();
    int64_t data_size;
    if (is_in_bin_list(index_type)) {
        data_size = dim / 8 * row_num;
    } else {
        data_size = dim * row_num * sizeof(float);
    }
    std::vector<uint8_t> raw_data;
    raw_data.resize(data_size);
    memcpy(raw_data.data(), tensor, data_size);
    return raw_data;
}

}  // namespace milvus::index
