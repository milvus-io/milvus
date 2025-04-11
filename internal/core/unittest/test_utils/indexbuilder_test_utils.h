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

#include <tuple>
#include <map>
#include <limits>
#include <cmath>
#include <google/protobuf/text_format.h>

#include "DataGen.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/StringIndex.h"
#include "index/Utils.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "knowhere/comp/index_param.h"
#include "pb/index_cgo_msg.pb.h"
#include "storage/Types.h"
#include "knowhere/comp/index_param.h"

constexpr int64_t DIM = 16;
constexpr int64_t NQ = 10;
constexpr int64_t K = 4;

namespace indexcgo = milvus::proto::indexcgo;
namespace schemapb = milvus::proto::schema;
using MapParams = std::map<std::string, std::string>;
using milvus::indexbuilder::ScalarIndexCreator;
using ScalarTestParams = std::pair<MapParams, MapParams>;
using milvus::index::ScalarIndexPtr;
using milvus::index::StringIndexPtr;

namespace {

auto
generate_build_conf(const milvus::IndexType& index_type,
                    const milvus::MetricType& metric_type) {
    if (index_type == knowhere::IndexEnum::INDEX_FAISS_IDMAP) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {knowhere::indexparam::NLIST, "16"},
            {knowhere::indexparam::M, "4"},
            {knowhere::indexparam::NBITS, "8"},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT ||
               index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {knowhere::indexparam::NLIST, "16"},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {knowhere::indexparam::NLIST, "16"},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {knowhere::indexparam::NLIST, "16"},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_HNSW) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {knowhere::indexparam::HNSW_M, "16"},
            {knowhere::indexparam::EFCONSTRUCTION, "200"},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {milvus::index::DISK_ANN_MAX_DEGREE, std::to_string(48)},
            {milvus::index::DISK_ANN_SEARCH_LIST_SIZE, std::to_string(128)},
            {milvus::index::DISK_ANN_PQ_CODE_BUDGET, std::to_string(0.001)},
            {milvus::index::DISK_ANN_BUILD_DRAM_BUDGET, std::to_string(32)},
            {milvus::index::DISK_ANN_BUILD_THREAD_NUM, std::to_string(2)},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
               index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
        if (metric_type == knowhere::metric::BM25) {
            return knowhere::Json{
                {knowhere::meta::METRIC_TYPE, metric_type},
                {knowhere::indexparam::DROP_RATIO_BUILD, "0.1"},
                {knowhere::meta::BM25_K1, "1.2"},
                {knowhere::meta::BM25_B, "0.75"},
                {knowhere::meta::BM25_AVGDL, "100"}};
        }
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::indexparam::DROP_RATIO_BUILD, "0.1"},
        };
    } else if (index_type == knowhere::IndexEnum::INDEX_FAISS_SCANN ||
               index_type == knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
        };
    }
    return knowhere::Json();
}

template <typename DataType = float>
inline auto
generate_load_conf(const milvus::IndexType& index_type,
                   const milvus::MetricType& metric_type,
                   int64_t nb) {
    if (index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        return knowhere::Json{
            {knowhere::meta::METRIC_TYPE, metric_type},
            {knowhere::meta::DIM, std::to_string(DIM)},
            {milvus::index::DISK_ANN_LOAD_THREAD_NUM, std::to_string(2)},
            {milvus::index::DISK_ANN_SEARCH_CACHE_BUDGET,
             std::to_string(0.05 * sizeof(DataType) * nb /
                            (1024.0 * 1024.0 * 1024.0))},
        };
    }
    return knowhere::Json{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {knowhere::meta::DIM, std::to_string(DIM)},
    };
}

std::vector<milvus::IndexType>
search_with_nprobe_list() {
    static std::vector<milvus::IndexType> ret{
        knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
        knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
        knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
    };
    return ret;
}

auto
generate_search_conf(const milvus::IndexType& index_type,
                     const milvus::MetricType& metric_type) {
    auto conf = milvus::Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
    };

    if (milvus::is_in_list<milvus::IndexType>(index_type,
                                              search_with_nprobe_list)) {
        conf[knowhere::indexparam::NPROBE] = 4;
    } else if (index_type == knowhere::IndexEnum::INDEX_HNSW) {
        conf[knowhere::indexparam::EF] = 200;
    } else if (index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        conf[milvus::index::DISK_ANN_QUERY_LIST] = K * 2;
    }
    return conf;
}

auto
generate_range_search_conf(const milvus::IndexType& index_type,
                           const milvus::MetricType& metric_type) {
    auto conf = milvus::Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
    };

    if (metric_type == knowhere::metric::IP) {
        conf[knowhere::meta::RADIUS] = 0.1;
        conf[knowhere::meta::RANGE_FILTER] = 0.2;
    } else {
        conf[knowhere::meta::RADIUS] = 0.2;
        conf[knowhere::meta::RANGE_FILTER] = 0.1;
    }

    if (milvus::is_in_list<milvus::IndexType>(index_type,
                                              search_with_nprobe_list)) {
        conf[knowhere::indexparam::NPROBE] = 4;
    } else if (index_type == knowhere::IndexEnum::INDEX_HNSW) {
        conf[knowhere::indexparam::EF] = 200;
    } else if (index_type == knowhere::IndexEnum::INDEX_DISKANN) {
        conf[milvus::index::DISK_ANN_QUERY_LIST] = K * 2;
    }
    return conf;
}

auto
generate_params(const knowhere::IndexType& index_type,
                const knowhere::MetricType& metric_type) {
    namespace indexcgo = milvus::proto::indexcgo;

    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;

    auto configs = generate_build_conf(index_type, metric_type);
    for (auto& [key, value] : configs.items()) {
        auto param = index_params.add_params();
        auto value_str =
            value.is_string() ? value.get<std::string>() : value.dump();
        param->set_key(key);
        param->set_value(value_str);
    }

    auto param = index_params.add_params();
    param->set_key("index_type");
    param->set_value(std::string(index_type));

    return std::make_tuple(type_params, index_params);
}

auto
GenFieldData(int64_t N,
             const knowhere::MetricType& metric_type,
             milvus::DataType data_type = milvus::DataType::VECTOR_FLOAT,
             int64_t dim = DIM) {
    auto schema = std::make_shared<milvus::Schema>();
    schema->AddDebugField(
        "fakevec",
        data_type,
        (data_type != milvus::DataType::VECTOR_SPARSE_FLOAT ? dim : 0),
        metric_type);
    return milvus::segcore::DataGen(schema, N);
}

using QueryResultPtr = std::unique_ptr<milvus::SearchResult>;
void
PrintQueryResult(const QueryResultPtr& result) {
    auto nq = result->total_nq_;
    auto k = result->unity_topK_;

    std::stringstream ss_id;
    std::stringstream ss_dist;

    for (auto i = 0; i < nq; i++) {
        for (auto j = 0; j < k; ++j) {
            ss_id << result->seg_offsets_[i * k + j] << " ";
            ss_dist << result->distances_[i * k + j] << " ";
        }
        ss_id << std::endl;
        ss_dist << std::endl;
    }
    std::cout << "id\n" << ss_id.str() << std::endl;
    std::cout << "dist\n" << ss_dist.str() << std::endl;
}

float
L2(const float* point_a, const float* point_b, int dim) {
    float dis = 0;
    for (auto i = 0; i < dim; i++) {
        auto c_a = point_a[i];
        auto c_b = point_b[i];
        dis += pow(c_b - c_a, 2);
    }
    return dis;
}

int
hamming_weight(uint8_t n) {
    int count = 0;
    while (n != 0) {
        count += n & 1;
        n >>= 1;
    }
    return count;
}
float
Jaccard(const uint8_t* point_a, const uint8_t* point_b, int dim) {
    float dis;
    int len = dim / 8;
    float intersection = 0;
    float union_num = 0;
    for (int i = 0; i < len; i++) {
        intersection += hamming_weight(point_a[i] & point_b[i]);
        union_num += hamming_weight(point_a[i] | point_b[i]);
    }
    dis = 1 - (intersection / union_num);
    return dis;
}

float
CountDistance(const void* point_a,
              const void* point_b,
              int dim,
              const knowhere::MetricType& metric,
              bool is_binary = false) {
    if (point_a == nullptr || point_b == nullptr) {
        return std::numeric_limits<float>::max();
    }
    if (milvus::IsMetricType(metric, knowhere::metric::L2)) {
        return L2(static_cast<const float*>(point_a),
                  static_cast<const float*>(point_b),
                  dim);
    } else if (milvus::IsMetricType(metric, knowhere::metric::JACCARD)) {
        return Jaccard(static_cast<const uint8_t*>(point_a),
                       static_cast<const uint8_t*>(point_b),
                       dim);
    } else {
        return std::numeric_limits<float>::max();
    }
}

void
CheckDistances(const QueryResultPtr& result,
               const knowhere::DataSetPtr& base_dataset,
               const knowhere::DataSetPtr& query_dataset,
               const knowhere::MetricType& metric,
               const float threshold = 1.0e-5) {
    auto base_vecs = (float*)(base_dataset->GetTensor());
    auto query_vecs = (float*)(query_dataset->GetTensor());
    auto dim = base_dataset->GetDim();
    auto nq = result->total_nq_;
    auto k = result->unity_topK_;
    for (auto i = 0; i < nq; i++) {
        for (auto j = 0; j < k; ++j) {
            auto dis = result->distances_[i * k + j];
            auto id = result->seg_offsets_[i * k + j];
            auto count_dis = CountDistance(
                query_vecs + i * dim, base_vecs + id * dim, dim, metric);
            // assert(std::abs(dis - count_dis) < threshold);
        }
    }
}

auto
generate_type_params(const MapParams& m) {
    indexcgo::TypeParams p;
    for (const auto& [k, v] : m) {
        auto kv = p.add_params();
        kv->set_key(k);
        kv->set_value(v);
    }
    std::string str;
    auto ok = google::protobuf::TextFormat::PrintToString(p, &str);
    Assert(ok);
    return str;
}

auto
generate_index_params(const MapParams& m) {
    indexcgo::IndexParams p;
    for (const auto& [k, v] : m) {
        auto kv = p.add_params();
        kv->set_key(k);
        kv->set_value(v);
    }
    std::string str;
    auto ok = google::protobuf::TextFormat::PrintToString(p, &str);
    Assert(ok);
    return str;
}

// TODO: std::is_arithmetic_v, hard to compare float point value. std::is_integral_v.
template <typename T,
          typename = typename std::enable_if_t<std::is_arithmetic_v<T> ||
                                               std::is_same_v<T, std::string>>>
inline std::vector<T>
GenSortedArr(int64_t n) {
    auto max_i8 = std::numeric_limits<int8_t>::max() - 1;
    std::vector<T> arr;
    arr.resize(n);
    for (int64_t i = 0; i < n; i++) {
        arr[i] = static_cast<T>(rand() % max_i8);
    }
    std::sort(arr.begin(), arr.end());
    return arr;
}

inline auto
GenStrArr(int64_t n) {
    using T = std::string;
    std::vector<T> arr;
    arr.resize(n);
    for (int64_t i = 0; i < n; i++) {
        auto gen = std::to_string(std::rand());
        arr[i] = gen;
    }
    std::sort(arr.begin(), arr.end());
    return arr;
}

template <>
inline std::vector<std::string>
GenSortedArr<std::string>(int64_t n) {
    return GenStrArr(n);
}

std::vector<ScalarTestParams>
GenBoolParams() {
    std::vector<ScalarTestParams> ret;
    ret.emplace_back(ScalarTestParams(MapParams(), {{"index_type", "sort"}}));
    ret.emplace_back(ScalarTestParams(MapParams(), {{"index_type", "flat"}}));
    return ret;
}

std::vector<ScalarTestParams>
GenStringParams() {
    std::vector<ScalarTestParams> ret;
    ret.emplace_back(ScalarTestParams(MapParams(), {{"index_type", "marisa"}}));
    return ret;
}

template <typename T,
          typename = typename std::enable_if_t<std::is_arithmetic_v<T> |
                                               std::is_same_v<std::string, T>>>
inline std::vector<ScalarTestParams>
GenParams() {
    if (std::is_same_v<std::string, T>) {
        return GenStringParams();
    }

    if (std::is_same_v<T, bool>) {
        return GenBoolParams();
    }

    std::vector<ScalarTestParams> ret;
    ret.emplace_back(ScalarTestParams(MapParams(), {{"index_type", "sort"}}));
    ret.emplace_back(ScalarTestParams(MapParams(), {{"index_type", "flat"}}));
    return ret;
}

void
PrintMapParam(const ScalarTestParams& tp) {
    for (const auto& [k, v] : tp.first) {
        std::cout << "k: " << k << ", v: " << v << std::endl;
    }
    for (const auto& [k, v] : tp.second) {
        std::cout << "k: " << k << ", v: " << v << std::endl;
    }
}

void
PrintMapParams(const std::vector<ScalarTestParams>& tps) {
    for (const auto& tp : tps) {
        PrintMapParam(tp);
    }
}

// memory generated by this function should be freed by the caller.
auto
GenDsFromPB(const google::protobuf::Message& msg) {
    auto data = new char[msg.ByteSizeLong()];
    msg.SerializeToArray(data, msg.ByteSizeLong());
    return knowhere::GenDataSet(msg.ByteSizeLong(), 8, data);
}

template <typename T>
inline std::vector<std::string>
GetIndexTypes() {
    return std::vector<std::string>{"sort", milvus::index::BITMAP_INDEX_TYPE};
}

template <>
inline std::vector<std::string>
GetIndexTypes<std::string>() {
    return std::vector<std::string>{
        "sort", "marisa", milvus::index::BITMAP_INDEX_TYPE};
}

template <typename T>
inline std::vector<std::string>
GetIndexTypesV2() {
    return std::vector<std::string>{"sort", milvus::index::INVERTED_INDEX_TYPE};
}

template <>
inline std::vector<std::string>
GetIndexTypesV2<std::string>() {
    return std::vector<std::string>{"marisa",
                                    milvus::index::INVERTED_INDEX_TYPE};
}

}  // namespace
