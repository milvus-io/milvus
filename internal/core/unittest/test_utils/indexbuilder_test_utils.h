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
#include <knowhere/common/MetricType.h>

#include "pb/index_cgo_msg.pb.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h"
#include "index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "indexbuilder/IndexWrapper.h"
#include "indexbuilder/index_c.h"
#include "DataGen.h"
#include "index/knowhere/knowhere/index/vector_index/VecIndexFactory.h"
#include "indexbuilder/utils.h"

constexpr int64_t DIM = 128;
constexpr int64_t NQ = 10;
constexpr int64_t K = 4;
#ifdef MILVUS_GPU_VERSION
int DEVICEID = 0;
#endif

namespace {
auto
generate_conf(const milvus::knowhere::IndexType& index_type, const milvus::knowhere::MetricType& metric_type) {
    if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IDMAP) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 16},
            {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::m, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 16},
            {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
#ifdef MILVUS_GPU_VERSION
            {milvus::knowhere::meta::DEVICEID, DEVICEID},
#endif
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 16},
            {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
#ifdef MILVUS_GPU_VERSION
            {milvus::knowhere::meta::DEVICEID, DEVICEID},
#endif
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 16},
            {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::m, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::Metric::TYPE, metric_type},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_NSG) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::IndexParams::nlist, 163},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nprobe, 8},
            {milvus::knowhere::IndexParams::knng, 20},
            {milvus::knowhere::IndexParams::search_length, 40},
            {milvus::knowhere::IndexParams::out_degree, 30},
            {milvus::knowhere::IndexParams::candidate, 100},
            {milvus::knowhere::Metric::TYPE, metric_type},
        };
#ifdef MILVUS_SUPPORT_SPTAG
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
#endif
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_HNSW) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},       {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::M, 16},   {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200}, {milvus::knowhere::Metric::TYPE, metric_type},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_ANNOY) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::n_trees, 4},
            {milvus::knowhere::IndexParams::search_k, 100},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_RHNSWFlat) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_RHNSWPQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
            {milvus::knowhere::IndexParams::PQM, 8},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_RHNSWSQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_NGTPANNG) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::IndexParams::edge_size, 10},
            {milvus::knowhere::IndexParams::epsilon, 0.1},
            {milvus::knowhere::IndexParams::max_search_edges, 50},
            {milvus::knowhere::IndexParams::forcedly_pruned_edge_size, 60},
            {milvus::knowhere::IndexParams::selectively_pruned_edge_size, 30},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_NGTONNG) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::IndexParams::edge_size, 20},
            {milvus::knowhere::IndexParams::epsilon, 0.1},
            {milvus::knowhere::IndexParams::max_search_edges, 50},
            {milvus::knowhere::IndexParams::outgoing_edge_size, 5},
            {milvus::knowhere::IndexParams::incoming_edge_size, 40},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    }
    return milvus::knowhere::Config();
}

auto
generate_params(const milvus::knowhere::IndexType& index_type, const milvus::knowhere::MetricType& metric_type) {
    namespace indexcgo = milvus::proto::indexcgo;

    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;

    auto configs = generate_conf(index_type, metric_type);
    for (auto& [key, value] : configs.items()) {
        auto param = index_params.add_params();
        auto value_str = value.is_string() ? value.get<std::string>() : value.dump();
        param->set_key(key);
        param->set_value(value_str);
    }

    auto param = index_params.add_params();
    param->set_key("index_type");
    param->set_value(std::string(index_type));

    return std::make_tuple(type_params, index_params);
}

auto
GenDataset(int64_t N, const milvus::knowhere::MetricType& metric_type, bool is_binary, int64_t dim = DIM) {
    auto schema = std::make_shared<milvus::Schema>();
    auto faiss_metric_type = milvus::knowhere::GetMetricType(metric_type);
    if (!is_binary) {
        schema->AddDebugField("fakevec", milvus::engine::DataType::VECTOR_FLOAT, dim, faiss_metric_type);
        return milvus::segcore::DataGen(schema, N);
    } else {
        schema->AddDebugField("fakebinvec", milvus::engine::DataType::VECTOR_BINARY, dim, faiss_metric_type);
        return milvus::segcore::DataGen(schema, N);
    }
}

using QueryResultPtr = std::unique_ptr<milvus::indexbuilder::IndexWrapper::QueryResult>;
void
PrintQueryResult(const QueryResultPtr& result) {
    auto nq = result->nq;
    auto k = result->topk;

    std::stringstream ss_id;
    std::stringstream ss_dist;

    for (auto i = 0; i < nq; i++) {
        for (auto j = 0; j < k; ++j) {
            ss_id << result->ids[i * k + j] << " ";
            ss_dist << result->distances[i * k + j] << " ";
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
              const milvus::knowhere::MetricType& metric,
              bool is_binary = false) {
    if (point_a == nullptr || point_b == nullptr) {
        return std::numeric_limits<float>::max();
    }
    if (metric == milvus::knowhere::Metric::L2) {
        return L2(static_cast<const float*>(point_a), static_cast<const float*>(point_b), dim);
    } else if (metric == milvus::knowhere::Metric::JACCARD) {
        return Jaccard(static_cast<const uint8_t*>(point_a), static_cast<const uint8_t*>(point_b), dim);
    } else {
        return std::numeric_limits<float>::max();
    }
}

void
CheckDistances(const QueryResultPtr& result,
               const milvus::knowhere::DatasetPtr& base_dataset,
               const milvus::knowhere::DatasetPtr& query_dataset,
               const milvus::knowhere::MetricType& metric,
               const float threshold = 1.0e-5) {
    auto base_vecs = base_dataset->Get<float*>(milvus::knowhere::meta::TENSOR);
    auto query_vecs = query_dataset->Get<float*>(milvus::knowhere::meta::TENSOR);
    auto dim = base_dataset->Get<int64_t>(milvus::knowhere::meta::DIM);
    auto nq = result->nq;
    auto k = result->topk;
    for (auto i = 0; i < nq; i++) {
        for (auto j = 0; j < k; ++j) {
            auto dis = result->distances[i * k + j];
            auto id = result->ids[i * k + j];
            auto count_dis = CountDistance(query_vecs + i * dim, base_vecs + id * dim, dim, metric);
            // assert(std::abs(dis - count_dis) < threshold);
        }
    }
}
}  // namespace
