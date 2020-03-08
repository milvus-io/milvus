// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
#include "wrapper/ConfAdapter.h"

#include <fiu-local.h>

#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "WrapperException.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "server/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

#if CUDA_VERSION > 9000
#define GPU_MAX_NRPOBE 2048
#else
#define GPU_MAX_NRPOBE 1024
#endif

#define DEFAULT_MAX_DIM 32768
#define DEFAULT_MIN_DIM 1
#define DEFAULT_MAX_K 16384
#define DEFAULT_MIN_K 1
#define DEFAULT_MIN_ROWS 1  // minimum size for build index
#define DEFAULT_MAX_ROWS 50000000

#define CheckIntByRange(key, min, max)                                                                   \
    if (!oricfg.contains(key) || !oricfg[key].is_number_integer() || oricfg[key].get<int64_t>() > max || \
        oricfg[key].get<int64_t>() < min) {                                                              \
        return false;                                                                                    \
    }

// #define checkfloat(key, min, max)                                                                              \
//     if (!oricfg.contains(key) || !oricfg[key].is_number_float() || oricfg[key] >= max || oricfg[key] <= min) { \
//         return false;                                                                                          \
//     }

#define CheckIntByValues(key, container)                                                                 \
    if (!oricfg.contains(key) || !oricfg[key].is_number_integer()) {                                     \
        return false;                                                                                    \
    } else {                                                                                             \
        auto finder = std::find(std::begin(container), std::end(container), oricfg[key].get<int64_t>()); \
        if (finder == std::end(container)) {                                                             \
            return false;                                                                                \
        }                                                                                                \
    }

#define CheckStrByValues(key, container)                                                                     \
    if (!oricfg.contains(key) || !oricfg[key].is_string()) {                                                 \
        return false;                                                                                        \
    } else {                                                                                                 \
        auto finder = std::find(std::begin(container), std::end(container), oricfg[key].get<std::string>()); \
        if (finder == std::end(container)) {                                                                 \
            return false;                                                                                    \
        }                                                                                                    \
    }

bool
ConfAdapter::CheckTrain(milvus::json& oricfg) {
    static std::vector<std::string> METRICS{knowhere::Metric::L2, knowhere::Metric::IP};

    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);

    return true;
}

bool
ConfAdapter::CheckSearch(milvus::json& oricfg, const IndexType& type) {
    CheckIntByRange(knowhere::meta::TOPK, DEFAULT_MIN_K - 1, DEFAULT_MAX_K);

    return true;
}

int64_t
MatchNlist(const int64_t& size, const int64_t& nlist, const int64_t& per_nlist) {
    static float TYPICAL_COUNT = 1000000.0;
    if (size <= TYPICAL_COUNT / per_nlist + 1) {
        // handle less row count, avoid nlist set to 0
        return 1;
    } else if (int(size / TYPICAL_COUNT) * nlist <= 0) {
        // calculate a proper nlist if nlist not specified or size less than TYPICAL_COUNT
        return int(size / TYPICAL_COUNT * per_nlist);
    }
    return nlist;
}

bool
IVFConfAdapter::CheckTrain(milvus::json& oricfg) {
    static int64_t MAX_NLIST = 999999;
    static int64_t MIN_NLIST = 1;

    CheckIntByRange(knowhere::IndexParams::nlist, MIN_NLIST, MAX_NLIST);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);

    // int64_t nlist = oricfg[knowhere::IndexParams::nlist];
    // CheckIntByRange(knowhere::meta::ROWS, nlist, DEFAULT_MAX_ROWS);

    // auto tune params
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(oricfg[knowhere::meta::ROWS].get<int64_t>(),
                                                      oricfg[knowhere::IndexParams::nlist].get<int64_t>(), 16384);

    // Best Practice
    // static int64_t MIN_POINTS_PER_CENTROID = 40;
    // static int64_t MAX_POINTS_PER_CENTROID = 256;
    // CheckIntByRange(knowhere::meta::ROWS, MIN_POINTS_PER_CENTROID * nlist, MAX_POINTS_PER_CENTROID * nlist);

    return ConfAdapter::CheckTrain(oricfg);
}

bool
IVFConfAdapter::CheckSearch(milvus::json& oricfg, const IndexType& type) {
    static int64_t MIN_NPROBE = 1;
    static int64_t MAX_NPROBE = 999999;  // todo(linxj): [1, nlist]

    if (type == IndexType::FAISS_IVFPQ_GPU || type == IndexType::FAISS_IVFSQ8_GPU ||
        type == IndexType::FAISS_IVFSQ8_HYBRID || type == IndexType::FAISS_IVFFLAT_GPU) {
        CheckIntByRange(knowhere::IndexParams::nprobe, MIN_NPROBE, GPU_MAX_NRPOBE);
    } else {
        CheckIntByRange(knowhere::IndexParams::nprobe, MIN_NPROBE, MAX_NPROBE);
    }

    return ConfAdapter::CheckSearch(oricfg, type);
}

bool
IVFSQConfAdapter::CheckTrain(milvus::json& oricfg) {
    static int64_t DEFAULT_NBITS = 8;
    oricfg[knowhere::IndexParams::nbits] = DEFAULT_NBITS;

    return IVFConfAdapter::CheckTrain(oricfg);
}

bool
IVFPQConfAdapter::CheckTrain(milvus::json& oricfg) {
    static int64_t DEFAULT_NBITS = 8;
    static int64_t MAX_NLIST = 999999;
    static int64_t MIN_NLIST = 1;
    static std::vector<std::string> CPU_METRICS{knowhere::Metric::L2, knowhere::Metric::IP};
    static std::vector<std::string> GPU_METRICS{knowhere::Metric::L2};

    oricfg[knowhere::IndexParams::nbits] = DEFAULT_NBITS;

#ifdef MILVUS_GPU_VERSION
    Status s;
    bool enable_gpu = false;
    server::Config& config = server::Config::GetInstance();
    s = config.GetGpuResourceConfigEnable(enable_gpu);
    if (s.ok()) {
        CheckStrByValues(knowhere::Metric::TYPE, GPU_METRICS);
    } else {
        CheckStrByValues(knowhere::Metric::TYPE, CPU_METRICS);
    }
#endif
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::nlist, MIN_NLIST, MAX_NLIST);

    // int64_t nlist = oricfg[knowhere::IndexParams::nlist];
    // CheckIntByRange(knowhere::meta::ROWS, nlist, DEFAULT_MAX_ROWS);

    // auto tune params
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(oricfg[knowhere::meta::ROWS].get<int64_t>(),
                                                      oricfg[knowhere::IndexParams::nlist].get<int64_t>(), 16384);

    // Best Practice
    // static int64_t MIN_POINTS_PER_CENTROID = 40;
    // static int64_t MAX_POINTS_PER_CENTROID = 256;
    // CheckIntByRange(knowhere::meta::ROWS, MIN_POINTS_PER_CENTROID * nlist, MAX_POINTS_PER_CENTROID * nlist);

    /*
     * Faiss 1.6
     * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
     * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
     */
    static std::vector<int64_t> support_dim_per_subquantizer{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1};
    static std::vector<int64_t> support_subquantizer{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1};
    std::vector<int64_t> resset;
    for (const auto& dimperquantizer : support_dim_per_subquantizer) {
        if (!(oricfg[knowhere::meta::DIM].get<int64_t>() % dimperquantizer)) {
            auto subquantzier_num = oricfg[knowhere::meta::DIM].get<int64_t>() / dimperquantizer;
            auto finder = std::find(support_subquantizer.begin(), support_subquantizer.end(), subquantzier_num);
            if (finder != support_subquantizer.end()) {
                resset.push_back(subquantzier_num);
            }
        }
    }
    CheckIntByValues(knowhere::IndexParams::m, resset);

    return true;
}

bool
NSGConfAdapter::CheckTrain(milvus::json& oricfg) {
    static int64_t MIN_KNNG = 5;
    static int64_t MAX_KNNG = 300;
    static int64_t MIN_SEARCH_LENGTH = 10;
    static int64_t MAX_SEARCH_LENGTH = 300;
    static int64_t MIN_OUT_DEGREE = 5;
    static int64_t MAX_OUT_DEGREE = 300;
    static int64_t MIN_CANDIDATE_POOL_SIZE = 50;
    static int64_t MAX_CANDIDATE_POOL_SIZE = 1000;
    static std::vector<std::string> METRICS{knowhere::Metric::L2};

    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::knng, MIN_KNNG, MAX_KNNG);
    CheckIntByRange(knowhere::IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);
    CheckIntByRange(knowhere::IndexParams::out_degree, MIN_OUT_DEGREE, MAX_OUT_DEGREE);
    CheckIntByRange(knowhere::IndexParams::candidate, MIN_CANDIDATE_POOL_SIZE, MAX_CANDIDATE_POOL_SIZE);

    // auto tune params
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(oricfg[knowhere::meta::ROWS].get<int64_t>(), 8192, 8192);
    oricfg[knowhere::IndexParams::nprobe] = int(oricfg[knowhere::IndexParams::nlist].get<int64_t>() * 0.01);

    return true;
}

bool
NSGConfAdapter::CheckSearch(milvus::json& oricfg, const IndexType& type) {
    static int64_t MIN_SEARCH_LENGTH = 1;
    static int64_t MAX_SEARCH_LENGTH = 300;

    CheckIntByRange(knowhere::IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);

    return ConfAdapter::CheckSearch(oricfg, type);
}

bool
HNSWConfAdapter::CheckTrain(milvus::json& oricfg) {
    static int64_t MIN_EFCONSTRUCTION = 100;
    static int64_t MAX_EFCONSTRUCTION = 500;
    static int64_t MIN_M = 5;
    static int64_t MAX_M = 48;

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::efConstruction, MIN_EFCONSTRUCTION, MAX_EFCONSTRUCTION);
    CheckIntByRange(knowhere::IndexParams::M, MIN_M, MAX_M);

    return ConfAdapter::CheckTrain(oricfg);
}

bool
HNSWConfAdapter::CheckSearch(milvus::json& oricfg, const IndexType& type) {
    static int64_t MAX_EF = 4096;

    CheckIntByRange(knowhere::IndexParams::ef, oricfg[knowhere::meta::TOPK], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg, type);
}

bool
BinIDMAPConfAdapter::CheckTrain(milvus::json& oricfg) {
    static std::vector<std::string> METRICS{knowhere::Metric::HAMMING, knowhere::Metric::JACCARD,
                                            knowhere::Metric::TANIMOTO};

    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);

    return true;
}

bool
BinIVFConfAdapter::CheckTrain(milvus::json& oricfg) {
    static std::vector<std::string> METRICS{knowhere::Metric::HAMMING, knowhere::Metric::JACCARD,
                                            knowhere::Metric::TANIMOTO};
    static int64_t MAX_NLIST = 999999;
    static int64_t MIN_NLIST = 1;

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckIntByRange(knowhere::IndexParams::nlist, MIN_NLIST, MAX_NLIST);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);

    int64_t nlist = oricfg[knowhere::IndexParams::nlist];
    CheckIntByRange(knowhere::meta::ROWS, nlist, DEFAULT_MAX_ROWS);

    // Best Practice
    // static int64_t MIN_POINTS_PER_CENTROID = 40;
    // static int64_t MAX_POINTS_PER_CENTROID = 256;
    // CheckIntByRange(knowhere::meta::ROWS, MIN_POINTS_PER_CENTROID * nlist, MAX_POINTS_PER_CENTROID * nlist);

    return true;
}
}  // namespace engine
}  // namespace milvus
