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

#include "knowhere/index/vector_index/ConfAdapter.h"

#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

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
ConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static std::vector<std::string> METRICS{knowhere::Metric::L2, knowhere::Metric::IP};
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    return true;
}

bool
ConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
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
IVFConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MAX_NLIST = 999999;
    static int64_t MIN_NLIST = 1;

    CheckIntByRange(knowhere::IndexParams::nlist, MIN_NLIST, MAX_NLIST);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);

    // int64_t nlist = oricfg[knowhere::IndexParams::nlist];
    // CheckIntByRange(knowhere::meta::ROWS, nlist, DEFAULT_MAX_ROWS);

    // auto tune params
    int64_t nq = oricfg[knowhere::meta::ROWS].get<int64_t>();
    int64_t nlist = oricfg[knowhere::IndexParams::nlist].get<int64_t>();
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(nq, nlist, 16384);

    // Best Practice
    // static int64_t MIN_POINTS_PER_CENTROID = 40;
    // static int64_t MAX_POINTS_PER_CENTROID = 256;
    // CheckIntByRange(knowhere::meta::ROWS, MIN_POINTS_PER_CENTROID * nlist, MAX_POINTS_PER_CENTROID * nlist);

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
IVFConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    static int64_t MIN_NPROBE = 1;
    static int64_t MAX_NPROBE = 999999;  // todo(linxj): [1, nlist]

    if (mode == IndexMode::MODE_GPU) {
        CheckIntByRange(knowhere::IndexParams::nprobe, MIN_NPROBE, GPU_MAX_NRPOBE);
    } else {
        CheckIntByRange(knowhere::IndexParams::nprobe, MIN_NPROBE, MAX_NPROBE);
    }
    CheckIntByRange(knowhere::IndexParams::nprobe, MIN_NPROBE, MAX_NPROBE);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
IVFSQConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t DEFAULT_NBITS = 8;
    oricfg[knowhere::IndexParams::nbits] = DEFAULT_NBITS;

    return IVFConfAdapter::CheckTrain(oricfg, mode);
}

bool
IVFPQConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t DEFAULT_NBITS = 8;
    static int64_t MAX_NLIST = 999999;
    static int64_t MIN_NLIST = 1;
    static std::vector<std::string> CPU_METRICS{knowhere::Metric::L2, knowhere::Metric::IP};
    static std::vector<std::string> GPU_METRICS{knowhere::Metric::L2};

    oricfg[knowhere::IndexParams::nbits] = DEFAULT_NBITS;

    if (mode == IndexMode::MODE_GPU) {
        CheckStrByValues(knowhere::Metric::TYPE, GPU_METRICS);
    } else {
        CheckStrByValues(knowhere::Metric::TYPE, CPU_METRICS);
    }
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

    std::vector<int64_t> resset;
    int64_t dimension = oricfg[knowhere::meta::DIM].get<int64_t>();
    IVFPQConfAdapter::GetValidMList(dimension, resset);

    CheckIntByValues(knowhere::IndexParams::m, resset);

    return true;
}

void
IVFPQConfAdapter::GetValidMList(int64_t dimension, std::vector<int64_t>& resset) {
    resset.clear();
    /*
     * Faiss 1.6
     * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
     * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
     */
    static std::vector<int64_t> support_dim_per_subquantizer{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1};
    static std::vector<int64_t> support_subquantizer{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1};

    for (const auto& dimperquantizer : support_dim_per_subquantizer) {
        if (!(dimension % dimperquantizer)) {
            auto subquantzier_num = dimension / dimperquantizer;
            auto finder = std::find(support_subquantizer.begin(), support_subquantizer.end(), subquantzier_num);
            if (finder != support_subquantizer.end()) {
                resset.push_back(subquantzier_num);
            }
        }
    }
}

bool
NSGConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MIN_KNNG = 5;
    static int64_t MAX_KNNG = 300;
    static int64_t MIN_SEARCH_LENGTH = 10;
    static int64_t MAX_SEARCH_LENGTH = 300;
    static int64_t MIN_OUT_DEGREE = 5;
    static int64_t MAX_OUT_DEGREE = 300;
    static int64_t MIN_CANDIDATE_POOL_SIZE = 50;
    static int64_t MAX_CANDIDATE_POOL_SIZE = 1000;
    static std::vector<std::string> METRICS{knowhere::Metric::L2, knowhere::Metric::IP};

    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::knng, MIN_KNNG, MAX_KNNG);
    CheckIntByRange(knowhere::IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);
    CheckIntByRange(knowhere::IndexParams::out_degree, MIN_OUT_DEGREE, MAX_OUT_DEGREE);
    CheckIntByRange(knowhere::IndexParams::candidate, MIN_CANDIDATE_POOL_SIZE, MAX_CANDIDATE_POOL_SIZE);

    // auto tune params
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(oricfg[knowhere::meta::ROWS].get<int64_t>(), 8192, 8192);

    int64_t nprobe = int(oricfg[knowhere::IndexParams::nlist].get<int64_t>() * 0.1);
    oricfg[knowhere::IndexParams::nprobe] = nprobe < 1 ? 1 : nprobe;

    return true;
}

bool
NSGConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    static int64_t MIN_SEARCH_LENGTH = 1;
    static int64_t MAX_SEARCH_LENGTH = 300;

    CheckIntByRange(knowhere::IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
HNSWConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MIN_EFCONSTRUCTION = 100;
    static int64_t MAX_EFCONSTRUCTION = 800;
    static int64_t MIN_M = 5;
    static int64_t MAX_M = 48;

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::efConstruction, MIN_EFCONSTRUCTION, MAX_EFCONSTRUCTION);
    CheckIntByRange(knowhere::IndexParams::M, MIN_M, MAX_M);

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
HNSWConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    static int64_t MAX_EF = 4096;

    CheckIntByRange(knowhere::IndexParams::ef, oricfg[knowhere::meta::TOPK], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
BinIDMAPConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static std::vector<std::string> METRICS{knowhere::Metric::HAMMING, knowhere::Metric::JACCARD,
                                            knowhere::Metric::TANIMOTO, knowhere::Metric::SUBSTRUCTURE,
                                            knowhere::Metric::SUPERSTRUCTURE};

    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);

    return true;
}

bool
BinIVFConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
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

bool
ANNOYConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MIN_NTREES = 1;
    // too large of n_trees takes much time, if there is real requirement, change this threshold.
    static int64_t MAX_NTREES = 1024;

    CheckIntByRange(knowhere::IndexParams::n_trees, MIN_NTREES, MAX_NTREES);

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
ANNOYConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

}  // namespace knowhere
}  // namespace milvus
