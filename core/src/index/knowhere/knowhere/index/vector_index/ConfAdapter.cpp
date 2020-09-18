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
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

#ifdef MILVUS_GPU_VERSION
#include "faiss/gpu/utils/DeviceUtils.h"
#endif

namespace milvus {
namespace knowhere {

static const int64_t MIN_NLIST = 1;
static const int64_t MAX_NLIST = 65536;
static const int64_t MIN_NPROBE = 1;
static const int64_t MAX_NPROBE = MAX_NLIST;
static const int64_t DEFAULT_MIN_DIM = 1;
static const int64_t DEFAULT_MAX_DIM = 32768;
static const int64_t DEFAULT_MIN_ROWS = 1;  // minimum size for build index
static const int64_t DEFAULT_MAX_ROWS = 50000000;
static const int64_t NGT_MIN_EDGE_SIZE = 1;
static const int64_t NGT_MAX_EDGE_SIZE = 200;
static const std::vector<std::string> METRICS{knowhere::Metric::L2, knowhere::Metric::IP};

#define CheckIntByRange(key, min, max)                                                                   \
    if (!oricfg.contains(key) || !oricfg[key].is_number_integer() || oricfg[key].get<int64_t>() > max || \
        oricfg[key].get<int64_t>() < min) {                                                              \
        return false;                                                                                    \
    }

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
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    return true;
}

bool
ConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    const int64_t DEFAULT_MIN_K = 1;
    const int64_t DEFAULT_MAX_K = 16384;
    CheckIntByRange(knowhere::meta::TOPK, DEFAULT_MIN_K - 1, DEFAULT_MAX_K);
    return true;
}

int64_t
MatchNlist(int64_t size, int64_t nlist) {
    const int64_t TYPICAL_COUNT = 1000000;
    const int64_t PER_NLIST = 16384;

    if (nlist * TYPICAL_COUNT > size * PER_NLIST) {
        // nlist is too large, adjust to a proper value
        nlist = std::max(1L, size * PER_NLIST / TYPICAL_COUNT);
    }
    return nlist;
}

bool
IVFConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    CheckIntByRange(knowhere::IndexParams::nlist, MIN_NLIST, MAX_NLIST);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);

    // int64_t nlist = oricfg[knowhere::IndexParams::nlist];
    // CheckIntByRange(knowhere::meta::ROWS, nlist, DEFAULT_MAX_ROWS);

    // auto tune params
    auto nq = oricfg[knowhere::meta::ROWS].get<int64_t>();
    auto nlist = oricfg[knowhere::IndexParams::nlist].get<int64_t>();
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(nq, nlist);

    // Best Practice
    // static int64_t MIN_POINTS_PER_CENTROID = 40;
    // static int64_t MAX_POINTS_PER_CENTROID = 256;
    // CheckIntByRange(knowhere::meta::ROWS, MIN_POINTS_PER_CENTROID * nlist, MAX_POINTS_PER_CENTROID * nlist);

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
IVFConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    int64_t max_nprobe = MAX_NPROBE;
#ifdef MILVUS_GPU_VERSION
    if (mode == IndexMode::MODE_GPU) {
        max_nprobe = faiss::gpu::getMaxKSelection();
    }
#endif
    CheckIntByRange(knowhere::IndexParams::nprobe, MIN_NPROBE, max_nprobe);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
IVFSQConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    const int64_t DEFAULT_NBITS = 8;
    oricfg[knowhere::IndexParams::nbits] = DEFAULT_NBITS;

    return IVFConfAdapter::CheckTrain(oricfg, mode);
}

bool
IVFPQConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    const int64_t DEFAULT_NBITS = 8;

    oricfg[knowhere::IndexParams::nbits] = DEFAULT_NBITS;

    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::nlist, MIN_NLIST, MAX_NLIST);

    // int64_t nlist = oricfg[knowhere::IndexParams::nlist];
    // CheckIntByRange(knowhere::meta::ROWS, nlist, DEFAULT_MAX_ROWS);

    // auto tune params
    oricfg[knowhere::IndexParams::nlist] =
        MatchNlist(oricfg[knowhere::meta::ROWS].get<int64_t>(), oricfg[knowhere::IndexParams::nlist].get<int64_t>());
    auto m = oricfg[knowhere::IndexParams::m].get<int64_t>();
    auto dimension = oricfg[knowhere::meta::DIM].get<int64_t>();
    // Best Practice
    // static int64_t MIN_POINTS_PER_CENTROID = 40;
    // static int64_t MAX_POINTS_PER_CENTROID = 256;
    // CheckIntByRange(knowhere::meta::ROWS, MIN_POINTS_PER_CENTROID * nlist, MAX_POINTS_PER_CENTROID * nlist);

    /*std::vector<int64_t> resset;
    IVFPQConfAdapter::GetValidCPUM(dimension, resset);*/
    IndexMode ivfpq_mode = mode;
    return GetValidM(dimension, m, ivfpq_mode);
}

bool
IVFPQConfAdapter::GetValidM(int64_t dimension, int64_t m, IndexMode& mode) {
#ifdef MILVUS_GPU_VERSION
    if (mode == knowhere::IndexMode::MODE_GPU && !IVFPQConfAdapter::GetValidGPUM(dimension, m)) {
        mode = knowhere::IndexMode::MODE_CPU;
    }
#endif
    if (mode == knowhere::IndexMode::MODE_CPU && !IVFPQConfAdapter::GetValidCPUM(dimension, m)) {
        return false;
    }
    return true;
}

bool
IVFPQConfAdapter::GetValidGPUM(int64_t dimension, int64_t m) {
    /*
     * Faiss 1.6
     * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
     * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
     */
    static const std::vector<int64_t> support_dim_per_subquantizer{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1};
    static const std::vector<int64_t> support_subquantizer{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1};

    int64_t sub_dim = dimension / m;
    return (std::find(std::begin(support_subquantizer), std::end(support_subquantizer), m) !=
            support_subquantizer.end()) &&
           (std::find(std::begin(support_dim_per_subquantizer), std::end(support_dim_per_subquantizer), sub_dim) !=
            support_dim_per_subquantizer.end());

    /*resset.clear();
      for (const auto& dimperquantizer : support_dim_per_subquantizer) {
        if (!(dimension % dimperquantizer)) {
            auto subquantzier_num = dimension / dimperquantizer;
            auto finder = std::find(support_subquantizer.begin(), support_subquantizer.end(), subquantzier_num);
            if (finder != support_subquantizer.end()) {
                resset.push_back(subquantzier_num);
            }
        }
    }*/
}

bool
IVFPQConfAdapter::GetValidCPUM(int64_t dimension, int64_t m) {
    return (dimension % m == 0);
}

bool
NSGConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    const int64_t MIN_KNNG = 5;
    const int64_t MAX_KNNG = 300;
    const int64_t MIN_SEARCH_LENGTH = 10;
    const int64_t MAX_SEARCH_LENGTH = 300;
    const int64_t MIN_OUT_DEGREE = 5;
    const int64_t MAX_OUT_DEGREE = 300;
    const int64_t MIN_CANDIDATE_POOL_SIZE = 50;
    const int64_t MAX_CANDIDATE_POOL_SIZE = 1000;

    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::knng, MIN_KNNG, MAX_KNNG);
    CheckIntByRange(knowhere::IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);
    CheckIntByRange(knowhere::IndexParams::out_degree, MIN_OUT_DEGREE, MAX_OUT_DEGREE);
    CheckIntByRange(knowhere::IndexParams::candidate, MIN_CANDIDATE_POOL_SIZE, MAX_CANDIDATE_POOL_SIZE);

    // auto tune params
    oricfg[knowhere::IndexParams::nlist] = MatchNlist(oricfg[knowhere::meta::ROWS].get<int64_t>(), 8192);

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
    static int64_t MIN_EFCONSTRUCTION = 8;
    static int64_t MAX_EFCONSTRUCTION = 512;
    static int64_t MIN_M = 4;
    static int64_t MAX_M = 64;

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
RHNSWFlatConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MIN_EFCONSTRUCTION = 8;
    static int64_t MAX_EFCONSTRUCTION = 512;
    static int64_t MIN_M = 4;
    static int64_t MAX_M = 64;

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::efConstruction, MIN_EFCONSTRUCTION, MAX_EFCONSTRUCTION);
    CheckIntByRange(knowhere::IndexParams::M, MIN_M, MAX_M);

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
RHNSWFlatConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    static int64_t MAX_EF = 4096;

    CheckIntByRange(knowhere::IndexParams::ef, oricfg[knowhere::meta::TOPK], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
RHNSWPQConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MIN_EFCONSTRUCTION = 8;
    static int64_t MAX_EFCONSTRUCTION = 512;
    static int64_t MIN_M = 4;
    static int64_t MAX_M = 64;

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::efConstruction, MIN_EFCONSTRUCTION, MAX_EFCONSTRUCTION);
    CheckIntByRange(knowhere::IndexParams::M, MIN_M, MAX_M);

    auto dimension = oricfg[knowhere::meta::DIM].get<int64_t>();

    IVFPQConfAdapter::GetValidCPUM(dimension, oricfg[knowhere::IndexParams::PQM].get<int64_t>());

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
RHNSWPQConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    static int64_t MAX_EF = 4096;

    CheckIntByRange(knowhere::IndexParams::ef, oricfg[knowhere::meta::TOPK], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
RHNSWSQConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static int64_t MIN_EFCONSTRUCTION = 8;
    static int64_t MAX_EFCONSTRUCTION = 512;
    static int64_t MIN_M = 4;
    static int64_t MAX_M = 64;

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::IndexParams::efConstruction, MIN_EFCONSTRUCTION, MAX_EFCONSTRUCTION);
    CheckIntByRange(knowhere::IndexParams::M, MIN_M, MAX_M);

    return ConfAdapter::CheckTrain(oricfg, mode);
}

bool
RHNSWSQConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    static int64_t MAX_EF = 4096;

    CheckIntByRange(knowhere::IndexParams::ef, oricfg[knowhere::meta::TOPK], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
BinIDMAPConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static const std::vector<std::string> METRICS{knowhere::Metric::HAMMING, knowhere::Metric::JACCARD,
                                                  knowhere::Metric::TANIMOTO, knowhere::Metric::SUBSTRUCTURE,
                                                  knowhere::Metric::SUPERSTRUCTURE};

    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);

    return true;
}

bool
BinIVFConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static const std::vector<std::string> METRICS{knowhere::Metric::HAMMING, knowhere::Metric::JACCARD,
                                                  knowhere::Metric::TANIMOTO};

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
    CheckIntByRange(knowhere::IndexParams::search_k, std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<int64_t>::max());
    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
NGTPANNGConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static std::vector<std::string> METRICS{knowhere::Metric::L2, knowhere::Metric::HAMMING, knowhere::Metric::JACCARD};

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    CheckIntByRange(knowhere::IndexParams::edge_size, NGT_MIN_EDGE_SIZE, NGT_MAX_EDGE_SIZE);

    return true;
}

bool
NGTPANNGConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

bool
NGTONNGConfAdapter::CheckTrain(Config& oricfg, const IndexMode mode) {
    static std::vector<std::string> METRICS{knowhere::Metric::L2, knowhere::Metric::HAMMING, knowhere::Metric::JACCARD};

    CheckIntByRange(knowhere::meta::ROWS, DEFAULT_MIN_ROWS, DEFAULT_MAX_ROWS);
    CheckIntByRange(knowhere::meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    CheckStrByValues(knowhere::Metric::TYPE, METRICS);
    CheckIntByRange(knowhere::IndexParams::edge_size, NGT_MIN_EDGE_SIZE, NGT_MAX_EDGE_SIZE);

    return true;
}

bool
NGTONNGConfAdapter::CheckSearch(Config& oricfg, const IndexType type, const IndexMode mode) {
    return ConfAdapter::CheckSearch(oricfg, type, mode);
}

}  // namespace knowhere
}  // namespace milvus
