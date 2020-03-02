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
#include "knowhere/Index/vector_index/helpers/IndexParameter.h"

#include <fiu-local.h>

#include <cmath>
#include <memory>
#include <vector>

#include "WrapperException.h"
#include "server/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

using namespace knowhere;

#if CUDA_VERSION > 9000
#define GPU_MAX_NRPOBE 2048
#else
#define GPU_MAX_NRPOBE 1024
#endif

#define DEFAULT_MAX_DIM = 16384;
#define DEFAULT_MIN_DIM = 1;
#define DEFAULT_MAX_K = 16384;
#define DEFAULT_MIN_K = 1;

#define checkint(key, min, max)                                                                                  \
    if (!oricfg.contains(key) || !oricfg[key].is_number_integer() || oricfg[key] >= max || oricfg[key] <= min) { \
        return false;                                                                                            \
    }

#define checkfloat(key, min, max)                                                                              \
    if (!oricfg.contains(key) || !oricfg[key].is_number_float() || oricfg[key] >= max || oricfg[key] <= min) { \
        return false;                                                                                          \
    }

#define checkintbyvalue(key, container)                                                                  \
    if (!oricfg.contains(key) || !oricfg[key].is_number_integer()) {                                     \
        return false;                                                                                    \
    } else {                                                                                             \
        auto finder = std::find(std::begin(container), std::end(container), oricfg[key].get<int64_t>()); \
        if (finder == std::end(container)) {                                                             \
            return false;                                                                                \
        }                                                                                                \
    }

#define checkstr(key, container)                                                                             \
    if (!oricfg.contains(key) || !oricfg[key].is_string()) {                                                 \
        return false;                                                                                        \
    } else {                                                                                                 \
        auto finder = std::find(std::begin(container), std::end(container), oricfg[key].get<std::string>()); \
        if (finder == std::end(container)) {                                                                 \
            return false;                                                                                    \
        }                                                                                                    \
    }

bool
ConfAdapter::CheckTrain(Json& oricfg) {
    static std::vector<std::string> METRICS{Metric::L2, Metric::IP};

    checkint(meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    checkstr(Metric::TYPE, METRICS);

    return true;
}

bool
ConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    checkint(meta::TOPK, DEFAULT_MIN_K, DEFAULT_MAX_K);

    return true;
}

bool
IVFConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t MAX_NLIST = 99999;  // todo(jinhai): default value
    static int64_t MIN_NLIST = 1;

    checkint(IndexParams::nlist, MIN_NLIST, MAX_NLIST);

    return ConfAdapter::CheckTrain(oricfg);
}

bool
IVFConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    static int64_t MIN_NPROBE = 1;
    static int64_t MAX_NPROBE = 99999;  // todo(linxj): [1, nlist]

    if (type == IndexType::FAISS_IVFPQ_GPU || IndexType::FAISS_IVFSQ8_GPU || IndexType::FAISS_IVFSQ8_HYBRID ||
        IndexType::FAISS_IVFFLAT_GPU) {
        checkint(IndexParams::nprobe, MIN_NPROBE, GPU_MAX_NRPOBE);
    } else {
        checkint(IndexParams::nprobe, MIN_NPROBE, MAX_NPROBE);
    }

    return ConfAdapter::CheckSearch(oricfg);
}

bool
IVFSQConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t DEFAULT_NBITS = 8;
    oricfg[IndexParams::nbits] = DEFAULT_NBITS;

    return IVFConfAdapter::CheckTrain(oricfg);
}

bool
IVFPQConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t DEFAULT_NBITS = 8;
    static std::vector<std::string> CPU_METRICS{Metric::L2, Metric::IP};
    static std::vector<std::string> GPU_METRICS{Metric::L2};

    oricfg[IndexParams::nbits] = DEFAULT_NBITS;

#ifdef MILVUS_GPU_VERSION
    Status s;
    bool enable_gpu = false;
    server::Config& config = server::Config::GetInstance();
    s = config.GetGpuResourceConfigEnable(enable_gpu);
    if (s.ok()) {
        checkstr(Metric::TYPE, GPU_METRICS);
    } else {
        checkstr(Metric::TYPE, CPU_METRICS);
    }
#endif
    checkint(meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);

    /*
     * Faiss 1.6
     * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
     * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
     */
    static std::vector<int64_t> support_dim_per_subquantizer{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1};
    static std::vector<int64_t> support_subquantizer{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1};
    std::vector<int64_t> resset;
    for (const auto& dimperquantizer : support_dim_per_subquantizer) {
        if (!(oricfg[meta::DIM] % dimperquantizer)) {
            auto subquantzier_num = oricfg[meta::DIM] / dimperquantizer;
            auto finder = std::find(support_subquantizer.begin(), support_subquantizer.end(), subquantzier_num);
            if (finder != support_subquantizer.end()) {
                resset.push_back(subquantzier_num);
            }
        }
    }
    checkintbyvalue(IndexParams::m, resset);

    return true;
}

bool
NSGConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t MIN_KNNG = 5;
    static int64_t MAX_KNNG = 300;
    static int64_t MIN_SEARCH_LENGTH = 10;
    static int64_t MAX_SEARCH_LENGTH = 300;
    static int64_t MIN_OUT_DEGREE = 5;
    static int64_t MAX_OUT_DEGREE = 300;
    static int64_t MIN_CANDIDATE_POOL_SIZE = 50;
    static int64_t MAX_CANDIDATE_POOL_SIZE = 1000;
    static std::vector<std::string> METRICS{Metric::L2};

    checkstr(Metric::TYPE, METRICS);
    checkint(IndexParams::knng, MIN_KNNG, MAX_KNNG);
    checkint(IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);
    checkint(IndexParams::out_degree, MIN_OUT_DEGREE, MAX_OUT_DEGREE);
    checkint(IndexParams::candidate, MIN_CANDIDATE_POOL_SIZE, MAX_CANDIDATE_POOL_SIZE);

    return true;
}

void
NSGConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    static int64_t MIN_SEARCH_LENGTH = 1;
    static int64_t MAX_SEARCH_LENGTH = 300;

    checkint(IndexParams::search_length, MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);

    return ConfAdapter::CheckSearch(oricfg);
}

bool
HNSWConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t MIN_EFCONSTRUCTION = 100;
    static int64_t MAX_EFCONSTRUCTION = 500;
    static int64_t MIN_M = 5;
    static int64_t MAX_M = 48;

    checkint(IndexParams::efConstruction, MIN_EFCONSTRUCTION, MAX_EFCONSTRUCTION);
    checkint(IndexParams::M, MIN_M, MAX_M);

    return ConfAdapter::CheckTrain(oricfg);
}

bool
HNSWConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    static int64_t MAX_EF = 4096;

    checkint(IndexParams::ef, oricfg[meta::TOPK], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg);
}

bool
BinIDMAPConfAdapter::CheckTrain(Json& oricfg) {
    static std::vector<std::string> METRICS{Metric::HAMMING, Metric::JACCARD, Metric::TANIMOTO};

    checkint(meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    checkstr(Metric::TYPE, METRICS);

    return true;
}

bool
BinIVFConfAdapter::CheckTrain(Json& oricfg) {
    static std::vector<std::string> METRICS{Metric::HAMMING, Metric::JACCARD, Metric::TANIMOTO};
    static int64_t MAX_NLIST = 99999;  // todo(jinhai): default value
    static int64_t MIN_NLIST = 1;

    checkint(meta::DIM, DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    checkint(IndexParams::nlist, MIN_NLIST, MAX_NLIST);
    checkstr(Metric::TYPE, METRICS);

    return true;
}
}  // namespace engine
}  // namespace milvus
