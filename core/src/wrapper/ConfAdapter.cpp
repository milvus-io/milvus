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
#include <vector>

#include "WrapperException.h"
#include "server/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

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
    static std::vector<std::string> METRICS{"L2", "IP"};

    checkint("dim", DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    checkstr("metric", METRICS);

    return true;
}

bool
ConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    checkint("k", DEFAULT_MIN_K, DEFAULT_MAX_K);

    return true;
}

bool
IVFConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t MAX_NLIST = 99999;  // todo(jinhai): default value
    static int64_t MIN_NLIST = 1;

    checkint("nlist", MIN_NLIST, MAX_NLIST);

    return ConfAdapter::CheckTrain(oricfg);
}

bool
IVFConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    static int64_t MIN_NPROBE = 1;
    static int64_t MAX_NPROBE = 99999;  // todo(linxj): [1, nlist]

    if (type == IndexType::FAISS_IVFPQ_GPU || IndexType::FAISS_IVFSQ8_GPU || IndexType::FAISS_IVFSQ8_HYBRID ||
        IndexType::FAISS_IVFFLAT_GPU) {
        checkint("nprobe", MIN_NPROBE, GPU_MAX_NRPOBE);
    } else {
        checkint("nprobe", MIN_NPROBE, MAX_NPROBE);
    }

    return ConfAdapter::CheckSearch(oricfg);
}

bool
IVFSQConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t DEFAULT_NBITS = 8;
    oricfg["nbits"] = DEFAULT_NBITS;

    return IVFConfAdapter::CheckTrain(oricfg);
}

bool
IVFPQConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t DEFAULT_NBITS = 8;
    static std::vector<std::string> CPU_METRICS{"L2", "IP"};
    static std::vector<std::string> GPU_METRICS{"L2"};

    oricfg["nbits"] = DEFAULT_NBITS;

#ifdef MILVUS_GPU_VERSION
    Status s;
    bool enable_gpu = false;
    server::Config& config = server::Config::GetInstance();
    s = config.GetGpuResourceConfigEnable(enable_gpu);
    if (s.ok()) {
        checkstr("metric", GPU_METRICS);
    } else {
        checkstr("metric", CPU_METRICS);
    }
#endif
    checkint("dim", DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);

    /*
     * Faiss 1.6
     * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
     * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
     */
    static std::vector<int64_t> support_dim_per_subquantizer{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1};
    static std::vector<int64_t> support_subquantizer{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1};
    std::vector<int64_t> resset;
    for (const auto& dimperquantizer : support_dim_per_subquantizer) {
        if (!(oricfg["dim"] % dimperquantizer)) {
            auto subquantzier_num = oricfg["dim"] / dimperquantizer;
            auto finder = std::find(support_subquantizer.begin(), support_subquantizer.end(), subquantzier_num);
            if (finder != support_subquantizer.end()) {
                resset.push_back(subquantzier_num);
            }
        }
    }
    checkintbyvalue("m", resset);

    return true;
}

bool
NSGConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t MIN_KNNG = 5;
    static int64_t MAX_KNNG = 300;
    static int64_t MIN_SEARCH_LENGTH = 10;
    static int64_t MAX_SEARCH_LENGTH = 200;
    static int64_t MIN_OUT_DEGREE = 5;
    static int64_t MAX_OUT_DEGREE = 300;
    static int64_t MIN_CANDIDATE_POOL_SIZE = 50;
    static int64_t MAX_CANDIDATE_POOL_SIZE = 1000;

    checkint("knng", MIN_KNNG, MAX_KNNG);
    checkint("search_length", MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);
    checkint("out_degree", MIN_OUT_DEGREE, MAX_OUT_DEGREE);
    checkint("candidate_pool_size", MIN_CANDIDATE_POOL_SIZE, MAX_CANDIDATE_POOL_SIZE);

    return ConfAdapter::CheckTrain(oricfg);
}

void
NSGConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    static int64_t MIN_SEARCH_LENGTH = 1;
    static int64_t MAX_SEARCH_LENGTH = 200;

    checkint("search_length", MIN_SEARCH_LENGTH, MAX_SEARCH_LENGTH);

    return ConfAdapter::CheckSearch(oricfg);
}

bool
HNSWConfAdapter::CheckTrain(Json& oricfg) {
    static int64_t MIN_EF_CONSTRUCT = 100;
    static int64_t MAX_EF_CONSTRUCT = 500;
    static int64_t MIN_M = 5;
    static int64_t MAX_M = 48;

    checkint("ef_construct", MIN_EF_CONSTRUCT, MAX_EF_CONSTRUCT);
    checkint("M", MIN_M, MAX_M);

    return ConfAdapter::CheckTrain(oricfg);
}

bool
HNSWConfAdapter::CheckSearch(Json& oricfg, const IndexType& type) {
    static int64_t MAX_EF = 4096;

    checkint("ef", oricfg["k"], MAX_EF);

    return ConfAdapter::CheckSearch(oricfg);
}

bool
BinIDMAPConfAdapter::CheckTrain(Json& oricfg) {
    static std::vector<std::string> METRICS{"HAMMING", "JACCARD", "TANIMOTO"};

    checkint("dim", DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    checkstr("metric", METRICS);

    return true;
}

bool
BinIVFConfAdapter::CheckTrain(Json& oricfg) {
    static std::vector<std::string> METRICS{"HAMMING", "JACCARD", "TANIMOTO"};
    static int64_t MAX_NLIST = 99999;  // todo(jinhai): default value
    static int64_t MIN_NLIST = 1;

    checkint("dim", DEFAULT_MIN_DIM, DEFAULT_MAX_DIM);
    checkint("nlist", MIN_NLIST, MAX_NLIST);
    checkstr("metric", METRICS);

    return true;
}
}  // namespace engine
}  // namespace milvus
