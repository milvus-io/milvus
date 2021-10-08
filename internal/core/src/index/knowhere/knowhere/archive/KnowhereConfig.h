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

#pragma once

#include <vector>
#include <string>

#include "utils/Status.h"

namespace milvus {
namespace engine {

class KnowhereConfig {
 public:
    /**
     * set SIMD type
     */
    enum SimdType {
        AUTO = 0,  // enable all and depend on the system
        SSE,       // only enable SSE
        AVX2,      // only enable AVX2
        AVX512,    // only enable AVX512
    };

    static std::string
    SetSimdType(const SimdType simd_type);

    /**
     * Set openblas threshold
     *   if nq < use_blas_threshold, calculated by omp
     *   else, calculated by openblas
     */
    static void
    SetBlasThreshold(const int64_t use_blas_threshold);

    /**
     * set Clustering early stop [0, 100]
     *   It is to reduce the number of iterations of K-means.
     *   Between each two iterations, if the optimization rate < early_stop_threshold, stop
     *   And if early_stop_threshold = 0, won't early stop
     */
    static void
    SetEarlyStopThreshold(const double early_stop_threshold);

    /**
     * set Clustering type
     */
    enum ClusteringType {
        K_MEANS,            // k-means (default)
        K_MEANS_PLUS_PLUS,  // k-means++
    };

    static void
    SetClusteringType(const ClusteringType clustering_type);

    /**
     * set Statistics Level [0, 3]
     */
    static void
    SetStatisticsLevel(const int64_t stat_level);

    // todo: add log level?
    /**
     * set Log handler
     */
    static void
    SetLogHandler();

#ifdef MILVUS_GPU_VERSION
    // todo: move to ohter file?
    /**
     * init GPU Resource
     */
    static void
    InitGPUResource(const std::vector<int64_t>& gpu_ids);

    /**
     * free GPU Resource
     */
    static void
    FreeGPUResource();
#endif
};

}  // namespace engine
}  // namespace milvus
