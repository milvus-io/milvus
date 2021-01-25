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

#include "utils/Status.h"
#include "value/config/ServerConfig.h"

namespace milvus {
namespace engine {

class KnowhereResource {
 public:
    static Status
    Initialize();

    static Status
    Finalize();

    static void
    SetSimdType(const int64_t& st);

    static Status
    FaissHook();

    static void
    SetBlasThreshold(const int64_t& use_blas_threshold);

    static void
    SetEarlyStopThreshold(const double& early_stop_threshold);

    static void
    SetClusteringType(const int64_t& clustering_type);

    static void
    SetStatisticsLevel(const int64_t& stat_level);

    static void
    SetFaissLogHandler();

    static void
    SetNGTLogHandler();

    static void
    SetGPUEnable(bool enable_gpu);
};

}  // namespace engine
}  // namespace milvus
