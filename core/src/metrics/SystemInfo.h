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

#include <chrono>
#include <string>
#include <utility>
#include <vector>

namespace milvus {

class SystemInfo {
 public:
    static double
    CpuUtilizationRatio();

    static int64_t
    CpuTemperature();

#ifdef MILVUS_GPU_VERSION
    static void
    GpuInit();

    static int64_t
    GpuUtilizationRatio(int64_t device_id);

    static int64_t
    GpuTemperature(int64_t device_id);

    static int64_t
    GpuMemUsage(int64_t device_id);

    static int64_t
    GpuMemTotal(int64_t device_id);

    static int64_t
    GpuMemAvailable(int64_t device_id);
#endif

    static int64_t
    MemUsage();

    static int64_t
    MemTotal();

    static int64_t
    MemAvailable();

    static int64_t
    NetworkInOctets();

    static int64_t
    NetworkOutOctets();

    static double
    FloatingReadSpeed();

    static double
    FloatingWriteSpeed();

 private:
    static int64_t
    ParseLine(char* line);

    static std::pair<int64_t, int64_t>
    Octets();
};

}  // namespace milvus
