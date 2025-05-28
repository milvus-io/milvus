// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "common/PrometheusClient.h"
#include <chrono>

namespace milvus::monitor {

const prometheus::Histogram::BucketBoundaries secondsBuckets = {
    std::chrono::duration<float>(std::chrono::microseconds(10)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(50)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(100)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(250)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(500)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(1)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(5)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(10)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(20)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(50)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(100)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(200)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(500)).count(),
    std::chrono::duration<float>(std::chrono::seconds(1)).count(),
    std::chrono::duration<float>(std::chrono::seconds(2)).count(),
    std::chrono::duration<float>(std::chrono::seconds(5)).count(),
    std::chrono::duration<float>(std::chrono::seconds(10)).count(),
};

const prometheus::Histogram::BucketBoundaries buckets = {1,
                                                         2,
                                                         4,
                                                         8,
                                                         16,
                                                         32,
                                                         64,
                                                         128,
                                                         256,
                                                         512,
                                                         1024,
                                                         2048,
                                                         4096,
                                                         8192,
                                                         16384,
                                                         32768,
                                                         65536};

const prometheus::Histogram::BucketBoundaries bytesBuckets = {
    1024,         // 1k
    8192,         // 8k
    65536,        // 64k
    262144,       // 256k
    524288,       // 512k
    1048576,      // 1M
    4194304,      // 4M
    8388608,      // 8M
    16777216,     // 16M
    67108864,     // 64M
    134217728,    // 128M
    268435456,    // 256M
    536870912,    // 512M
    1073741824};  // 1G

const prometheus::Histogram::BucketBoundaries ratioBuckets = {
    0.0,  0.05, 0.1,  0.15, 0.2,  0.25, 0.3,  0.35, 0.4,  0.45, 0.5,
    0.55, 0.6,  0.65, 0.7,  0.75, 0.8,  0.85, 0.9,  0.95, 1.0};

}  // namespace milvus::monitor
