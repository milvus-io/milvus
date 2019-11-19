// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "scheduler/Utils.h"
#include "server/Config.h"
#include "utils/Log.h"

#ifdef MILVUS_GPU_VERSION
#include <cuda_runtime.h>
#endif
#include <chrono>
#include <set>
#include <string>

namespace milvus {
namespace scheduler {

uint64_t
get_current_timestamp() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return millis;
}

uint64_t
get_num_gpu() {
    int n_devices = 0;
#ifdef MILVUS_GPU_VERSION
    cudaGetDeviceCount(&n_devices);
#endif
    return n_devices;
}

std::vector<uint64_t>
get_gpu_pool() {
    std::vector<uint64_t> gpu_pool;

    server::Config& config = server::Config::GetInstance();
    std::vector<std::string> pool;
    Status s = config.GetResourceConfigSearchResources(pool);
    if (!s.ok()) {
        SERVER_LOG_ERROR << s.message();
    }

    std::set<uint64_t> gpu_ids;

    for (auto& resource : pool) {
        if (resource == "cpu") {
            continue;
        } else {
            if (resource.length() < 4 || resource.substr(0, 3) != "gpu") {
                // error
                exit(-1);
            }
            auto gpu_id = std::stoi(resource.substr(3));
            if (gpu_id >= scheduler::get_num_gpu()) {
                // error
                exit(-1);
            }
            gpu_ids.insert(gpu_id);
        }
    }

    for (auto& gpu_id : gpu_ids) {
        gpu_pool.push_back(gpu_id);
    }

    return gpu_pool;
}

std::vector<int64_t>
get_build_resources() {
    std::vector<int64_t> gpu_pool;

    server::Config& config = server::Config::GetInstance();
    std::vector<std::string> pool;
    Status s = config.GetResourceConfigIndexBuildResources(pool);
    if (!s.ok()) {
        SERVER_LOG_ERROR << s.message();
    }

    std::set<uint64_t> gpu_ids;

    for (auto& resource : pool) {
        if (resource == "cpu") {
            gpu_pool.push_back(server::CPU_DEVICE_ID);
            continue;
        } else {
            if (resource.length() < 4 || resource.substr(0, 3) != "gpu") {
                // error
                exit(-1);
            }
            auto gpu_id = std::stoi(resource.substr(3));
            if (gpu_id >= scheduler::get_num_gpu()) {
                // error
                exit(-1);
            }
            gpu_ids.insert(gpu_id);
        }
    }

    for (auto& gpu_id : gpu_ids) {
        gpu_pool.push_back(gpu_id);
    }

    return gpu_pool;
}

}  // namespace scheduler
}  // namespace milvus
