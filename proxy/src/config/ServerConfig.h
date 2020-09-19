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

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "config/ConfigType.h"

namespace milvus {

extern std::mutex&
GetConfigMutex();

template <typename T>
class ConfigValue {
 public:
    explicit ConfigValue(T init_value) : value(std::move(init_value)) {
    }

    const T&
    operator()() {
        std::lock_guard<std::mutex> lock(GetConfigMutex());
        return value;
    }

 public:
    T value;
};

enum ClusterRole {
    RW = 1,
    RO,
};

enum SimdType {
    AUTO = 1,
    SSE,
    AVX2,
    AVX512,
};

const configEnum SimdMap{
    {"auto", SimdType::AUTO},
    {"sse", SimdType::SSE},
    {"avx2", SimdType::AVX2},
    {"avx512", SimdType::AVX512},
};

struct ServerConfig {
    using String = ConfigValue<std::string>;
    using Bool = ConfigValue<bool>;
    using Integer = ConfigValue<int64_t>;
    using Floating = ConfigValue<double>;

    String timezone{"unknown"};
    Integer query_node_num{1};

    struct Network {
        String address{"unknown"};
        Integer port{0};
    } network;

    struct Pulsar{
        String address{"localhost"};
        Integer port{6650};
    }pulsar;

    struct Master{
      String address{"localhost"};
      Integer  port{6000};
    }master;

    struct Etcd{
      String address{"localhost"};
      Integer port{2379};
      String rootpath{"by-dev"};
    }etcd;

    struct TimeSync{
      Integer interval{10};
    }timesync;

    struct Engine {
        Integer build_index_threshold{4096};
        Integer search_combine_nq{0};
        Integer use_blas_threshold{0};
        Integer omp_thread_num{0};
        Integer simd_type{0};
    } engine;

    struct Tracing {
        String json_config_path{"unknown"};
    } tracing;


    struct Logs {
        String level{"unknown"};
        struct Trace {
            Bool enable{false};
        } trace;
        String path{"unknown"};
        Integer max_log_file_size{0};
        Integer log_rotate_num{0};
    } logs;
};

extern ServerConfig config;
extern std::mutex _config_mutex;

std::vector<std::string>
ParsePreloadCollection(const std::string&);

std::vector<int64_t>
ParseGPUDevices(const std::string&);
}  // namespace milvus
