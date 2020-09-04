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

const configEnum ClusterRoleMap{
    {"rw", ClusterRole::RW},
    {"ro", ClusterRole::RO},
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

enum ClusteringType {
    K_MEANS = 1,
    K_MEANS_PLUS_PLUS,
};

const configEnum ClusteringMap{
    {"k-means", ClusteringType::K_MEANS},
    {"k-means++", ClusteringType::K_MEANS_PLUS_PLUS},
};

struct ServerConfig {
    using String = ConfigValue<std::string>;
    using Bool = ConfigValue<bool>;
    using Integer = ConfigValue<int64_t>;
    using Floating = ConfigValue<double>;

    String version{"unknown"};

    struct Cluster {
        Bool enable{false};
        Integer role{0};
    } cluster;

    struct General {
        String timezone{"unknown"};
        String meta_uri{"unknown"};
    } general;

    struct Network {
        struct Bind {
            String address{"unknown"};
            Integer port{0};
        } bind;
        struct Http {
            Bool enable{false};
            Integer port{0};
        } http;
    } network;

    struct Storage {
        String path{"unknown"};
        Integer auto_flush_interval{0};
    } storage;

    struct Cache {
        Integer cache_size{0};
        Floating cpu_cache_threshold{0.0};
        Integer insert_buffer_size{0};
        Bool cache_insert_data{false};
        String preload_collection{"unknown"};
    } cache;

    struct Metric {
        Bool enable{false};
        String address{"unknown"};
        Integer port{0};
    } metric;

    struct Engine {
        Integer build_index_threshold{4096};
        Integer search_combine_nq{0};
        Integer use_blas_threshold{0};
        Integer omp_thread_num{0};
        Integer clustering_type{0};
        Integer simd_type{0};
    } engine;

    struct GPU {
        Bool enable{false};
        Integer cache_size{0};
        Floating cache_threshold{0.0};
        Integer gpu_search_threshold{0};
        String search_devices{"unknown"};
        String build_index_devices{"unknown"};
    } gpu;

    struct Tracing {
        String json_config_path{"unknown"};
    } tracing;

    struct WAL {
        Bool enable{false};
        Bool recovery_error_ignore{false};
        Integer buffer_size{0};
        String path{"unknown"};
    } wal;

    struct Logs {
        String level{"unknown"};
        struct Trace {
            Bool enable{false};
        } trace;
        String path{"unknown"};
        Integer max_log_file_size{0};
        Integer log_rotate_num{0};
    } logs;

    struct System {
        struct Lock {
            Bool enable{false};
        } lock;
    } system;

    struct Transcript {
        Bool enable{false};
        String replay{""};
    } transcript;
};

extern ServerConfig config;

std::vector<std::string>
ParsePreloadCollection(const std::string&);

std::vector<int64_t>
ParseGPUDevices(const std::string&);
}  // namespace milvus
