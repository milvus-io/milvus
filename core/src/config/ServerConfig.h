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

struct ServerConfig {
    ConfigValue<std::string> version{"unknown"};

    struct Cluster {
        ConfigValue<bool> enable{false};
        ConfigValue<int64_t> role{0};
    } cluster;

    struct General {
        ConfigValue<std::string> timezone{"unknown"};
        ConfigValue<std::string> meta_uri{"unknown"};
    } general;

    struct Network {
        struct Bind {
            ConfigValue<std::string> address{"unknown"};
            ConfigValue<int64_t> port{0};
        } bind;
        struct Http {
            ConfigValue<bool> enable{false};
            ConfigValue<int64_t> port{0};
        } http;
    } network;

    struct DB {
        ConfigValue<double> archive_disk_threshold{0.0};
        ConfigValue<int64_t> archive_days_threshold{0};
    } db;

    struct Storage {
        ConfigValue<std::string> path{"unknown"};
        ConfigValue<int64_t> auto_flush_interval{0};
        ConfigValue<int64_t> file_cleanup_timeout{0};
    } storage;

    struct Cache {
        ConfigValue<int64_t> cache_size{0};
        ConfigValue<double> cpu_cache_threshold{0.0};
        ConfigValue<int64_t> insert_buffer_size{0};
        ConfigValue<bool> cache_insert_data{false};
        ConfigValue<std::string> preload_collection{"unknown"};
    } cache;

    struct Metric {
        ConfigValue<bool> enable{false};
        ConfigValue<std::string> address{"unknown"};
        ConfigValue<int64_t> port{0};
    } metric;

    struct Engine {
        ConfigValue<int64_t> search_combine_nq{0};
        ConfigValue<int64_t> use_blas_threshold{0};
        ConfigValue<int64_t> omp_thread_num{0};
        ConfigValue<int64_t> simd_type{0};
    } engine;

    struct GPU {
        ConfigValue<bool> enable{false};
        ConfigValue<int64_t> cache_size{0};
        ConfigValue<double> cache_threshold{0.0};
        ConfigValue<int64_t> gpu_search_threshold{0};
        ConfigValue<std::string> search_devices{"unknown"};
        ConfigValue<std::string> build_index_devices{"unknown"};
    } gpu;

    struct Tracing {
        ConfigValue<std::string> json_config_path{"unknown"};
    } tracing;

    struct WAL {
        ConfigValue<bool> enable{false};
        ConfigValue<bool> recovery_error_ignore{false};
        ConfigValue<int64_t> buffer_size{0};
        ConfigValue<std::string> path{"unknown"};
    } wal;

    struct Logs {
        ConfigValue<std::string> level{"unknown"};
        struct Trace {
            ConfigValue<bool> enable{false};
        } trace;
        ConfigValue<std::string> path{"unknown"};
        ConfigValue<int64_t> max_log_file_size{0};
        ConfigValue<int64_t> log_rotate_num{0};
    } logs;
};

extern ServerConfig config;
extern std::mutex _config_mutex;

std::vector<std::string>
ParsePreloadCollection(const std::string&);

std::vector<int64_t>
ParseGPUDevices(const std::string&);
}  // namespace milvus
