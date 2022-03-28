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

#include "value/Value.h"
#include "value/ValueType.h"

namespace milvus {

enum ClusterRole {
    RW = 1,
    RO,
};

const valueEnum ClusterRoleMap{
    {"rw", ClusterRole::RW},
    {"ro", ClusterRole::RO},
};

enum SimdType {
    AUTO = 1,
    SSE4_2,
    AVX2,
    AVX512,
};

const valueEnum SimdMap{
    {"auto", SimdType::AUTO},
    {"sse4_2", SimdType::SSE4_2},
    {"avx2", SimdType::AVX2},
    {"avx512", SimdType::AVX512},
};

enum ClusteringType {
    K_MEANS = 1,
    K_MEANS_PLUS_PLUS,
};

const valueEnum ClusteringMap{
    {"k-means", ClusteringType::K_MEANS},
    {"k-means++", ClusteringType::K_MEANS_PLUS_PLUS},
};

struct ServerConfig {
    using String = Value<std::string>;
    using Bool = Value<bool>;
    using Integer = Value<int64_t>;
    using Floating = Value<double>;

    String version;

    struct Cluster {
        Bool enable;
        Integer role;
        String node_id;
    } cluster;

    struct General {
        String timezone;
        String meta_uri;
        Integer stale_snapshots_count;
        Integer stale_snapshots_duration;
    } general;

    struct Network {
        struct Bind {
            String address;
            Integer port;
        } bind;
        struct Http {
            Bool enable;
            Integer port;
        } http;
    } network;

    struct Storage {
        String path;
        Integer auto_flush_interval;
    } storage;

    struct Cache {
        Integer cache_size;
        Floating cpu_cache_threshold;
        Integer insert_buffer_size;
        Bool cache_insert_data;
        String preload_collection;
        Integer max_concurrent_insert_request_size;
    } cache;

    struct Engine {
        Integer max_partition_num;
        Integer build_index_threshold;
        Integer search_combine_nq;
        Integer use_blas_threshold;
        Integer omp_thread_num;
        Integer clustering_type;
        Integer simd_type;
        Integer statistics_level;
    } engine;

    struct GPU {
        Bool enable;
        Integer cache_size;
        Floating cache_threshold;
        Integer gpu_search_threshold;
        String search_devices;
        String build_index_devices;
    } gpu;

    struct Tracing {
        String json_config_path;
    } tracing;

    struct WAL {
        Bool enable;
        Bool sync_mode;
        Bool recovery_error_ignore;
        Integer buffer_size;
        String path;
    } wal;

    struct Logs {
        struct Trace {
            Bool enable;
        } trace;
        String path;
        Integer max_log_file_size;
        Integer log_rotate_num;
        Bool log_to_stdout;
        Bool log_to_file;
    } logs;

    struct Log {
        String min_messages;
        Integer rotation_age;
        Integer rotation_size;
    } log;

    struct System {
        struct Lock {
            Bool enable;
        } lock;
    } system;

    struct Transcript {
        Bool enable;
        String replay;
    } transcript;
};

extern ServerConfig config;

std::vector<std::string>
ParsePreloadCollection(const std::string&);

std::vector<int64_t>
ParseGPUDevices(const std::string&);
}  // namespace milvus
