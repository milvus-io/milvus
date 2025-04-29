// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "pthread.h"
#include "config/ConfigKnowhere.h"
#include "fmt/core.h"
#include "log/Log.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/segcore_init_c.h"
#include "cachinglayer/Manager.h"

namespace milvus::segcore {

std::once_flag close_glog_once;

extern "C" void
SegcoreInit(const char* conf_file) {
    milvus::config::KnowhereInitImpl(conf_file);
}

// TODO merge small index config into one config map, including enable/disable small_index
extern "C" void
SegcoreSetChunkRows(const int64_t value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_chunk_rows(value);
}

extern "C" void
SegcoreSetEnableTempSegmentIndex(const bool value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_enable_interim_segment_index(value);
}

extern "C" void
SegcoreSetNlist(const int64_t value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_nlist(value);
}

extern "C" void
SegcoreSetNprobe(const int64_t value) {
    milvus::segcore::SegcoreConfig& config =
        milvus::segcore::SegcoreConfig::default_config();
    config.set_nprobe(value);
}

extern "C" void
SegcoreSetKnowhereBuildThreadPoolNum(const uint32_t num_threads) {
    milvus::config::KnowhereInitBuildThreadPool(num_threads);
}

extern "C" void
SegcoreSetKnowhereSearchThreadPoolNum(const uint32_t num_threads) {
    milvus::config::KnowhereInitSearchThreadPool(num_threads);
}

extern "C" void
SegcoreSetKnowhereGpuMemoryPoolSize(const uint32_t init_size,
                                    const uint32_t max_size) {
    milvus::config::KnowhereInitGPUMemoryPool(init_size, max_size);
}

// return value must be freed by the caller
extern "C" char*
SegcoreSetSimdType(const char* value) {
    LOG_DEBUG("set config simd_type: {}", value);
    auto real_type = milvus::config::KnowhereSetSimdType(value);
    char* ret = reinterpret_cast<char*>(malloc(real_type.length() + 1));
    AssertInfo(ret != nullptr, "memmory allocation for ret failed!");
    memcpy(ret, real_type.c_str(), real_type.length());
    ret[real_type.length()] = 0;
    return ret;
}

extern "C" void
SegcoreEnableKnowhereScoreConsistency() {
    milvus::config::EnableKnowhereScoreConsistency();
}

extern "C" void
SegcoreCloseGlog() {
    std::call_once(close_glog_once, [&]() {
        if (google::IsGoogleLoggingInitialized()) {
            google::ShutdownGoogleLogging();
        }
    });
}

extern "C" int32_t
GetCurrentIndexVersion() {
    return milvus::config::GetCurrentIndexVersion();
}

extern "C" int32_t
GetMinimalIndexVersion() {
    return milvus::config::GetMinimalIndexVersion();
}

extern "C" int32_t
GetMaximumIndexVersion() {
    return milvus::config::GetMaximumIndexVersion();
}

extern "C" void
SetThreadName(const char* name) {
#ifdef __linux__
    pthread_setname_np(pthread_self(), name);
#elif __APPLE__
    pthread_setname_np(name);
#endif
}

extern "C" void
ConfigureTieredStorage(const bool enabled_globally,
                       const int64_t memory_limit_bytes,
                       const int64_t disk_limit_bytes) {
    milvus::cachinglayer::Manager::ConfigureTieredStorage(
        enabled_globally, memory_limit_bytes, disk_limit_bytes);
}

}  // namespace milvus::segcore
