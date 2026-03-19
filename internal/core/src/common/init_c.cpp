// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstddef>
#include <mutex>
#include <string>

#include <arrow/io/interfaces.h>
#include <openssl/evp.h>
#include "common/init_c.h"
#include "common/Common.h"
#include "common/Tracer.h"
#include "common/init_c.h"
#include "exec/expression/ExprCache.h"
#include "log/Log.h"
#include "storage/ThreadPool.h"

std::once_flag traceFlag;
std::once_flag cpuNumFlag;
std::once_flag fipsFlag;

void
InitCpuNum(const int value) {
    std::call_once(cpuNumFlag, [value]() { milvus::InitCpuNum(value); });
}

void
SetIndexSliceSize(const int64_t size) {
    milvus::SetIndexSliceSize(size);
}

void
SetHighPriorityThreadCoreCoefficient(const float value) {
    milvus::SetHighPriorityThreadCoreCoefficient(value);
}

void
SetMiddlePriorityThreadCoreCoefficient(const float value) {
    milvus::SetMiddlePriorityThreadCoreCoefficient(value);
}

void
SetLowPriorityThreadCoreCoefficient(const float value) {
    milvus::SetLowPriorityThreadCoreCoefficient(value);
}

void
SetThreadPoolMaxThreadsSize(const int value) {
    milvus::SetThreadPoolMaxThreadsSize(value);
}

void
SetDefaultExprEvalBatchSize(int64_t val) {
    milvus::SetDefaultExecEvalExprBatchSize(val);
}

void
SetDefaultDeleteDumpBatchSize(int64_t val) {
    milvus::SetDefaultDeleteDumpBatchSize(val);
}

void
SetDefaultOptimizeExprEnable(bool val) {
    milvus::SetDefaultOptimizeExprEnable(val);
}

void
SetDefaultGrowingJSONKeyStatsEnable(bool val) {
    milvus::SetDefaultGrowingJSONKeyStatsEnable(val);
}

void
SetDefaultConfigParamTypeCheck(bool val) {
    milvus::SetDefaultConfigParamTypeCheck(val);
}

void
SetDefaultEnableParquetStatsSkipIndex(bool val) {
    milvus::SetDefaultEnableParquetStatsSkipIndex(val);
}

void
SetEnableLatestDeleteSnapshotOptimization(bool val) {
    milvus::SetEnableLatestDeleteSnapshotOptimization(val);
}

void
SetLogLevel(const char* level) {
    milvus::SetLogLevel(level);
}

void
SetExprResCacheEnable(bool val) {
    milvus::exec::ExprResCacheManager::SetEnabled(val);
}

void
SetExprResCacheConfig(const char* mode,
                      const char* disk_base_path,
                      int64_t mem_max_bytes,
                      bool compression_enabled,
                      int32_t admission_threshold,
                      int64_t mem_min_eval_duration_us,
                      int64_t disk_max_file_size,
                      int64_t disk_min_eval_duration_us) {
    milvus::exec::CacheConfig config;
    std::string mode_value = mode == nullptr ? "" : std::string(mode);
    if (mode_value == "disk") {
        config.mode = milvus::exec::CacheMode::Disk;
    } else if (mode_value == "memory") {
        config.mode = milvus::exec::CacheMode::Memory;
    } else {
        LOG_WARN("invalid expr result cache mode '{}', disabling cache",
                 mode_value);
        milvus::exec::ExprResCacheManager::SetEnabled(false);
        return;
    }

    if ((config.mode == milvus::exec::CacheMode::Memory &&
         mem_max_bytes <= 0) ||
        (config.mode == milvus::exec::CacheMode::Disk &&
         disk_max_file_size <= 0)) {
        LOG_WARN("invalid expr result cache size config, disabling cache");
        milvus::exec::ExprResCacheManager::SetEnabled(false);
        return;
    }

    config.disk_base_path =
        disk_base_path == nullptr ? std::string() : std::string(disk_base_path);
    config.mem_max_bytes = static_cast<size_t>(mem_max_bytes);
    config.compression_enabled = compression_enabled;
    if (admission_threshold < 1) {
        admission_threshold = 1;
    } else if (admission_threshold > 255) {
        admission_threshold = 255;
    }
    config.admission_threshold = static_cast<uint8_t>(admission_threshold);
    config.mem_min_eval_duration_us =
        mem_min_eval_duration_us < 0 ? 0 : mem_min_eval_duration_us;
    config.disk_max_file_size = static_cast<uint64_t>(disk_max_file_size);
    config.disk_min_eval_duration_us =
        disk_min_eval_duration_us < 0 ? 0 : disk_min_eval_duration_us;

    milvus::exec::ExprResCacheManager::Instance().SetConfig(config);
}

void
SetArrowIOThreadPoolCapacity(int threads) {
    if (threads <= 0) {
        return;
    }
    auto status = arrow::io::SetIOThreadPoolCapacity(threads);
    if (!status.ok()) {
        LOG_WARN("failed to set arrow io thread pool capacity to {}: {}",
                 threads,
                 status.ToString());
        return;
    }
    LOG_INFO("arrow io thread pool capacity set to {}", threads);
}

void
LogOpenSSLFIPSStatus() {
    std::call_once(fipsFlag, []() {
        LOG_INFO("Milvus FIPS in OpenSSL: {}",
                 EVP_default_properties_is_fips_enabled(NULL) ? "enabled"
                                                              : "disabled");
    });
}

void
InitTrace(CTraceConfig* config) {
    auto traceConfig = milvus::tracer::TraceConfig{config->exporter,
                                                   config->sampleFraction,
                                                   config->jaegerURL,
                                                   config->otlpEndpoint,
                                                   config->otlpMethod,
                                                   config->otlpHeaders,
                                                   config->oltpSecure,
                                                   config->nodeID};
    std::call_once(
        traceFlag,
        [](const milvus::tracer::TraceConfig& c) {
            milvus::tracer::initTelemetry(c);
        },
        traceConfig);
}

void
SetTrace(CTraceConfig* config) {
    auto traceConfig = milvus::tracer::TraceConfig{config->exporter,
                                                   config->sampleFraction,
                                                   config->jaegerURL,
                                                   config->otlpEndpoint,
                                                   config->otlpMethod,
                                                   config->otlpHeaders,
                                                   config->oltpSecure,
                                                   config->nodeID};
    milvus::tracer::initTelemetry(traceConfig);
}
