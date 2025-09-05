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

#include <mutex>
#include "common/init_c.h"
#include "common/Common.h"
#include "common/Tracer.h"
#include "storage/ThreadPool.h"
#include "log/Log.h"
#include "exec/expression/ExprCache.h"

std::once_flag traceFlag;
std::once_flag cpuNumFlag;

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
SetLogLevel(const char* level) {
    milvus::SetLogLevel(level);
}

void
SetExprResCacheEnable(bool val) {
    milvus::exec::ExprResCacheManager::SetEnabled(val);
}

void
SetExprResCacheCapacityBytes(int64_t bytes) {
    milvus::exec::ExprResCacheManager::Instance().SetCapacityBytes(
        static_cast<size_t>(bytes));
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
