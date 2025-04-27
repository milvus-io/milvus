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

std::once_flag flag1, flag2, flag3, flag4, flag5, flag6, flag7, flag8, flag9,
    flag10;
std::once_flag traceFlag;

void
InitIndexSliceSize(const int64_t size) {
    std::call_once(
        flag1, [](int64_t size) { milvus::SetIndexSliceSize(size); }, size);
}

void
InitHighPriorityThreadCoreCoefficient(const float value) {
    std::call_once(
        flag2,
        [](float value) {
            milvus::SetHighPriorityThreadCoreCoefficient(value);
        },
        value);
}

void
InitMiddlePriorityThreadCoreCoefficient(const float value) {
    std::call_once(
        flag4,
        [](float value) {
            milvus::SetMiddlePriorityThreadCoreCoefficient(value);
        },
        value);
}

void
InitLowPriorityThreadCoreCoefficient(const float value) {
    std::call_once(
        flag5,
        [](float value) { milvus::SetLowPriorityThreadCoreCoefficient(value); },
        value);
}

void
InitCpuNum(const int value) {
    std::call_once(
        flag3, [](int value) { milvus::SetCpuNum(value); }, value);
}

void
InitDefaultExprEvalBatchSize(int64_t val) {
    std::call_once(
        flag6,
        [](int val) { milvus::SetDefaultExecEvalExprBatchSize(val); },
        val);
}

void
InitDefaultJSONKeyStatsCommitInterval(int64_t val) {
    std::call_once(
        flag8,
        [](int val) { milvus::SetDefaultJSONKeyStatsCommitInterval(val); },
        val);
}

void
InitDefaultGrowingJSONKeyStatsEnable(bool val) {
    std::call_once(
        flag9,
        [](bool val) { milvus::SetDefaultGrowingJSONKeyStatsEnable(val); },
        val);
}

void
InitDefaultOptimizeExprEnable(bool val) {
    std::call_once(
        flag10,
        [](bool val) { milvus::SetDefaultOptimizeExprEnable(val); },
        val);
}

void
InitTrace(CTraceConfig* config) {
    auto traceConfig = milvus::tracer::TraceConfig{config->exporter,
                                                   config->sampleFraction,
                                                   config->jaegerURL,
                                                   config->otlpEndpoint,
                                                   config->otlpMethod,
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
                                                   config->oltpSecure,
                                                   config->nodeID};
    milvus::tracer::initTelemetry(traceConfig);
}
