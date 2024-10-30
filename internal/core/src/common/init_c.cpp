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

#include <memory>
#include <mutex>

#include "common/init_c.h"

#include <string>
#include "common/Slice.h"
#include "common/Common.h"
#include "common/Tracer.h"
#include "log/Log.h"

std::once_flag flag1, flag2, flag3, flag4, flag5, flag6;
std::once_flag traceFlag;

void
InitIndexSliceSize(const int64_t size) {
    std::call_once(
        flag1, [](int64_t size) { milvus::SetIndexSliceSize(size); }, size);
}

void
InitHighPriorityThreadCoreCoefficient(const int64_t value) {
    std::call_once(
        flag2,
        [](int64_t value) {
            milvus::SetHighPriorityThreadCoreCoefficient(value);
        },
        value);
}

void
InitMiddlePriorityThreadCoreCoefficient(const int64_t value) {
    std::call_once(
        flag4,
        [](int64_t value) {
            milvus::SetMiddlePriorityThreadCoreCoefficient(value);
        },
        value);
}

void
InitLowPriorityThreadCoreCoefficient(const int64_t value) {
    std::call_once(
        flag5,
        [](int64_t value) {
            milvus::SetLowPriorityThreadCoreCoefficient(value);
        },
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
