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

#include "common/Common.h"
#include "log/Log.h"

namespace milvus {

int CPU_NUM = DEFAULT_CPU_NUM;

std::atomic<int64_t> FILE_SLICE_SIZE(DEFAULT_INDEX_FILE_SLICE_SIZE);
std::atomic<float> HIGH_PRIORITY_THREAD_CORE_COEFFICIENT(
    DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT);
std::atomic<float> MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT(
    DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT);
std::atomic<float> LOW_PRIORITY_THREAD_CORE_COEFFICIENT(
    DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT);
std::atomic<int64_t> EXEC_EVAL_EXPR_BATCH_SIZE(
    DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE);
std::atomic<bool> OPTIMIZE_EXPR_ENABLED(DEFAULT_OPTIMIZE_EXPR_ENABLED);

std::atomic<int64_t> JSON_KEY_STATS_COMMIT_INTERVAL(
    DEFAULT_JSON_KEY_STATS_COMMIT_INTERVAL);
std::atomic<bool> GROWING_JSON_KEY_STATS_ENABLED(
    DEFAULT_GROWING_JSON_KEY_STATS_ENABLED);
std::atomic<bool> CONFIG_PARAM_TYPE_CHECK_ENABLED(
    DEFAULT_CONFIG_PARAM_TYPE_CHECK_ENABLED);

void
InitCpuNum(const int num) {
    CPU_NUM = num;
    LOG_INFO("set cpu num: {}", CPU_NUM);
}

void
SetIndexSliceSize(const int64_t size) {
    FILE_SLICE_SIZE.store(size << 20);
    LOG_INFO("set config index slice size (byte): {}", FILE_SLICE_SIZE.load());
}

void
SetHighPriorityThreadCoreCoefficient(const float coefficient) {
    HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.store(coefficient);
    LOG_INFO("set high priority thread pool core coefficient: {}",
             HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.load());
}

void
SetMiddlePriorityThreadCoreCoefficient(const float coefficient) {
    MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.store(coefficient);
    LOG_INFO("set middle priority thread pool core coefficient: {}",
             MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load());
}

void
SetLowPriorityThreadCoreCoefficient(const float coefficient) {
    LOW_PRIORITY_THREAD_CORE_COEFFICIENT.store(coefficient);
    LOG_INFO("set low priority thread pool core coefficient: {}",
             LOW_PRIORITY_THREAD_CORE_COEFFICIENT.load());
}

void
SetDefaultExecEvalExprBatchSize(int64_t val) {
    EXEC_EVAL_EXPR_BATCH_SIZE.store(val);
    LOG_INFO("set default expr eval batch size: {}",
             EXEC_EVAL_EXPR_BATCH_SIZE.load());
}

void
SetDefaultOptimizeExprEnable(bool val) {
    OPTIMIZE_EXPR_ENABLED.store(val);
    LOG_INFO("set default optimize expr enabled: {}",
             OPTIMIZE_EXPR_ENABLED.load());
}

void
SetDefaultJSONKeyStatsCommitInterval(int64_t val) {
    JSON_KEY_STATS_COMMIT_INTERVAL.store(val);
    LOG_INFO("set default json key Stats commit interval: {}",
             JSON_KEY_STATS_COMMIT_INTERVAL.load());
}

void
SetDefaultGrowingJSONKeyStatsEnable(bool val) {
    GROWING_JSON_KEY_STATS_ENABLED.store(val);
    LOG_INFO("set default growing json key index enable: {}",
             GROWING_JSON_KEY_STATS_ENABLED.load());
}

void
SetDefaultConfigParamTypeCheck(bool val) {
    CONFIG_PARAM_TYPE_CHECK_ENABLED.store(val);
    LOG_INFO("set default config param type check enabled: {}",
             CONFIG_PARAM_TYPE_CHECK_ENABLED.load());
}

void
SetLogLevel(const char* level) {
    LOG_INFO("set log level: {}", level);
    if (strcmp(level, "debug") == 0) {
        FLAGS_v = 5;
    } else if (strcmp(level, "trace") == 0) {
        FLAGS_v = 6;
    } else {
        FLAGS_v = 4;
    }
}

}  // namespace milvus
