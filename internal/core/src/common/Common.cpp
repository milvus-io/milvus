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

int64_t FILE_SLICE_SIZE = DEFAULT_INDEX_FILE_SLICE_SIZE;
float HIGH_PRIORITY_THREAD_CORE_COEFFICIENT =
    DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
float MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT =
    DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT;
float LOW_PRIORITY_THREAD_CORE_COEFFICIENT =
    DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT;
int CPU_NUM = DEFAULT_CPU_NUM;
int64_t EXEC_EVAL_EXPR_BATCH_SIZE = DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE;

int64_t JSON_KEY_STATS_COMMIT_INTERVAL = DEFAULT_JSON_KEY_STATS_COMMIT_INTERVAL;
bool OPTIMIZE_EXPR_ENABLED = DEFAULT_OPTIMIZE_EXPR_ENABLED;
bool GROWING_JSON_KEY_STATS_ENABLED = DEFAULT_GROWING_JSON_KEY_STATS_ENABLED;

void
SetIndexSliceSize(const int64_t size) {
    FILE_SLICE_SIZE = size << 20;
    LOG_INFO("set config index slice size (byte): {}", FILE_SLICE_SIZE);
}

void
SetHighPriorityThreadCoreCoefficient(const float coefficient) {
    HIGH_PRIORITY_THREAD_CORE_COEFFICIENT = coefficient;
    LOG_INFO("set high priority thread pool core coefficient: {}",
             HIGH_PRIORITY_THREAD_CORE_COEFFICIENT);
}

void
SetMiddlePriorityThreadCoreCoefficient(const float coefficient) {
    MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT = coefficient;
    LOG_INFO("set middle priority thread pool core coefficient: {}",
             MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT);
}

void
SetLowPriorityThreadCoreCoefficient(const float coefficient) {
    LOW_PRIORITY_THREAD_CORE_COEFFICIENT = coefficient;
    LOG_INFO("set low priority thread pool core coefficient: {}",
             LOW_PRIORITY_THREAD_CORE_COEFFICIENT);
}

void
SetDefaultExecEvalExprBatchSize(int64_t val) {
    EXEC_EVAL_EXPR_BATCH_SIZE = val;
    LOG_INFO("set default expr eval batch size: {}", EXEC_EVAL_EXPR_BATCH_SIZE);
}

void
SetCpuNum(const int num) {
    CPU_NUM = num;
}

void
SetDefaultJSONKeyStatsCommitInterval(int64_t val) {
    JSON_KEY_STATS_COMMIT_INTERVAL = val;
    LOG_INFO("set default json key Stats commit interval: {}",
             JSON_KEY_STATS_COMMIT_INTERVAL);
}

void
SetDefaultGrowingJSONKeyStatsEnable(bool val) {
    GROWING_JSON_KEY_STATS_ENABLED = val;
    LOG_INFO("set default growing json key index enable: {}",
             GROWING_JSON_KEY_STATS_ENABLED);
}

void
SetDefaultOptimizeExprEnable(bool val) {
    OPTIMIZE_EXPR_ENABLED = val;
    LOG_INFO("set default optimize expr enabled: {}", OPTIMIZE_EXPR_ENABLED);
}

}  // namespace milvus
