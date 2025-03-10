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
int64_t HIGH_PRIORITY_THREAD_CORE_COEFFICIENT =
    DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
int64_t MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT =
    DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT;
int64_t LOW_PRIORITY_THREAD_CORE_COEFFICIENT =
    DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT;
int CPU_NUM = DEFAULT_CPU_NUM;
int64_t EXEC_EVAL_EXPR_BATCH_SIZE = DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE;

int64_t JSON_INDEX_MEMORY_BUDGET = DEFAULT_JSON_INDEX_MEMORY_BUDGET;
int64_t JSON_INDEX_COMMIT_INTERVAL = DEFAULT_JSON_INDEX_COMMIT_INTERVAL;
bool JSON_INDEX_ENABLED = DEFAULT_JSON_INDEX_ENABLED;
bool SCALAR_INDEX_HAS_RAW_DATA = DEFAULT_SCALAR_INDEX_HAS_RAW_DATA;

void
SetIndexSliceSize(const int64_t size) {
    FILE_SLICE_SIZE = size << 20;
    LOG_INFO("set config index slice size (byte): {}", FILE_SLICE_SIZE);
}

void
SetHighPriorityThreadCoreCoefficient(const int64_t coefficient) {
    HIGH_PRIORITY_THREAD_CORE_COEFFICIENT = coefficient;
    LOG_INFO("set high priority thread pool core coefficient: {}",
             HIGH_PRIORITY_THREAD_CORE_COEFFICIENT);
}

void
SetMiddlePriorityThreadCoreCoefficient(const int64_t coefficient) {
    MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT = coefficient;
    LOG_INFO("set middle priority thread pool core coefficient: {}",
             MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT);
}

void
SetLowPriorityThreadCoreCoefficient(const int64_t coefficient) {
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
SetDefaultJSONKeyIndexMemoryBudget(int64_t val) {
    JSON_INDEX_MEMORY_BUDGET = val;
    LOG_INFO("set default json key index memory budget: {}",
             JSON_INDEX_MEMORY_BUDGET);
}

void
SetDefaultJSONKeyIndexCommitInterval(int64_t val) {
    JSON_INDEX_COMMIT_INTERVAL = val;
    LOG_INFO("set default json key index commit interval: {}",
             JSON_INDEX_COMMIT_INTERVAL);
}

void
SetDefaultJSONKeyIndexEnable(bool val) {
    JSON_INDEX_ENABLED = val;
    LOG_INFO("set default json key index enable: {}", JSON_INDEX_ENABLED);
}

void
SetDefaultScalarIndexHasRawData(bool val) {
    SCALAR_INDEX_HAS_RAW_DATA = val;
    LOG_INFO("set default scalar index has raw data: {}",
             SCALAR_INDEX_HAS_RAW_DATA);
}
}  // namespace milvus
