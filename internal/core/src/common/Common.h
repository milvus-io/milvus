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

#pragma once

#include <atomic>
#include <iostream>
#include <utility>
#include <variant>
#include "common/Consts.h"
#include "storage/ThreadPool.h"

namespace milvus {

extern std::atomic<int64_t> FILE_SLICE_SIZE;
extern std::atomic<int64_t> EXEC_EVAL_EXPR_BATCH_SIZE;
extern std::atomic<int64_t> DELETE_DUMP_BATCH_SIZE;
extern std::atomic<bool> OPTIMIZE_EXPR_ENABLED;
extern std::atomic<bool> GROWING_JSON_KEY_STATS_ENABLED;
extern std::atomic<bool> CONFIG_PARAM_TYPE_CHECK_ENABLED;

void
SetIndexSliceSize(const int64_t size);

void
SetDefaultExecEvalExprBatchSize(int64_t val);

void
SetDefaultDeleteDumpBatchSize(int64_t val);

void
SetDefaultOptimizeExprEnable(bool val);

void
SetDefaultGrowingJSONKeyStatsEnable(bool val);

void
SetDefaultConfigParamTypeCheck(bool val);

void
SetLogLevel(const char* level);

struct BufferView {
    struct Element {
        const char* data_;
        uint32_t* offsets_;
        int start_;
        int end_;
    };

    std::variant<std::vector<Element>, std::pair<char*, size_t>> data_;
};

}  // namespace milvus
