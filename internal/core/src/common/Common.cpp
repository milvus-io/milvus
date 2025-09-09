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
#include "gflags/gflags.h"
#include "log/Log.h"

namespace milvus {

std::atomic<int64_t> FILE_SLICE_SIZE(DEFAULT_INDEX_FILE_SLICE_SIZE);
std::atomic<int64_t> EXEC_EVAL_EXPR_BATCH_SIZE(
    DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE);
std::atomic<int64_t> DELETE_DUMP_BATCH_SIZE(DEFAULT_DELETE_DUMP_BATCH_SIZE);
std::atomic<bool> OPTIMIZE_EXPR_ENABLED(DEFAULT_OPTIMIZE_EXPR_ENABLED);

std::atomic<bool> GROWING_JSON_KEY_STATS_ENABLED(
    DEFAULT_GROWING_JSON_KEY_STATS_ENABLED);
std::atomic<bool> CONFIG_PARAM_TYPE_CHECK_ENABLED(
    DEFAULT_CONFIG_PARAM_TYPE_CHECK_ENABLED);

void
SetIndexSliceSize(const int64_t size) {
    FILE_SLICE_SIZE.store(size << 20);
    LOG_INFO("set config index slice size (byte): {}", FILE_SLICE_SIZE.load());
}

void
SetDefaultExecEvalExprBatchSize(int64_t val) {
    EXEC_EVAL_EXPR_BATCH_SIZE.store(val);
    LOG_INFO("set default expr eval batch size: {}",
             EXEC_EVAL_EXPR_BATCH_SIZE.load());
}

void
SetDefaultDeleteDumpBatchSize(int64_t val) {
    DELETE_DUMP_BATCH_SIZE.store(val);
    LOG_INFO("set default delete dump batch size: {}",
             DELETE_DUMP_BATCH_SIZE.load());
}

void
SetDefaultOptimizeExprEnable(bool val) {
    OPTIMIZE_EXPR_ENABLED.store(val);
    LOG_INFO("set default optimize expr enabled: {}",
             OPTIMIZE_EXPR_ENABLED.load());
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
        gflags::SetCommandLineOption("minloglevel", "0");
        gflags::SetCommandLineOption("v", "5");
    } else if (strcmp(level, "trace") == 0) {
        gflags::SetCommandLineOption("minloglevel", "0");
        gflags::SetCommandLineOption("v", "6");
    } else {
        gflags::SetCommandLineOption("v", "4");
        if (strcmp(level, "info") == 0) {
            gflags::SetCommandLineOption("minloglevel", "0");
        } else if (strcmp(level, "warn") == 0) {
            gflags::SetCommandLineOption("minloglevel", "1");
        } else if (strcmp(level, "error") == 0) {
            gflags::SetCommandLineOption("minloglevel", "2");
        }
    }
}

}  // namespace milvus
