// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "easyloggingpp/easylogging++.h"
#include "utils/Status.h"
#include "value/config/ServerConfig.h"

#include <sstream>
#include <string>
#include <unordered_map>

namespace milvus {

class LogMgr {
 public:
    static Status
    InitLog(bool trace_enable,
            const std::string& level,
            const std::string& logs_path,
            const std::string& filename,
            int64_t max_log_file_size,
            int64_t delete_exceeds,
            bool log_to_stdout,
            bool log_to_file);

    static void
    RolloutHandler(const char* filename, std::size_t size, el::Level level);

 private:
    explicit LogMgr(std::string log_path);

    LogMgr&
    Default();

    LogMgr&
    Filename(const std::string& filename);

    /* Non-const for fiu to injecting error */
    LogMgr&
    Level(std::unordered_map<std::string, bool>& enables);

    LogMgr&
    To(bool log_to_stdout, bool log_to_file);

    LogMgr&
    Rotate(int64_t max_log_file_size, int64_t log_rotate_num);

    void
    Setup();

 private:
    static void
    rename_and_delete(const std::string& filename, int64_t idx);

    static std::unordered_map<std::string, bool>
    parse_level(const std::string& level);

    /**
     * @brief Configures if output corresponding level log
     */
    static void
    enable(el::Configurations& default_conf, el::Level level, bool enable);

 private:
    el::Configurations el_config_;
    std::string logs_path_;

 private:
    static int trace_idx;
    static int global_idx;
    static int debug_idx;
    static int info_idx;
    static int warning_idx;
    static int error_idx;
    static int fatal_idx;
    static int64_t logs_delete_exceeds;
    static bool enable_log_delete;

    const int64_t MAX_LOG_FILE_SIZE_MIN = 536870912;  /* 512 MB */
    const int64_t MAX_LOG_FILE_SIZE_MAX = 4294967296; /* 4 GB */
    const int64_t LOG_ROTATE_NUM_MIN = 0;
    const int64_t LOG_ROTATE_NUM_MAX = 1024;
};

}  // namespace milvus
