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

#include <sstream>
#include <string>

namespace milvus {
namespace server {

Status
InitLog(bool trace_enable, bool debug_enable, bool info_enable, bool warning_enable, bool error_enable,
        bool fatal_enable, const std::string& logs_path, int64_t max_log_file_size, int64_t delete_exceeds);

void
RolloutHandler(const char* filename, std::size_t size, el::Level level);

#define SHOW_LOCATION
#ifdef SHOW_LOCATION
#define LOCATION_INFO "[" << sql::server::GetFileName(__FILE__) << ":" << __LINE__ << "] "
#else
#define LOCATION_INFO ""
#endif

void
LogConfigInFile(const std::string& path);

void
LogConfigInMem();

void
LogCpuInfo();

}  // namespace server
}  // namespace milvus
