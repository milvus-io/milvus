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

#include "utils/LogUtil.h"

#include <ctype.h>
#include <libgen.h>
#include <string>

namespace milvus {
namespace server {

namespace {
static int global_idx = 0;
static int debug_idx = 0;
static int warning_idx = 0;
static int trace_idx = 0;
static int error_idx = 0;
static int fatal_idx = 0;
}  // namespace

// TODO(yzb) : change the easylogging library to get the log level from parameter rather than filename
void
RolloutHandler(const char* filename, std::size_t size, el::Level level) {
    char* dirc = strdup(filename);
    char* basec = strdup(filename);
    char* dir = dirname(dirc);
    char* base = basename(basec);

    std::string s(base);
    std::stringstream ss;
    std::string list[] = {"\\", " ", "\'", "\"", "*", "\?", "{", "}", ";", "<",
                          ">",  "|", "^",  "&",  "$", "#",  "!", "`", "~"};
    std::string::size_type position;
    for (auto substr : list) {
        position = 0;
        while ((position = s.find_first_of(substr, position)) != std::string::npos) {
            s.insert(position, "\\");
            position += 2;
        }
    }
    int ret;
    std::string m(std::string(dir) + "/" + s);
    s = m;
    if (level == el::Level::Global) {
        s.append("." + std::to_string(++global_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if (level == el::Level::Debug) {
        s.append("." + std::to_string(++debug_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if (level == el::Level::Warning) {
        s.append("." + std::to_string(++warning_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if (level == el::Level::Trace) {
        s.append("." + std::to_string(++trace_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if (level == el::Level::Error) {
        s.append("." + std::to_string(++error_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if (level == el::Level::Fatal) {
        s.append("." + std::to_string(++fatal_idx));
        ret = rename(m.c_str(), s.c_str());
    } else {
        s.append("." + std::to_string(++global_idx));
        ret = rename(m.c_str(), s.c_str());
    }
}

Status
InitLog(const std::string& log_config_file) {
    el::Configurations conf(log_config_file);
    el::Loggers::reconfigureAllLoggers(conf);

    el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
    el::Helpers::installPreRollOutCallback(RolloutHandler);
    el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog);

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
