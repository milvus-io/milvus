// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "LogUtil.h"
#include "server/ServerConfig.h"
#include "easylogging++.h"

#include <ctype.h>

#include <string>
#include <libgen.h>


namespace zilliz {
namespace milvus {
namespace server {

namespace {
static int global_idx = 0;
static int debug_idx = 0;
static int warning_idx = 0;
static int trace_idx = 0;
static int error_idx = 0;
static int fatal_idx = 0;
}

// TODO(yzb) : change the easylogging library to get the log level from parameter rather than filename
void RolloutHandler(const char *filename, std::size_t size, el::Level level) {
    char *dirc = strdup(filename);
    char *basec = strdup(filename);
    char *dir = dirname(dirc);
    char *base = basename(basec);

    std::string s(base);
    std::stringstream ss;
    std::string
        list[] = {"\\", " ", "\'", "\"", "*", "\?", "{", "}", ";", "<", ">", "|", "^", "&", "$", "#", "!", "`", "~"};
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

int32_t InitLog(const std::string &log_config_file) {
#if 0
    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode log_config = config.GetConfig(CONFIG_LOG);
    const std::map<std::string, ConfigNode>& settings = log_config.GetChildren();

    std::string str_config;
    for(auto iter : settings) {
        str_config += "* ";
        str_config += iter.first;
        str_config += ":";
        str_config.append("\n");

        auto sub_configs = iter.second.GetConfig();
        for(auto it_sub : sub_configs) {
            str_config += "    ";
            str_config += it_sub.first;
            str_config += " = ";
            std::string temp = it_sub.first;
            std::transform(temp.begin(), temp.end(), temp.begin(), ::tolower);
            bool is_text = (temp == "format" || temp == "filename");
            if(is_text){
                str_config += "\"";
            }
            str_config += it_sub.second;
            if(is_text){
                str_config += "\"";
            }
            str_config.append("\n");
        }
    }

    el::Configurations conf;
    conf.parseFromText(str_config);
#else
    el::Configurations conf(log_config_file);
#endif
    el::Loggers::reconfigureAllLoggers(conf);

    el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
    el::Helpers::installPreRollOutCallback(RolloutHandler);
    el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog);
    return 0;
}


}   // server
}   // milvus
}   // zilliz
