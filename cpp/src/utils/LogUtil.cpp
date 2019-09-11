////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "LogUtil.h"
#include "server/ServerConfig.h"

#include <easylogging++.h>
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
void RolloutHandler(const char *filename, std::size_t size) {
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
    if ((position = s.find("global")) != std::string::npos) {
        s.append("." + std::to_string(++global_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if ((position = s.find("debug")) != std::string::npos) {
        s.append("." + std::to_string(++debug_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if ((position = s.find("warning")) != std::string::npos) {
        s.append("." + std::to_string(++warning_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if ((position = s.find("trace")) != std::string::npos) {
        s.append("." + std::to_string(++trace_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if ((position = s.find("error")) != std::string::npos) {
        s.append("." + std::to_string(++error_idx));
        ret = rename(m.c_str(), s.c_str());
    } else if ((position = s.find("fatal")) != std::string::npos) {
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
    return 0;
}


}   // server
}   // milvus
}   // zilliz
