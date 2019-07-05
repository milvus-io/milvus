////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "LogUtil.h"
#include "server/ServerConfig.h"

#include <easylogging++.h>
#include <ctype.h>

namespace zilliz {
namespace milvus {
namespace server {

int32_t InitLog(const std::string& log_config_file) {
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
    return 0;
}


}   // server
}   // milvus
}   // zilliz
