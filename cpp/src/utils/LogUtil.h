/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <sstream>

namespace zilliz {
namespace vecwise {
namespace server {

int32_t InitLog(const std::string& log_config_file);

inline std::string GetFileName(std::string filename) {
    int pos = filename.find_last_of('/');
    return filename.substr(pos + 1);
}

#define SHOW_LOCATION
#ifdef SHOW_LOCATION
#define LOCATION_INFO "[" << zilliz::sql::server::GetFileName(__FILE__) << ":" << __LINE__ << "] "
#else
#define LOCATION_INFO ""
#endif

}
}
}
