/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>

#include "Error.h"

namespace zilliz {
namespace vecwise {
namespace server {

class CommonUtil {
 public:
    static bool GetSystemMemInfo(unsigned long &totalMem, unsigned long &freeMem);
    static bool GetSystemAvailableThreads(unsigned int &threadCnt);

    static bool IsFileExist(const std::string &path);
    static bool IsDirectoryExit(const std::string &path);
    static ServerError CreateDirectory(const std::string &path);
    static ServerError DeleteDirectory(const std::string &path);
};

}
}
}

