/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#pragma once

#include "sys/types.h"
#include "sys/sysinfo.h"


namespace zilliz {
namespace vecwise {
namespace server {

class SystemInfo {
 private:

 public:
    static SystemInfo &
    GetInstance(){
        static SystemInfo instance;
        return instance;
    }

    long long GetPhysicalMemory();



};

}
}
}
