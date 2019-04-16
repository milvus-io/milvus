/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"

#include <cstdint>
#include <string>

namespace zilliz {
namespace vecwise {
namespace server {

class VecServiceWrapper {
public:
    static void StartService();
    static void StopService();
};


}
}
}
