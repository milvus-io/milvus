/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>

namespace zilliz {
namespace vecwise {
namespace client {

class ClientApp {
public:
    void Run(const std::string& config_file);
};

}
}
}
