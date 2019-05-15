/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "thrift/gen-cpp/VecService.h"

#include <memory>

namespace zilliz {
namespace vecwise {
namespace client {

using VecServiceClientPtr = std::shared_ptr<megasearch::VecServiceClient>;

class ClientSession {
public:
    ClientSession(const std::string& address, int32_t port, const std::string& protocol);
    ~ClientSession();

    VecServiceClientPtr interface();

    VecServiceClientPtr client_;
};

}
}
}
