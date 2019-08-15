/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#pragma once

#include <memory>

namespace zilliz {
namespace milvus {
namespace engine {

class RegisterHandler {
 public:
    virtual void Exec() = 0;
};

using RegisterHandlerPtr = std::shared_ptr<RegisterHandler>;

}
}
}