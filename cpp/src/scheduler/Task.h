/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {

// dummy task
class Task {
public:
    Task(const std::string &name) {}

    void
    Execute() {}
};

using TaskPtr = std::shared_ptr<Task>;

}
}
}
