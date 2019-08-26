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

enum class TaskLabelType {
    DEFAULT, // means can be executed in any resource
    SPECIAL_RESOURCE, // means must executing in special resource
    BROADCAST, // means all enable-executor resource must execute task
};

class TaskLabel {
public:
    inline TaskLabelType
    Type() const {
        return type_;
    }

protected:
    TaskLabel(TaskLabelType type) : type_(type) {}

private:
    TaskLabelType type_;
};

using TaskLabelPtr = std::shared_ptr<TaskLabel>;

}
}
}

