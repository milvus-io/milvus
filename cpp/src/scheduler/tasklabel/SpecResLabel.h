/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "TaskLabel.h"

#include <string>
#include <memory>


class Resource;

using ResourceWPtr = std::weak_ptr<Resource>;

namespace zilliz {
namespace milvus {
namespace engine {

class SpecResLabel : public TaskLabel {
public:
    SpecResLabel(const ResourceWPtr &resource)
        : TaskLabel(TaskLabelType::SPECIAL_RESOURCE), resource_(resource) {}

    inline ResourceWPtr &
    resource() const {
        return resource_;
    }

    inline std::string &
    resource_name() const {
        return resource_name_;
    }

private:
    ResourceWPtr resource_;
    std::string resource_name_;
}

using SpecResLabelPtr = std::make_shared<SpecResLabel>;

}
}
}

