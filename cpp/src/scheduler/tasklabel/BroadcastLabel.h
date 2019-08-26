/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "TaskLabel.h"

#include <memory>


namespace zilliz {
namespace milvus {
namespace engine {


class BroadcastLabel : public TaskLabel {
public:
    BroadcastLabel() : TaskLabel(TaskLabelType::BROADCAST) {}
};

using BroadcastLabelPtr = std::shared_ptr<BroadcastLabel>;

}
}
}
