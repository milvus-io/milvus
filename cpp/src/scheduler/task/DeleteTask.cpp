/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DeleteTask.h"


namespace zilliz {
namespace milvus {
namespace engine {

XDeleteTask::XDeleteTask(DeleteContextPtr &delete_context)
    : delete_context_ptr_(delete_context) {}

void
XDeleteTask::Load(LoadType type, uint8_t device_id) {

}

void
XDeleteTask::Execute() {
    delete_context_ptr_->ResourceDone();
}

TaskPtr
XDeleteTask::Clone() {
    auto task = std::make_shared<XDeleteTask>(delete_context_ptr_);
    return task;
}

}
}
}
