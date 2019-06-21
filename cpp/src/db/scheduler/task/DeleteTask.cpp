/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DeleteTask.h"

namespace zilliz {
namespace milvus {
namespace engine {

DeleteTask::DeleteTask(const DeleteContextPtr& context)
    : IScheduleTask(ScheduleTaskType::kDelete),
      context_(context) {

}

std::shared_ptr<IScheduleTask> DeleteTask::Execute() {

    if(context_ != nullptr && context_->meta() != nullptr) {
        context_->meta()->DeleteTableFiles(context_->table_id());
    }

    return nullptr;
}

}
}
}