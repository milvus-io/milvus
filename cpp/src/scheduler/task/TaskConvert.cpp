/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "TaskConvert.h"


namespace zilliz {
namespace milvus {
namespace engine {

TaskPtr
TaskConvert(const ScheduleTaskPtr &schedule_task) {
    switch (schedule_task->type()) {
        case ScheduleTaskType::kIndexLoad: {
            auto load_task = std::static_pointer_cast<IndexLoadTask>(schedule_task);
            auto task = std::make_shared<XSearchTask>(load_task->file_);
            task->search_contexts_ = load_task->search_contexts_;
            task->task_ = schedule_task;
            return task;
        }
        case ScheduleTaskType::kDelete: {
            // TODO: convert to delete task
            return nullptr;
        }
        default: {
            // TODO: unexpected !!!
            return nullptr;
        }
    }
}

}
}
}
