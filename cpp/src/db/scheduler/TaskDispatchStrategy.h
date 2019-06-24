/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "context/IScheduleContext.h"
#include "task/IScheduleTask.h"

#include <list>

namespace zilliz {
namespace milvus {
namespace engine {

class TaskDispatchStrategy {
public:
    static bool Schedule(const ScheduleContextPtr &context_ptr, std::list<ScheduleTaskPtr>& task_list);
};

}
}
}
