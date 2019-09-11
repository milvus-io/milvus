/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "SearchTask.h"
#include "metrics/Metrics.h"
#include "db/Log.h"
#include "utils/TimeRecorder.h"

#include <thread>

namespace zilliz {
namespace milvus {
namespace engine {

SearchTask::SearchTask()
: IScheduleTask(ScheduleTaskType::kSearch) {
}

std::shared_ptr<IScheduleTask> SearchTask::Execute() {
   return nullptr;
}

}
}
}
