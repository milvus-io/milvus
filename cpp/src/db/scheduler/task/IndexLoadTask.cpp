/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "IndexLoadTask.h"
#include "SearchTask.h"
#include "db/Log.h"
#include "db/engine/EngineFactory.h"
#include "utils/TimeRecorder.h"
#include "metrics/Metrics.h"

namespace zilliz {
namespace milvus {
namespace engine {

IndexLoadTask::IndexLoadTask()
    : IScheduleTask(ScheduleTaskType::kIndexLoad) {

}

std::shared_ptr<IScheduleTask> IndexLoadTask::Execute() {
    return nullptr;
}

}
}
}