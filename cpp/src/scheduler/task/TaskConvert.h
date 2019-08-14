/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "src/db/scheduler/task/IndexLoadTask.h"
#include "Task.h"
#include "SearchTask.h"

namespace zilliz {
namespace milvus {
namespace engine {


TaskPtr
TaskConvert(const ScheduleTaskPtr &schedule_task);

}
}
}
