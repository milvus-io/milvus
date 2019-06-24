/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "context/IScheduleContext.h"
#include "task/IScheduleTask.h"
#include "TaskDispatchQueue.h"
#include "utils/BlockingQueue.h"

namespace zilliz {
namespace milvus {
namespace engine {

class TaskScheduler {
private:
    TaskScheduler();
    virtual ~TaskScheduler();

public:
    static TaskScheduler& GetInstance();

    bool Schedule(ScheduleContextPtr context);

private:
    bool Start();
    bool Stop();

    bool TaskDispatchWorker();
    bool TaskWorker();

private:
    std::shared_ptr<std::thread> task_dispatch_thread_;
    std::shared_ptr<std::thread> task_thread_;

    TaskDispatchQueue task_dispatch_queue_;

    using TaskQueue = server::BlockingQueue<ScheduleTaskPtr>;
    TaskQueue task_queue_;

    bool stopped_ = true;
};


}
}
}
