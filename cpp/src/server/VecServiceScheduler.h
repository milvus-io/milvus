/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/BlockingQueue.h"

#include <map>
#include <vector>
#include <thread>

namespace zilliz {
namespace vecwise {
namespace server {

class BaseTask {
protected:
    BaseTask(const std::string& task_group);
    virtual ~BaseTask();

public:
    ServerError Execute();
    ServerError WaitToFinish();

    std::string TaskGroup() const { return task_group_; }

    ServerError ErrorCode() const { return error_code_; }
protected:
    virtual ServerError OnExecute() = 0;

protected:
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;

    std::string task_group_;
    bool done_;
    ServerError error_code_;
};

using BaseTaskPtr = std::shared_ptr<BaseTask>;
using TaskQueue = BlockingQueue<BaseTaskPtr>;
using TaskQueuePtr = std::shared_ptr<TaskQueue>;
using ThreadPtr = std::shared_ptr<std::thread>;

class VecServiceScheduler {
public:
    static VecServiceScheduler& GetInstance() {
        static VecServiceScheduler scheduler;
        return scheduler;
    }

    void Start();
    void Stop();

    ServerError ExecuteTask(const BaseTaskPtr& task_ptr);

protected:
    VecServiceScheduler();
    virtual ~VecServiceScheduler();

    ServerError PutTaskToQueue(const BaseTaskPtr& task_ptr);

private:
    mutable std::mutex queue_mtx_;

    std::map<std::string, TaskQueuePtr> task_groups_;

    std::vector<ThreadPtr> execute_threads_;

    bool stopped_;
};


}
}
}
