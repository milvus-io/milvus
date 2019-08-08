/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/BlockingQueue.h"
#include "status.grpc.pb.h"
#include "status.pb.h"

#include <map>
#include <vector>
#include <thread>

namespace zilliz {
namespace milvus {
namespace server {

class BaseTask {
protected:
    BaseTask(const std::string& task_group, bool async = false);
    virtual ~BaseTask();

public:
    ServerError
    Execute();

    ServerError
    WaitToFinish();

    std::string
    TaskGroup() const { return task_group_; }

    ServerError
    ErrorCode() const { return error_code_; }

    std::string
    ErrorMsg() const { return error_msg_; }

    bool
    IsAsync() const { return async_; }

protected:
    virtual ServerError
    OnExecute() = 0;

    ServerError
    SetError(ServerError error_code, const std::string& msg);

protected:
    mutable std::mutex finish_mtx_;
    std::condition_variable finish_cond_;

    std::string task_group_;
    bool async_;
    bool done_;
    ServerError error_code_;
    std::string error_msg_;
};

using BaseTaskPtr = std::shared_ptr<BaseTask>;
using TaskQueue = BlockingQueue<BaseTaskPtr>;
using TaskQueuePtr = std::shared_ptr<TaskQueue>;
using ThreadPtr = std::shared_ptr<std::thread>;

class RequestScheduler {
public:
    static RequestScheduler& GetInstance() {
        static RequestScheduler scheduler;
        return scheduler;
    }

    void Start();
    void Stop();

    ServerError
    ExecuteTask(const BaseTaskPtr& task_ptr);

    static void
    ExecTask(BaseTaskPtr& task_ptr, ::milvus::grpc::Status* grpc_status);

protected:
    RequestScheduler();
    virtual ~RequestScheduler();

    ServerError
    PutTaskToQueue(const BaseTaskPtr& task_ptr);

private:
    mutable std::mutex queue_mtx_;

    std::map<std::string, TaskQueuePtr> task_groups_;

    std::vector<ThreadPtr> execute_threads_;

    bool stopped_;
};


}
}
}
