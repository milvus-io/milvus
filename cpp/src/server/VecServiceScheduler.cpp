/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "VecServiceScheduler.h"
#include "utils/Log.h"

namespace zilliz {
namespace vecwise {
namespace server {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
BaseTask::BaseTask(const std::string& task_group, bool async)
    : task_group_(task_group),
      async_(async),
      done_(false),
      error_code_(SERVER_SUCCESS) {

}

BaseTask::~BaseTask() {
    WaitToFinish();
}

ServerError BaseTask::Execute() {
    error_code_ = OnExecute();
    done_ = true;
    finish_cond_.notify_all();
    return error_code_;
}

ServerError BaseTask::WaitToFinish() {
    std::unique_lock <std::mutex> lock(finish_mtx_);
    finish_cond_.wait(lock, [this] { return done_; });

    return error_code_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
VecServiceScheduler::VecServiceScheduler()
: stopped_(false) {
    Start();
}

VecServiceScheduler::~VecServiceScheduler() {
    Stop();
}

void VecServiceScheduler::Start() {
    if(!stopped_) {
        return;
    }

    stopped_ = false;
}

void VecServiceScheduler::Stop() {
    if(stopped_) {
        return;
    }

    SERVER_LOG_INFO << "Scheduler gonna stop...";
    {
        std::lock_guard<std::mutex> lock(queue_mtx_);
        for(auto iter : task_groups_) {
            if(iter.second != nullptr) {
                iter.second->Put(nullptr);
            }
        }
    }

    for(auto iter : execute_threads_) {
        if(iter == nullptr)
            continue;

        iter->join();
    }
    stopped_ = true;
    SERVER_LOG_INFO << "Scheduler stopped";
}

ServerError VecServiceScheduler::ExecuteTask(const BaseTaskPtr& task_ptr) {
    if(task_ptr == nullptr) {
        return SERVER_NULL_POINTER;
    }

    ServerError err = PutTaskToQueue(task_ptr);
    if(err != SERVER_SUCCESS) {
        return err;
    }

    if(task_ptr->IsAsync()) {
        return SERVER_SUCCESS;//async execution, caller need to call WaitToFinish at somewhere
    }

    return task_ptr->WaitToFinish();//sync execution
}

namespace {
    void TakeTaskToExecute(TaskQueuePtr task_queue) {
        if(task_queue == nullptr) {
            return;
        }

        while(true) {
            BaseTaskPtr task = task_queue->Take();
            if (task == nullptr) {
                break;//stop the thread
            }

            try {
                ServerError err = task->Execute();
                if(err != SERVER_SUCCESS) {
                    SERVER_LOG_ERROR << "Task failed with code: " << err;
                }
            } catch (std::exception& ex) {
                SERVER_LOG_ERROR << "Task failed to execute: " << ex.what();
            }
        }
    }
}

ServerError VecServiceScheduler::PutTaskToQueue(const BaseTaskPtr& task_ptr) {
    std::lock_guard<std::mutex> lock(queue_mtx_);

    std::string group_name = task_ptr->TaskGroup();
    if(task_groups_.count(group_name) > 0) {
        task_groups_[group_name]->Put(task_ptr);
    } else {
        TaskQueuePtr queue = std::make_shared<TaskQueue>();
        queue->Put(task_ptr);
        task_groups_.insert(std::make_pair(group_name, queue));

        //start a thread
        ThreadPtr thread = std::make_shared<std::thread>(&TakeTaskToExecute, queue);
        execute_threads_.push_back(thread);
        SERVER_LOG_INFO << "Create new thread for task group: " << group_name;
    }

    return SERVER_SUCCESS;
}

}
}
}
