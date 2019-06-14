/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "RequestScheduler.h"
#include "utils/Log.h"

#include "milvus_types.h"
#include "milvus_constants.h"

namespace zilliz {
namespace milvus {
namespace server {
    
using namespace ::milvus;

namespace {
    const std::map<ServerError, thrift::ErrorCode::type> &ErrorMap() {
        static const std::map<ServerError, thrift::ErrorCode::type> code_map = {
                {SERVER_UNEXPECTED_ERROR,         thrift::ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_NULL_POINTER,             thrift::ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_INVALID_ARGUMENT,         thrift::ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_FILE_NOT_FOUND,           thrift::ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_NOT_IMPLEMENT,            thrift::ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_BLOCKING_QUEUE_EMPTY,     thrift::ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_TABLE_NOT_EXIST,          thrift::ErrorCode::TABLE_NOT_EXISTS},
                {SERVER_INVALID_TIME_RANGE,       thrift::ErrorCode::ILLEGAL_RANGE},
                {SERVER_INVALID_VECTOR_DIMENSION, thrift::ErrorCode::ILLEGAL_DIMENSION},
        };

        return code_map;
    }

    const std::map<ServerError, std::string> &ErrorMessage() {
        static const std::map<ServerError, std::string> msg_map = {
                {SERVER_UNEXPECTED_ERROR,         "unexpected error occurs"},
                {SERVER_NULL_POINTER,             "null pointer error"},
                {SERVER_INVALID_ARGUMENT,         "invalid argument"},
                {SERVER_FILE_NOT_FOUND,           "file not found"},
                {SERVER_NOT_IMPLEMENT,            "not implemented"},
                {SERVER_BLOCKING_QUEUE_EMPTY,     "queue empty"},
                {SERVER_TABLE_NOT_EXIST,          "table not exist"},
                {SERVER_INVALID_TIME_RANGE,       "invalid time range"},
                {SERVER_INVALID_VECTOR_DIMENSION, "invalid vector dimension"},
        };

        return msg_map;
    }
}

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
RequestScheduler::RequestScheduler()
: stopped_(false) {
    Start();
}

RequestScheduler::~RequestScheduler() {
    Stop();
}

void RequestScheduler::ExecTask(BaseTaskPtr& task_ptr) {
    if(task_ptr == nullptr) {
        return;
    }

    RequestScheduler& scheduler = RequestScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    if(!task_ptr->IsAsync()) {
        task_ptr->WaitToFinish();
        ServerError err = task_ptr->ErrorCode();
        if (err != SERVER_SUCCESS) {
            thrift::Exception ex;
            ex.__set_code(ErrorMap().at(err));
            std::string msg = task_ptr->ErrorMsg();
            if(msg.empty()){
                msg = ErrorMessage().at(err);
            }
            ex.__set_reason(msg);
            throw ex;
        }
    }
}

void RequestScheduler::Start() {
    if(!stopped_) {
        return;
    }

    stopped_ = false;
}

void RequestScheduler::Stop() {
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

ServerError RequestScheduler::ExecuteTask(const BaseTaskPtr& task_ptr) {
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

ServerError RequestScheduler::PutTaskToQueue(const BaseTaskPtr& task_ptr) {
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
