/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "RequestScheduler.h"
#include "utils/Log.h"

#include "../grpc/gen-status/status.pb.h"

namespace zilliz {
namespace milvus {
namespace server {

using namespace ::milvus;

namespace {
    const std::map<ServerError, ::milvus::ErrorCode > &ErrorMap() {
        static const std::map<ServerError, ::milvus::ErrorCode> code_map = {
                {SERVER_UNEXPECTED_ERROR,         ErrorCode::UNEXPECTED_ERROR},
                {SERVER_UNSUPPORTED_ERROR,        ErrorCode::UNEXPECTED_ERROR},
                {SERVER_NULL_POINTER,             ErrorCode::UNEXPECTED_ERROR},
                {SERVER_INVALID_ARGUMENT,         ErrorCode::ILLEGAL_ARGUMENT},
                {SERVER_FILE_NOT_FOUND,           ErrorCode::FILE_NOT_FOUND},
                {SERVER_NOT_IMPLEMENT,            ErrorCode::UNEXPECTED_ERROR},
                {SERVER_BLOCKING_QUEUE_EMPTY,     ErrorCode::UNEXPECTED_ERROR},
                {SERVER_CANNOT_CREATE_FOLDER,     ErrorCode::CANNOT_CREATE_FOLDER},
                {SERVER_CANNOT_CREATE_FILE,       ErrorCode::CANNOT_CREATE_FILE},
                {SERVER_CANNOT_DELETE_FOLDER,     ErrorCode::CANNOT_DELETE_FOLDER},
                {SERVER_CANNOT_DELETE_FILE,       ErrorCode::CANNOT_DELETE_FILE},
                {SERVER_TABLE_NOT_EXIST,          ErrorCode::TABLE_NOT_EXISTS},
                {SERVER_INVALID_TABLE_NAME,       ErrorCode::ILLEGAL_TABLE_NAME},
                {SERVER_INVALID_TABLE_DIMENSION,  ErrorCode::ILLEGAL_DIMENSION},
                {SERVER_INVALID_TIME_RANGE,       ErrorCode::ILLEGAL_RANGE},
                {SERVER_INVALID_VECTOR_DIMENSION, ErrorCode::ILLEGAL_DIMENSION},

                {SERVER_INVALID_INDEX_TYPE,       ErrorCode::ILLEGAL_INDEX_TYPE},
                {SERVER_INVALID_ROWRECORD,        ErrorCode::ILLEGAL_ROWRECORD},
                {SERVER_INVALID_ROWRECORD_ARRAY,  ErrorCode::ILLEGAL_ROWRECORD},
                {SERVER_INVALID_TOPK,             ErrorCode::ILLEGAL_TOPK},
                {SERVER_ILLEGAL_VECTOR_ID,        ErrorCode::ILLEGAL_VECTOR_ID},
                {SERVER_ILLEGAL_SEARCH_RESULT,    ErrorCode::ILLEGAL_SEARCH_RESULT},
                {SERVER_CACHE_ERROR,              ErrorCode::CACHE_FAILED},
                {DB_META_TRANSACTION_FAILED,      ErrorCode::META_FAILED},
                {SERVER_BUILD_INDEX_ERROR,        ErrorCode::BUILD_INDEX_ERROR},
        };

        return code_map;
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

ServerError BaseTask::SetError(ServerError error_code, const std::string& error_msg) {
    error_code_ = error_code;
    error_msg_ = error_msg;

    SERVER_LOG_ERROR << error_msg_;
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

void RequestScheduler::ExecTask(BaseTaskPtr& task_ptr, ::milvus::Status *grpc_status) {
    if(task_ptr == nullptr) {
        return;
    }

    RequestScheduler& scheduler = RequestScheduler::GetInstance();
    scheduler.ExecuteTask(task_ptr);

    if(!task_ptr->IsAsync()) {
        task_ptr->WaitToFinish();
        ServerError err = task_ptr->ErrorCode();
        if (err != SERVER_SUCCESS) {
            //TODO:Handle exception
            grpc_status->set_reason(task_ptr->ErrorMsg());
            grpc_status->set_error_code(::milvus::ErrorCode((int) err));
//            grpc_status.set_error_code(err);
//            Exception ex;
//            ex.__set_code(ErrorMap().at(err));
//            std::string msg = task_ptr->ErrorMsg();
//            if(msg.empty()){
//                msg = "Error message not set";
//            }
//            ex.__set_reason(msg);
//            throw ex;
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
