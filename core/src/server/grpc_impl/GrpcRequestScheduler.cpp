// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "server/grpc_impl/GrpcRequestScheduler.h"
#include "utils/Log.h"

#include "grpc/gen-status/status.pb.h"

#include <utility>

namespace milvus {
namespace server {
namespace grpc {

namespace {
::milvus::grpc::ErrorCode
ErrorMap(ErrorCode code) {
    static const std::map<ErrorCode, ::milvus::grpc::ErrorCode> code_map = {
        {SERVER_UNEXPECTED_ERROR, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_UNSUPPORTED_ERROR, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_NULL_POINTER, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_INVALID_ARGUMENT, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_FILE_NOT_FOUND, ::milvus::grpc::ErrorCode::FILE_NOT_FOUND},
        {SERVER_NOT_IMPLEMENT, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_CANNOT_CREATE_FOLDER, ::milvus::grpc::ErrorCode::CANNOT_CREATE_FOLDER},
        {SERVER_CANNOT_CREATE_FILE, ::milvus::grpc::ErrorCode::CANNOT_CREATE_FILE},
        {SERVER_CANNOT_DELETE_FOLDER, ::milvus::grpc::ErrorCode::CANNOT_DELETE_FOLDER},
        {SERVER_CANNOT_DELETE_FILE, ::milvus::grpc::ErrorCode::CANNOT_DELETE_FILE},
        {SERVER_TABLE_NOT_EXIST, ::milvus::grpc::ErrorCode::TABLE_NOT_EXISTS},
        {SERVER_INVALID_TABLE_NAME, ::milvus::grpc::ErrorCode::ILLEGAL_TABLE_NAME},
        {SERVER_INVALID_TABLE_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},
        {SERVER_INVALID_TIME_RANGE, ::milvus::grpc::ErrorCode::ILLEGAL_RANGE},
        {SERVER_INVALID_VECTOR_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},

        {SERVER_INVALID_INDEX_TYPE, ::milvus::grpc::ErrorCode::ILLEGAL_INDEX_TYPE},
        {SERVER_INVALID_ROWRECORD, ::milvus::grpc::ErrorCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_ROWRECORD_ARRAY, ::milvus::grpc::ErrorCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_TOPK, ::milvus::grpc::ErrorCode::ILLEGAL_TOPK},
        {SERVER_INVALID_NPROBE, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_INVALID_INDEX_NLIST, ::milvus::grpc::ErrorCode::ILLEGAL_NLIST},
        {SERVER_INVALID_INDEX_METRIC_TYPE, ::milvus::grpc::ErrorCode::ILLEGAL_METRIC_TYPE},
        {SERVER_INVALID_INDEX_FILE_SIZE, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_ILLEGAL_VECTOR_ID, ::milvus::grpc::ErrorCode::ILLEGAL_VECTOR_ID},
        {SERVER_ILLEGAL_SEARCH_RESULT, ::milvus::grpc::ErrorCode::ILLEGAL_SEARCH_RESULT},
        {SERVER_CACHE_FULL, ::milvus::grpc::ErrorCode::CACHE_FAILED},
        {DB_META_TRANSACTION_FAILED, ::milvus::grpc::ErrorCode::META_FAILED},
        {SERVER_BUILD_INDEX_ERROR, ::milvus::grpc::ErrorCode::BUILD_INDEX_ERROR},
        {SERVER_OUT_OF_MEMORY, ::milvus::grpc::ErrorCode::OUT_OF_MEMORY},
    };

    if (code_map.find(code) != code_map.end()) {
        return code_map.at(code);
    } else {
        return ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR;
    }
}
}  // namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
GrpcRequestScheduler::GrpcRequestScheduler() : stopped_(false) {
    Start();
}

GrpcRequestScheduler::~GrpcRequestScheduler() {
    Stop();
}

void
GrpcRequestScheduler::ExecRequest(BaseRequestPtr& request_ptr, ::milvus::grpc::Status* grpc_status) {
    if (request_ptr == nullptr) {
        return;
    }

    GrpcRequestScheduler& scheduler = GrpcRequestScheduler::GetInstance();
    scheduler.ExecuteRequest(request_ptr);

    if (!request_ptr->IsAsync()) {
        request_ptr->WaitToFinish();
        const Status& status = request_ptr->status();
        if (!status.ok()) {
            grpc_status->set_reason(status.message());
            grpc_status->set_error_code(ErrorMap(status.code()));
        }
    }
}

void
GrpcRequestScheduler::Start() {
    if (!stopped_) {
        return;
    }

    stopped_ = false;
}

void
GrpcRequestScheduler::Stop() {
    if (stopped_) {
        return;
    }

    SERVER_LOG_INFO << "Scheduler gonna stop...";
    {
        std::lock_guard<std::mutex> lock(queue_mtx_);
        for (auto iter : request_groups_) {
            if (iter.second != nullptr) {
                iter.second->Put(nullptr);
            }
        }
    }

    for (auto iter : execute_threads_) {
        if (iter == nullptr)
            continue;

        iter->join();
    }
    stopped_ = true;
    SERVER_LOG_INFO << "Scheduler stopped";
}

Status
GrpcRequestScheduler::ExecuteRequest(const BaseRequestPtr& request_ptr) {
    if (request_ptr == nullptr) {
        return Status::OK();
    }

    auto status = PutToQueue(request_ptr);
    if (!status.ok()) {
        SERVER_LOG_ERROR << "Put request to queue failed with code: " << status.ToString();
        return status;
    }

    if (request_ptr->IsAsync()) {
        return Status::OK();  // async execution, caller need to call WaitToFinish at somewhere
    }

    return request_ptr->WaitToFinish();  // sync execution
}

void
GrpcRequestScheduler::TakeToExecute(RequestQueuePtr request_queue) {
    if (request_queue == nullptr) {
        return;
    }

    while (true) {
        BaseRequestPtr request = request_queue->Take();
        if (request == nullptr) {
            SERVER_LOG_ERROR << "Take null from request queue, stop thread";
            break;  // stop the thread
        }

        try {
            auto status = request->Execute();
            if (!status.ok()) {
                SERVER_LOG_ERROR << "Request failed with code: " << status.ToString();
            }
        } catch (std::exception& ex) {
            SERVER_LOG_ERROR << "Request failed to execute: " << ex.what();
        }
    }
}

Status
GrpcRequestScheduler::PutToQueue(const BaseRequestPtr& request_ptr) {
    std::lock_guard<std::mutex> lock(queue_mtx_);

    std::string group_name = request_ptr->RequestGroup();
    if (request_groups_.count(group_name) > 0) {
        request_groups_[group_name]->Put(request_ptr);
    } else {
        RequestQueuePtr queue = std::make_shared<RequestQueue>();
        queue->Put(request_ptr);
        request_groups_.insert(std::make_pair(group_name, queue));

        // start a thread
        ThreadPtr thread = std::make_shared<std::thread>(&GrpcRequestScheduler::TakeToExecute, this, queue);
        execute_threads_.push_back(thread);
        SERVER_LOG_INFO << "Create new thread for request group: " << group_name;
    }

    return Status::OK();
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
