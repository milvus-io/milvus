// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "scheduler/task/SearchTask.h"

#include <fiu-local.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "db/Utils.h"
#include "db/engine/ExecutionEngineImpl.h"
#include "scheduler/SchedInst.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

SearchTask::SearchTask(const server::ContextPtr& context, const engine::DBOptions& options,
                       const query::QueryPtr& query_ptr, engine::snapshot::ID_TYPE segment_id, TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)),
      context_(context),
      options_(options),
      query_ptr_(query_ptr),
      segment_id_(segment_id) {
    CreateExecEngine();
}

void
SearchTask::CreateExecEngine() {
    if (execution_engine_ == nullptr && query_ptr_ != nullptr) {
        execution_engine_ = engine::EngineFactory::Build(options_.meta_.path_, query_ptr_->collection_id, segment_id_);
    }
}

Status
SearchTask::OnLoad(LoadType type, uint8_t device_id) {
    TimeRecorder rc(LogOut("[%s][%ld]", "search", segment_id_));
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        fiu_do_on("XSearchTask.Load.throw_std_exception", throw std::exception());
        if (type == LoadType::DISK2CPU) {
            engine::ExecutionEngineContext context;
            context.query_ptr_ = query_ptr_;
            stat = execution_engine_->Load(context);
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            stat = execution_engine_->CopyToGpu(device_id);
            type_str = "CPU2GPU" + std::to_string(device_id);
        } else if (type == LoadType::GPU2CPU) {
            // stat = engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load index file: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Encounter exception: %s", "search", 0, error_msg.c_str());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }

    if (!stat.ok()) {
        Status s;
        if (stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str + " : " + stat.message();
            s = Status(SERVER_OUT_OF_MEMORY, error_msg);
        } else {
            error_msg = "Failed to load index file: " + type_str + " : " + stat.message();
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        return s;
    }

    std::string info = "Search task load segment id: " + std::to_string(segment_id_) + " " + type_str + " totally cost";
    rc.ElapseFromBegin(info);

    return Status::OK();
}

Status
SearchTask::OnExecute() {
    milvus::server::ContextFollower tracer(context_, "XSearchTask::Execute " + std::to_string(segment_id_));
    TimeRecorder rc(LogOut("[%s][%ld] DoSearch file id:%ld", "search", 0, segment_id_));

    if (execution_engine_ == nullptr) {
        return Status(DB_ERROR, "execution engine is null");
    }

    try {
        /* step 2: search */
        engine::ExecutionEngineContext context;
        context.query_ptr_ = query_ptr_;
        context.query_result_ = std::make_shared<engine::QueryResult>();
        auto status = execution_engine_->Search(context);

        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("search done");

        /* step 3: pick up topk result */
        // auto spec_k = file_->row_count_ < topk ? file_->row_count_ : topk;
        // if (spec_k == 0) {
        //     LOG_ENGINE_WARNING_ << LogOut("[%s][%ld] Searching in an empty file. file location = %s",
        //     "search", 0,
        //                                   file_->location_.c_str());
        // } else {
        //     std::unique_lock<std::mutex> lock(search_job->mutex());
        //   XSearchTask::MergeTopkToResultSet(result, spec_k, nq, topk, ascending_, search_job->GetQueryResult());
        // }

        rc.RecordSection("reduce topk done");
    } catch (std::exception& ex) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] SearchTask encounter exception: %s", "search", 0, ex.what());
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");
    return Status::OK();
}

int64_t
SearchTask::nq() {
    return 0;
}

}  // namespace scheduler
}  // namespace milvus
