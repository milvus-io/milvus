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

#include <fiu-local.h>
#include <memory>
#include <utility>

#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "scheduler/job/SSBuildIndexJob.h"
#include "scheduler/task/SSBuildIndexTask.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

SSBuildIndexTask::SSBuildIndexTask(const engine::DBOptions& options, const std::string& collection_name,
                                   engine::snapshot::ID_TYPE segment_id, TaskLabelPtr label)
    : Task(TaskType::BuildIndexTask, std::move(label)),
      options_(options),
      collection_name_(collection_name),
      segment_id_(segment_id) {
    CreateExecEngine();
}

void
SSBuildIndexTask::CreateExecEngine() {
    if (execution_engine_ == nullptr) {
        execution_engine_ = engine::EngineFactory::Build(options_.meta_.path_, collection_name_, segment_id_);
    }
}

void
SSBuildIndexTask::Load(milvus::scheduler::LoadType type, uint8_t device_id) {
    TimeRecorder rc("SSBuildIndexTask::Load");
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    if (auto job = job_.lock()) {
        try {
            if (type == LoadType::DISK2CPU) {
                engine::ExecutionEngineContext context;
                stat = execution_engine_->Load(context);
                type_str = "DISK2CPU";
            } else if (type == LoadType::CPU2GPU) {
                stat = execution_engine_->CopyToGpu(device_id);
                type_str = "CPU2GPU:" + std::to_string(device_id);
            } else {
                error_msg = "Wrong load type";
                stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
            }
            fiu_do_on("XSSBuildIndexTask.Load.throw_std_exception", throw std::exception());
        } catch (std::exception& ex) {
            // typical error: out of disk space or permission denied
            error_msg = "Failed to load to_index file: " + std::string(ex.what());
            LOG_ENGINE_ERROR_ << error_msg;
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        if (!stat.ok()) {
            Status s;
            if (stat.ToString().find("out of memory") != std::string::npos) {
                error_msg = "out of memory: " + type_str;
                s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
            } else {
                error_msg = "Failed to load to_index file: " + type_str;
                s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
            }

            LOG_ENGINE_ERROR_ << s.message();

            auto build_index_job = std::static_pointer_cast<scheduler::SSBuildIndexJob>(job);
            build_index_job->status() = s;
            build_index_job->BuildIndexDone(segment_id_);
        }
    }
}

void
SSBuildIndexTask::Execute() {
    TimeRecorderAuto rc("XSSBuildIndexTask::Execute " + std::to_string(segment_id_));

    if (auto job = job_.lock()) {
        auto build_index_job = std::static_pointer_cast<scheduler::SSBuildIndexJob>(job);
        if (execution_engine_ == nullptr) {
            build_index_job->BuildIndexDone(segment_id_);
            build_index_job->status() = Status(DB_ERROR, "execution engine is null");
            return;
        }

        auto status = execution_engine_->BuildIndex();
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to create collection file: " << status.ToString();
            build_index_job->BuildIndexDone(segment_id_);
            build_index_job->status() = status;
            execution_engine_ = nullptr;
            return;
        }

        build_index_job->BuildIndexDone(segment_id_);
    }
}

}  // namespace scheduler
}  // namespace milvus
