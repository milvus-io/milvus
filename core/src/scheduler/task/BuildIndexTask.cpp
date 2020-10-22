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

#include "scheduler/task/BuildIndexTask.h"

#include <fiu/fiu-local.h>
#include <memory>
#include <utility>

#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

BuildIndexTask::BuildIndexTask(const engine::snapshot::ScopedSnapshotT& snapshot, const engine::DBOptions& options,
                               engine::snapshot::ID_TYPE segment_id, const engine::TargetFields& target_fields,
                               TaskLabelPtr label)
    : Task(TaskType::BuildIndexTask, std::move(label)),
      snapshot_(snapshot),
      options_(options),
      segment_id_(segment_id),
      target_fields_(target_fields) {
    CreateExecEngine();
}

void
BuildIndexTask::CreateExecEngine() {
    if (execution_engine_ == nullptr) {
        execution_engine_ = engine::EngineFactory::Build(snapshot_, options_.meta_.path_, segment_id_);
    }
}

Status
BuildIndexTask::OnLoad(milvus::scheduler::LoadType type, uint8_t device_id) {
    TimeRecorder rc("BuildIndexTask::OnLoad");
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        if (type == LoadType::DISK2CPU) {
            engine::ExecutionEngineContext context;
            context.target_fields_ = target_fields_;
            stat = execution_engine_->Load(context);
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            // no need to copy flat to gpu,
            //            stat = execution_engine_->CopyToGpu(device_id);
            //            type_str = "CPU2GPU:" + std::to_string(device_id);
            gpu_device_id = device_id;
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
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

        auto build_job = static_cast<scheduler::BuildIndexJob*>(job_);
        build_job->MarkFailedSegment(segment_id_, stat);

        return s;
    }

    return Status::OK();
}

Status
BuildIndexTask::OnExecute() {
    TimeRecorderAuto rc("BuildIndexTask::OnExecute " + std::to_string(segment_id_));

    if (execution_engine_ == nullptr) {
        return Status(DB_ERROR, "execution engine is null");
    }

    Status status;
    try {
        status = execution_engine_->BuildIndex(gpu_device_id);
    } catch (std::exception& e) {
        status = Status(DB_ERROR, e.what());
    }

    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to build index: " << status.ToString();
        execution_engine_ = nullptr;

        auto build_job = static_cast<scheduler::BuildIndexJob*>(job_);
        build_job->MarkFailedSegment(segment_id_, status);

        return status;
    }

    return Status::OK();
}

std::string
BuildIndexTask::GetIndexType() {
    auto segment_visitor = engine::SegmentVisitor::Build(snapshot_, segment_id_);
    auto& field_visitors = segment_visitor->GetFieldVisitors();

    std::vector<std::string> index_types;

    for (auto& pair : field_visitors) {
        auto& field_visitor = pair.second;
        auto element_visitor = field_visitor->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor == nullptr) {
            continue;  // index undefined
        }
        auto element = element_visitor->GetElement();
        index_types.push_back(element->GetTypeName());
    }

    if (index_types.size() != 1) {
        throw std::runtime_error("index type size not correct." + std::to_string(index_types.size()));
    }

    return index_types[0];
}

}  // namespace scheduler
}  // namespace milvus
