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
#include "db/engine/SSExecutionEngineImpl.h"
#include "scheduler/job/SSBuildIndexJob.h"
#include "scheduler/task/SSBuildIndexTask.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

XSSBuildIndexTask::XSSBuildIndexTask(const std::string& dir_root, const engine::SegmentVisitorPtr& visitor,
                                     TaskLabelPtr label)
    : Task(TaskType::BuildIndexTask, std::move(label)), visitor_(visitor) {
    engine_ = std::make_shared<engine::SSExecutionEngineImpl>(dir_root, visitor);
}

void
XSSBuildIndexTask::Load(milvus::scheduler::LoadType type, uint8_t device_id) {
    TimeRecorder rc("XSSBuildIndexTask::Load");
    auto seg_id = visitor_->GetSegment()->GetID();
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    if (auto job = job_.lock()) {
        auto build_index_job = std::static_pointer_cast<scheduler::SSBuildIndexJob>(job);
        // auto options = build_index_job->options();
        try {
            if (type == LoadType::DISK2CPU) {
                stat = engine_->Load(nullptr);
                type_str = "DISK2CPU";
            } else if (type == LoadType::CPU2GPU) {
                stat = engine_->CopyToGpu(device_id);
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
        fiu_do_on("XSSBuildIndexTask.Load.out_of_memory", stat = Status(SERVER_UNEXPECTED_ERROR, "out of memory"));
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

            if (auto job = job_.lock()) {
                auto build_index_job = std::static_pointer_cast<scheduler::SSBuildIndexJob>(job);
                build_index_job->status() = s;
                build_index_job->BuildIndexDone(seg_id);
            }

            return;
        }

        std::string info =
            "Build index task load segment id:" + std::to_string(seg_id) + " " + type_str + " totally cost";
        rc.ElapseFromBegin(info);
    }
}

void
XSSBuildIndexTask::Execute() {
    auto seg_id = visitor_->GetSegment()->GetID();
    TimeRecorderAuto rc("XSSBuildIndexTask::Execute " + std::to_string(seg_id));

    if (auto job = job_.lock()) {
        auto build_index_job = std::static_pointer_cast<scheduler::SSBuildIndexJob>(job);
        if (engine_ == nullptr) {
            build_index_job->BuildIndexDone(seg_id);
            build_index_job->status() = Status(DB_ERROR, "source index is null");
            return;
        }

        // SS TODO

        build_index_job->BuildIndexDone(seg_id);
    }

    engine_ = nullptr;
}

}  // namespace scheduler
}  // namespace milvus
