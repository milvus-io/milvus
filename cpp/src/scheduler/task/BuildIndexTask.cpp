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

#include "BuildIndexTask.h"
#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"
#include "scheduler/job/BuildIndexJob.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <string>
#include <thread>
#include <utility>

namespace zilliz {
namespace milvus {
namespace scheduler {

XBuildIndexTask::XBuildIndexTask(TableFileSchemaPtr file)
    : Task(TaskType::BuildIndexTask), file_(file) {
    if (file_) {
        to_index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, (EngineType) file_->engine_type_,
                                                (MetricType) file_->metric_type_, file_->nlist_);
    }
}

void
XBuildIndexTask::Load(zilliz::milvus::scheduler::LoadType type, uint8_t device_id) {
    TimeRecorder rc("");
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        if (type == LoadType::DISK2CPU) {
            stat = to_index_engine_->Load();
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            stat = to_index_engine_->CopyToGpu(device_id);
            type_str = "CPU2GPU";
        } else if (type == LoadType::GPU2CPU) {
            stat = to_index_engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load to_index file: " + std::string(ex.what());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }

    if (!stat.ok()) {
        Status s;
        if(stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str;
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        } else {
            error_msg = "Failed to load to_index file: " + type_str;
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        if (auto job = job_.lock()) {
            auto build_index_job = std::static_pointer_cast<scheduler::BuildIndexJob>(job);
            build_index_job->BuildIndexDone(file_->id_);
        }

        return;
    }

    size_t file_size = to_index_engine_->PhysicalSize();

    std::string info = "Load file id:" + std::to_string(file_->id_) + " file type:" +
        std::to_string(file_->file_type_) + " size:" + std::to_string(file_size) +
        " bytes from location: " + file_->location_ + " totally cost";
    double span = rc.ElapseFromBegin(info);

//    to_index_id_ = file_->id_;
//    to_index_type_ = file_->file_type_;
}

void
XBuildIndexTask::Execute() {
    if (to_index_engine_ == nullptr) {
        return;
    }

    TimeRecorder rc("DoBuildIndex file id:" + std::to_string(to_index_id_));

    if (auto job = job_.lock()) {
        auto build_job = std::static_pointer_cast<scheduler::BuildIndexJob>(job);
        std::string location = file_->location_;
        EngineType engine_type = (EngineType)file_->engine_type_;
        std::shared_ptr<engine::ExecutionEngine> index;

        try {
            index = to_index_engine_->BuildIndex(location, engine_type);
            if (index == nullptr) {
                table_file_.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
                //TODO: updatetablefile
            }
        } catch (std::exception &ex) {
            ENGINE_LOG_ERROR << "SearchTask encounter exception: " << ex.what();
        }

        build_job->BuildIndexDone(to_index_id_);
    }

    rc.ElapseFromBegin("totally cost");

    to_index_engine_ = nullptr;
}

}  // namespace scheduler
}  // namespace milvus
}  // namespace zilliz
