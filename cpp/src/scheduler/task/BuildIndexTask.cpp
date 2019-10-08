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

namespace milvus {
namespace scheduler {

XBuildIndexTask::XBuildIndexTask(TableFileSchemaPtr file, TaskLabelPtr label)
    : Task(TaskType::BuildIndexTask, std::move(label)), file_(file) {
    if (file_) {
        to_index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, (EngineType) file_->engine_type_,
                                                (MetricType) file_->metric_type_, file_->nlist_);
    }
}

void
XBuildIndexTask::Load(milvus::scheduler::LoadType type, uint8_t device_id) {
    TimeRecorder rc("");
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    if (auto job = job_.lock()) {
        auto build_index_job = std::static_pointer_cast<scheduler::BuildIndexJob>(job);
        auto options = build_index_job->options();
        try {
            if (type == LoadType::DISK2CPU) {
                stat = to_index_engine_->Load(options.insert_cache_immediately_);
                type_str = "DISK2CPU";
            } else if (type == LoadType::CPU2GPU) {
                stat = to_index_engine_->CopyToIndexFileToGpu(device_id);
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

        to_index_id_ = file_->id_;
        to_index_type_ = file_->file_type_;
    }
}

void
XBuildIndexTask::Execute() {
    if (to_index_engine_ == nullptr) {
        return;
    }

    TimeRecorder rc("DoBuildIndex file id:" + std::to_string(to_index_id_));

    if (auto job = job_.lock()) {
        auto build_index_job = std::static_pointer_cast<scheduler::BuildIndexJob>(job);
        std::string location = file_->location_;
        EngineType engine_type = (EngineType)file_->engine_type_;
        std::shared_ptr<engine::ExecutionEngine> index;

        // step 2: create table file
        engine::meta::TableFileSchema table_file;
        table_file.table_id_ = file_->table_id_;
        table_file.date_ = file_->date_;
        table_file.file_type_ =
            engine::meta::TableFileSchema::NEW_INDEX;  // for multi-db-path, distribute index file averagely to each path

        engine::meta::MetaPtr meta_ptr = build_index_job->meta();
        Status status = build_index_job->meta()->CreateTableFile(table_file);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to create table file: " << status.ToString();
            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = status;
            return;
        }

        // step 3: build index
        try {
            index = to_index_engine_->BuildIndex(table_file.location_, (EngineType) table_file.engine_type_);
            if (index == nullptr) {
                table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
                status = meta_ptr->UpdateTableFile(table_file);
                ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_
                                 << " to to_delete";

                return;
            }
        } catch (std::exception &ex) {
            std::string msg = "BuildIndex encounter exception: " + std::string(ex.what());
            ENGINE_LOG_ERROR << msg;

            table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
            status = meta_ptr->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

            std::cout << "ERROR: failed to build index, index file is too large or gpu memory is not enough"
                      << std::endl;

            build_index_job->GetStatus() = Status(DB_ERROR, msg);
            return;
        }

        // step 4: if table has been deleted, dont save index file
        bool has_table = false;
        meta_ptr->HasTable(file_->table_id_, has_table);
        if (!has_table) {
            meta_ptr->DeleteTableFiles(file_->table_id_);
            return;
        }

        // step 5: save index file
        try {
            index->Serialize();
        } catch (std::exception &ex) {
            // typical error: out of disk space or permition denied
            std::string msg = "Serialize index encounter exception: " + std::string(ex.what());
            ENGINE_LOG_ERROR << msg;

            table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
            status = meta_ptr->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

            std::cout << "ERROR: failed to persist index file: " << table_file.location_
                      << ", possible out of disk space" << std::endl;

            build_index_job->GetStatus() = Status(DB_ERROR, msg);
            return;
        }

        // step 6: update meta
        table_file.file_type_ = engine::meta::TableFileSchema::INDEX;
        table_file.file_size_ = index->PhysicalSize();
        table_file.row_count_ = index->Count();

        auto origin_file = *file_;
        origin_file.file_type_ = engine::meta::TableFileSchema::BACKUP;

        engine::meta::TableFilesSchema update_files = {table_file, origin_file};
        status = meta_ptr->UpdateTableFiles(update_files);
        if (status.ok()) {
            ENGINE_LOG_DEBUG << "New index file " << table_file.file_id_ << " of size " << index->PhysicalSize()
                             << " bytes"
                             << " from file " << origin_file.file_id_;

//            index->Cache();
        } else {
            // failed to update meta, mark the new file as to_delete, don't delete old file
            origin_file.file_type_ = engine::meta::TableFileSchema::TO_INDEX;
            status = meta_ptr->UpdateTableFile(origin_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << origin_file.file_id_ << " to to_index";

            table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
            status = meta_ptr->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to up  date file to index, mark file: " << table_file.file_id_ << " to to_delete";
        }

        build_index_job->BuildIndexDone(to_index_id_);
    }

    rc.ElapseFromBegin("totally cost");

    to_index_engine_ = nullptr;
}

}  // namespace scheduler
}  // namespace milvus
