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
#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"
#include "scheduler/job/BuildIndexJob.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace milvus {
namespace scheduler {

XBuildIndexTask::XBuildIndexTask(TableFileSchemaPtr file, TaskLabelPtr label)
    : Task(TaskType::BuildIndexTask, std::move(label)), file_(file) {
    if (file_) {
        to_index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, (EngineType)file_->engine_type_,
                                                (MetricType)file_->metric_type_, file_->nlist_);
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
                type_str = "CPU2GPU:" + std::to_string(device_id);
            } else {
                error_msg = "Wrong load type";
                stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
            }
            fiu_do_on("XBuildIndexTask.Load.throw_std_exception", throw std::exception());
        } catch (std::exception& ex) {
            // typical error: out of disk space or permition denied
            error_msg = "Failed to load to_index file: " + std::string(ex.what());
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
        fiu_do_on("XBuildIndexTask.Load.out_of_memory", stat = Status(SERVER_UNEXPECTED_ERROR, "out of memory"));
        if (!stat.ok()) {
            Status s;
            if (stat.ToString().find("out of memory") != std::string::npos) {
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

        std::string info = "Build index task load file id:" + std::to_string(file_->id_) + " " + type_str +
                           " file type:" + std::to_string(file_->file_type_) + " size:" + std::to_string(file_size) +
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
        table_file.file_type_ = engine::meta::TableFileSchema::NEW_INDEX;

        engine::meta::MetaPtr meta_ptr = build_index_job->meta();
        Status status = meta_ptr->CreateTableFile(table_file);
        fiu_do_on("XBuildIndexTask.Execute.create_table_success", status = Status::OK());
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Failed to create table file: " << status.ToString();
            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = status;
            to_index_engine_ = nullptr;
            return;
        }

        // step 3: build index
        try {
            ENGINE_LOG_DEBUG << "Begin build index for file:" + table_file.location_;
            index = to_index_engine_->BuildIndex(table_file.location_, (EngineType)table_file.engine_type_);
            fiu_do_on("XBuildIndexTask.Execute.build_index_fail", index = nullptr);
            if (index == nullptr) {
                throw Exception(DB_ERROR, "index NULL");
            }
        } catch (std::exception& ex) {
            std::string msg = "Build index exception: " + std::string(ex.what());
            ENGINE_LOG_ERROR << msg;

            table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
            status = meta_ptr->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Build index fail, mark file: " << table_file.file_id_ << " to to_delete";

            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = Status(DB_ERROR, msg);
            to_index_engine_ = nullptr;
            return;
        }

        // step 4: if table has been deleted, dont save index file
        bool has_table = false;
        meta_ptr->HasTable(file_->table_id_, has_table);
        fiu_do_on("XBuildIndexTask.Execute.has_table", has_table = true);

        if (!has_table) {
            meta_ptr->DeleteTableFiles(file_->table_id_);

            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = Status(DB_ERROR, "Table has been deleted, discard index file.");
            to_index_engine_ = nullptr;
            return;
        }

        // step 5: save index file
        try {
            fiu_do_on("XBuildIndexTask.Execute.throw_std_exception", throw std::exception());
            status = index->Serialize();
            if (!status.ok()) {
                ENGINE_LOG_ERROR << status.message();
            }
        } catch (std::exception& ex) {
            std::string msg = "Serialize index encounter exception: " + std::string(ex.what());
            ENGINE_LOG_ERROR << msg;
            status = Status(DB_ERROR, msg);
        }

        fiu_do_on("XBuildIndexTask.Execute.save_index_file_success", status = Status::OK());
        if (!status.ok()) {
            // if failed to serialize index file to disk
            // typical error: out of disk space, out of memory or permition denied
            table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
            status = meta_ptr->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

            ENGINE_LOG_ERROR << "Failed to persist index file: " << table_file.location_
                             << ", possible out of disk space or memory";

            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = status;
            to_index_engine_ = nullptr;
            return;
        }

        // step 6: update meta
        table_file.file_type_ = engine::meta::TableFileSchema::INDEX;
        table_file.file_size_ = index->PhysicalSize();
        table_file.row_count_ = index->Count();

        auto origin_file = *file_;
        origin_file.file_type_ = engine::meta::TableFileSchema::BACKUP;

        engine::meta::TableFilesSchema update_files = {table_file, origin_file};

        if (status.ok()) {  // makesure index file is sucessfully serialized to disk
            status = meta_ptr->UpdateTableFiles(update_files);
        }

        fiu_do_on("XBuildIndexTask.Execute.update_table_file_fail", status = Status(SERVER_UNEXPECTED_ERROR, ""));
        if (status.ok()) {
            ENGINE_LOG_DEBUG << "New index file " << table_file.file_id_ << " of size " << index->PhysicalSize()
                             << " bytes"
                             << " from file " << origin_file.file_id_;
            if (build_index_job->options().insert_cache_immediately_) {
                index->Cache();
            }
        } else {
            // failed to update meta, mark the new file as to_delete, don't delete old file
            origin_file.file_type_ = engine::meta::TableFileSchema::TO_INDEX;
            status = meta_ptr->UpdateTableFile(origin_file);
            ENGINE_LOG_DEBUG << "Failed to update file to index, mark file: " << origin_file.file_id_ << " to to_index";

            table_file.file_type_ = engine::meta::TableFileSchema::TO_DELETE;
            status = meta_ptr->UpdateTableFile(table_file);
            ENGINE_LOG_DEBUG << "Failed to up  date file to index, mark file: " << table_file.file_id_
                             << " to to_delete";
        }

        build_index_job->BuildIndexDone(to_index_id_);
    }

    rc.ElapseFromBegin("totally cost");

    to_index_engine_ = nullptr;
}

}  // namespace scheduler
}  // namespace milvus
