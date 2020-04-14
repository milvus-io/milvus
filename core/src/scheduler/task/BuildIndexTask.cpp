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

#include <fiu-local.h>

#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"
#include "scheduler/job/BuildIndexJob.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace scheduler {

XBuildIndexTask::XBuildIndexTask(SegmentSchemaPtr file, TaskLabelPtr label)
    : Task(TaskType::BuildIndexTask, std::move(label)), file_(file) {
    if (file_) {
        EngineType engine_type;
        if (file->file_type_ == SegmentSchema::FILE_TYPE::RAW ||
            file->file_type_ == SegmentSchema::FILE_TYPE::TO_INDEX ||
            file->file_type_ == SegmentSchema::FILE_TYPE::BACKUP) {
            engine_type = engine::utils::IsBinaryMetricType(file->metric_type_) ? EngineType::FAISS_BIN_IDMAP
                                                                                : EngineType::FAISS_IDMAP;
        } else {
            engine_type = (EngineType)file->engine_type_;
        }

        auto json = milvus::json::parse(file_->index_params_);
        to_index_engine_ = EngineFactory::Build(file_->dimension_, file_->location_, engine_type,
                                                (MetricType)file_->metric_type_, json);
    }
}

void
XBuildIndexTask::Load(milvus::scheduler::LoadType type, uint8_t device_id) {
    TimeRecorder rc("XBuildIndexTask::Load");
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

        size_t file_size = to_index_engine_->Size();

        std::string info = "Build index task load file id:" + std::to_string(file_->id_) + " " + type_str +
                           " file type:" + std::to_string(file_->file_type_) + " size:" + std::to_string(file_size) +
                           " bytes from location: " + file_->location_ + " totally cost";
        rc.ElapseFromBegin(info);

        to_index_id_ = file_->id_;
        to_index_type_ = file_->file_type_;
    }
}

void
XBuildIndexTask::Execute() {
    TimeRecorderAuto rc("XBuildIndexTask::Execute " + std::to_string(to_index_id_));

    if (auto job = job_.lock()) {
        auto build_index_job = std::static_pointer_cast<scheduler::BuildIndexJob>(job);
        if (to_index_engine_ == nullptr) {
            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = Status(DB_ERROR, "source index is null");
            return;
        }

        std::string location = file_->location_;
        EngineType engine_type = (EngineType)file_->engine_type_;
        std::shared_ptr<engine::ExecutionEngine> index;

        // step 1: create collection file
        engine::meta::SegmentSchema table_file;
        table_file.collection_id_ = file_->collection_id_;
        table_file.segment_id_ = file_->file_id_;
        table_file.date_ = file_->date_;
        table_file.file_type_ = engine::meta::SegmentSchema::NEW_INDEX;

        engine::meta::MetaPtr meta_ptr = build_index_job->meta();
        Status status = meta_ptr->CreateCollectionFile(table_file);

        fiu_do_on("XBuildIndexTask.Execute.create_table_success", status = Status::OK());
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to create collection file: " << status.ToString();
            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = status;
            to_index_engine_ = nullptr;
            return;
        }

        auto failed_build_index = [&](std::string log_msg, std::string err_msg) {
            table_file.file_type_ = engine::meta::SegmentSchema::TO_DELETE;
            status = meta_ptr->UpdateCollectionFile(table_file);
            LOG_ENGINE_ERROR_ << log_msg;

            build_index_job->BuildIndexDone(to_index_id_);
            build_index_job->GetStatus() = Status(DB_ERROR, err_msg);
            to_index_engine_ = nullptr;
        };

        // step 2: build index
        try {
            LOG_ENGINE_DEBUG_ << "Begin build index for file:" + table_file.location_;
            index = to_index_engine_->BuildIndex(table_file.location_, (EngineType)table_file.engine_type_);
            fiu_do_on("XBuildIndexTask.Execute.build_index_fail", index = nullptr);
            if (index == nullptr) {
                std::string log_msg = "Failed to build index " + table_file.file_id_ + ", reason: source index is null";
                failed_build_index(log_msg, "source index is null");
                return;
            }
        } catch (std::exception& ex) {
            std::string msg = "Failed to build index " + table_file.file_id_ + ", reason: " + std::string(ex.what());
            failed_build_index(msg, ex.what());
            return;
        }

        // step 3: if collection has been deleted, dont save index file
        bool has_collection = false;
        meta_ptr->HasCollection(file_->collection_id_, has_collection);
        fiu_do_on("XBuildIndexTask.Execute.has_collection", has_collection = true);

        if (!has_collection) {
            std::string msg = "Failed to build index " + table_file.file_id_ + ", reason: collection has been deleted";
            failed_build_index(msg, "Collection has been deleted");
            return;
        }

        // step 4: save index file
        try {
            fiu_do_on("XBuildIndexTask.Execute.throw_std_exception", throw std::exception());
            status = index->Serialize();
            if (!status.ok()) {
                std::string msg =
                    "Failed to persist index file: " + table_file.location_ + ", reason: " + status.message();
                failed_build_index(msg, status.message());
                return;
            }
        } catch (std::exception& ex) {
            // if failed to serialize index file to disk
            // typical error: out of disk space, out of memory or permition denied
            std::string msg =
                "Failed to persist index file:" + table_file.location_ + ", exception:" + std::string(ex.what());
            failed_build_index(msg, ex.what());
            return;
        }

        // step 5: update meta
        table_file.file_type_ = engine::meta::SegmentSchema::INDEX;
        table_file.file_size_ = server::CommonUtil::GetFileSize(table_file.location_);
        table_file.row_count_ = file_->row_count_;  // index->Count();

        auto origin_file = *file_;
        origin_file.file_type_ = engine::meta::SegmentSchema::BACKUP;

        engine::meta::SegmentsSchema update_files = {table_file, origin_file};

        if (status.ok()) {  // makesure index file is sucessfully serialized to disk
            status = meta_ptr->UpdateCollectionFiles(update_files);
        }

        fiu_do_on("XBuildIndexTask.Execute.update_table_file_fail", status = Status(SERVER_UNEXPECTED_ERROR, ""));
        if (status.ok()) {
            LOG_ENGINE_DEBUG_ << "New index file " << table_file.file_id_ << " of size " << table_file.file_size_
                              << " bytes"
                              << " from file " << origin_file.file_id_;
            if (build_index_job->options().insert_cache_immediately_) {
                index->Cache();
            }
        } else {
            // failed to update meta, mark the new file as to_delete, don't delete old file
            origin_file.file_type_ = engine::meta::SegmentSchema::TO_INDEX;
            status = meta_ptr->UpdateCollectionFile(origin_file);
            LOG_ENGINE_DEBUG_ << "Failed to update file to index, mark file: " << origin_file.file_id_
                              << " to to_index";

            table_file.file_type_ = engine::meta::SegmentSchema::TO_DELETE;
            status = meta_ptr->UpdateCollectionFile(table_file);
            LOG_ENGINE_DEBUG_ << "Failed to up  date file to index, mark file: " << table_file.file_id_
                              << " to to_delete";
        }

        build_index_job->BuildIndexDone(to_index_id_);
    }

    to_index_engine_ = nullptr;
}

}  // namespace scheduler
}  // namespace milvus
