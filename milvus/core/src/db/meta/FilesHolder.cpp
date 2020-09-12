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

#include "db/meta/FilesHolder.h"
#include "utils/Log.h"

#include <utility>

namespace milvus {
namespace engine {
namespace meta {

//////////////////////////////////////////////////////////////////////////////////////////////////////////
FilesHolder::OngoingFileChecker&
FilesHolder::OngoingFileChecker::GetInstance() {
    static OngoingFileChecker instance;
    return instance;
}

Status
FilesHolder::OngoingFileChecker::MarkOngoingFile(const meta::SegmentSchema& table_file) {
    std::lock_guard<std::mutex> lck(mutex_);
    return MarkOngoingFileNoLock(table_file);
}

Status
FilesHolder::OngoingFileChecker::MarkOngoingFiles(const meta::SegmentsSchema& table_files) {
    std::lock_guard<std::mutex> lck(mutex_);

    for (auto& table_file : table_files) {
        MarkOngoingFileNoLock(table_file);
    }

    return Status::OK();
}

Status
FilesHolder::OngoingFileChecker::UnmarkOngoingFile(const meta::SegmentSchema& table_file) {
    std::lock_guard<std::mutex> lck(mutex_);
    return UnmarkOngoingFileNoLock(table_file);
}

Status
FilesHolder::OngoingFileChecker::UnmarkOngoingFiles(const meta::SegmentsSchema& table_files) {
    std::lock_guard<std::mutex> lck(mutex_);

    for (auto& table_file : table_files) {
        UnmarkOngoingFileNoLock(table_file);
    }

    return Status::OK();
}

bool
FilesHolder::OngoingFileChecker::CanBeDeleted(const meta::SegmentSchema& schema) {
    std::lock_guard<std::mutex> lck(mutex_);

    auto iter = ongoing_files_.find(schema.collection_id_);
    if (iter == ongoing_files_.end()) {
        return true;
    } else {
        auto it_file = iter->second.find(schema.id_);
        if (it_file == iter->second.end()) {
            return true;
        } else {
            return (it_file->second > 0) ? false : true;
        }
    }
}

void
FilesHolder::OngoingFileChecker::PrintInfo() {
    std::lock_guard<std::mutex> lck(mutex_);
    if (!ongoing_files_.empty()) {
        LOG_ENGINE_DEBUG_ << "File reference information:";
        for (meta::Table2FileRef::iterator iter = ongoing_files_.begin(); iter != ongoing_files_.end(); ++iter) {
            LOG_ENGINE_DEBUG_ << "\t" << iter->first << ": " << iter->second.size() << " files in use";
        }
    }
}

Status
FilesHolder::OngoingFileChecker::MarkOngoingFileNoLock(const meta::SegmentSchema& table_file) {
    if (table_file.collection_id_.empty() || table_file.file_id_.empty()) {
        return Status(DB_ERROR, "Invalid collection files");
    }

    auto iter = ongoing_files_.find(table_file.collection_id_);
    if (iter == ongoing_files_.end()) {
        File2RefCount files_refcount;
        files_refcount.insert(std::make_pair(table_file.id_, 1));
        ongoing_files_.insert(std::make_pair(table_file.collection_id_, files_refcount));
    } else {
        auto it_file = iter->second.find(table_file.id_);
        if (it_file == iter->second.end()) {
            iter->second[table_file.id_] = 1;
        } else {
            it_file->second++;
        }
    }

    LOG_ENGINE_DEBUG_ << "Mark ongoing file:" << table_file.file_id_
                      << " refcount:" << ongoing_files_[table_file.collection_id_][table_file.id_];

    return Status::OK();
}

Status
FilesHolder::OngoingFileChecker::UnmarkOngoingFileNoLock(const meta::SegmentSchema& table_file) {
    if (table_file.collection_id_.empty() || table_file.file_id_.empty()) {
        return Status(DB_ERROR, "Invalid collection files");
    }

    auto iter = ongoing_files_.find(table_file.collection_id_);
    if (iter != ongoing_files_.end()) {
        auto it_file = iter->second.find(table_file.id_);
        if (it_file != iter->second.end()) {
            it_file->second--;

            LOG_ENGINE_DEBUG_ << "Unmark ongoing file:" << table_file.file_id_ << " refcount:" << it_file->second;

            if (it_file->second <= 0) {
                iter->second.erase(table_file.id_);
                if (iter->second.empty()) {
                    ongoing_files_.erase(table_file.collection_id_);
                }
            }
        }
    }

    return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
FilesHolder::FilesHolder() {
}

FilesHolder::~FilesHolder() {
    ReleaseFiles();
}

Status
FilesHolder::MarkFile(const meta::SegmentSchema& file) {
    std::lock_guard<std::mutex> lck(mutex_);
    return MarkFileInternal(file);
}

Status
FilesHolder::MarkFiles(const meta::SegmentsSchema& files) {
    std::lock_guard<std::mutex> lck(mutex_);
    for (auto& file : files) {
        MarkFileInternal(file);
    }

    return Status::OK();
}

Status
FilesHolder::UnmarkFile(const meta::SegmentSchema& file) {
    std::lock_guard<std::mutex> lck(mutex_);
    return UnmarkFileInternal(file);
}

Status
FilesHolder::UnmarkFiles(const meta::SegmentsSchema& files) {
    std::lock_guard<std::mutex> lck(mutex_);
    for (auto& file : files) {
        UnmarkFileInternal(file);
    }

    return Status::OK();
}

void
FilesHolder::ReleaseFiles() {
    std::lock_guard<std::mutex> lck(mutex_);
    OngoingFileChecker::GetInstance().UnmarkOngoingFiles(hold_files_);
    hold_files_.clear();
    unique_ids_.clear();
}

bool
FilesHolder::CanBeDeleted(const meta::SegmentSchema& file) {
    return OngoingFileChecker::GetInstance().CanBeDeleted(file);
}

void
FilesHolder::PrintInfo() {
    return OngoingFileChecker::GetInstance().PrintInfo();
}

Status
FilesHolder::MarkFileInternal(const meta::SegmentSchema& file) {
    if (unique_ids_.find(file.id_) != unique_ids_.end()) {
        return Status::OK();  // already marked
    }

    auto status = OngoingFileChecker::GetInstance().MarkOngoingFile(file);
    if (status.ok()) {
        unique_ids_.insert(file.id_);
        hold_files_.push_back(file);
    }

    return status;
}

Status
FilesHolder::UnmarkFileInternal(const meta::SegmentSchema& file) {
    if (unique_ids_.find(file.id_) == unique_ids_.end()) {
        return Status::OK();  // no such file
    }

    auto status = OngoingFileChecker::GetInstance().UnmarkOngoingFile(file);
    if (status.ok()) {
        for (auto iter = hold_files_.begin(); iter != hold_files_.end(); ++iter) {
            if (file.id_ == (*iter).id_) {
                hold_files_.erase(iter);
                break;
            }
        }

        unique_ids_.erase(file.id_);
    }
    return status;
}

}  // namespace meta
}  // namespace engine
}  // namespace milvus
