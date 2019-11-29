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

#include "db/IndexFailedChecker.h"

#include <utility>

namespace milvus {
namespace engine {

constexpr uint64_t INDEX_FAILED_RETRY_TIME = 1;

Status
IndexFailedChecker::CleanFailedIndexFileOfTable(const std::string& table_id) {
    std::lock_guard<std::mutex> lck(mutex_);
    index_failed_files_.erase(table_id);  // rebuild failed index files for this table

    return Status::OK();
}

Status
IndexFailedChecker::GetFailedIndexFileOfTable(const std::string& table_id, std::vector<std::string>& failed_files) {
    failed_files.clear();
    std::lock_guard<std::mutex> lck(mutex_);
    auto iter = index_failed_files_.find(table_id);
    if (iter != index_failed_files_.end()) {
        File2RefCount& failed_map = iter->second;
        for (auto it_file = failed_map.begin(); it_file != failed_map.end(); ++it_file) {
            failed_files.push_back(it_file->first);
        }
    }

    return Status::OK();
}

Status
IndexFailedChecker::MarkFailedIndexFile(const meta::TableFileSchema& file) {
    std::lock_guard<std::mutex> lck(mutex_);

    auto iter = index_failed_files_.find(file.table_id_);
    if (iter == index_failed_files_.end()) {
        File2RefCount failed_files;
        failed_files.insert(std::make_pair(file.file_id_, 1));
        index_failed_files_.insert(std::make_pair(file.table_id_, failed_files));
    } else {
        auto it_failed_files = iter->second.find(file.file_id_);
        if (it_failed_files != iter->second.end()) {
            it_failed_files->second++;
        } else {
            iter->second.insert(std::make_pair(file.file_id_, 1));
        }
    }

    return Status::OK();
}

Status
IndexFailedChecker::MarkSucceedIndexFile(const meta::TableFileSchema& file) {
    std::lock_guard<std::mutex> lck(mutex_);

    auto iter = index_failed_files_.find(file.table_id_);
    if (iter != index_failed_files_.end()) {
        iter->second.erase(file.file_id_);
        if (iter->second.empty()) {
            index_failed_files_.erase(file.table_id_);
        }
    }

    return Status::OK();
}

Status
IndexFailedChecker::IgnoreFailedIndexFiles(meta::TableFilesSchema& table_files) {
    std::lock_guard<std::mutex> lck(mutex_);

    // there could be some failed files belong to different table.
    // some files may has failed for several times, no need to build index for these files.
    // thus we can avoid dead circle for build index operation
    for (auto it_file = table_files.begin(); it_file != table_files.end();) {
        auto it_failed_files = index_failed_files_.find((*it_file).table_id_);
        if (it_failed_files != index_failed_files_.end()) {
            auto it_failed_file = it_failed_files->second.find((*it_file).file_id_);
            if (it_failed_file != it_failed_files->second.end()) {
                if (it_failed_file->second >= INDEX_FAILED_RETRY_TIME) {
                    it_file = table_files.erase(it_file);
                    continue;
                }
            }
        }

        ++it_file;
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
