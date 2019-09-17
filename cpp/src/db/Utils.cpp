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

#include "Utils.h"
#include "utils/CommonUtil.h"
#include "Log.h"

#include <mutex>
#include <chrono>
#include <boost/filesystem.hpp>

namespace zilliz {
namespace milvus {
namespace engine {
namespace utils {

namespace {

const char* TABLES_FOLDER = "/tables/";

uint64_t index_file_counter = 0;
std::mutex index_file_counter_mutex;

std::string ConstructParentFolder(const std::string& db_path, const meta::TableFileSchema& table_file) {
    std::string table_path = db_path + TABLES_FOLDER + table_file.table_id_;
    std::string partition_path = table_path + "/" + std::to_string(table_file.date_);
    return partition_path;
}

std::string GetTableFileParentFolder(const DBMetaOptions& options, const meta::TableFileSchema& table_file) {
    uint64_t path_count = options.slave_paths.size() + 1;
    std::string target_path = options.path;
    uint64_t index = 0;

    if(meta::TableFileSchema::NEW_INDEX == table_file.file_type_) {
        // index file is large file and to be persisted permanently
        // we need to distribute index files to each db_path averagely
        // round robin according to a file counter
        std::lock_guard<std::mutex> lock(index_file_counter_mutex);
        index = index_file_counter % path_count;
        index_file_counter++;
    } else {
        // for other type files, they could be merged or deleted
        // so we round robin according to their file id
        index = table_file.id_ % path_count;
    }

    if (index > 0) {
        target_path = options.slave_paths[index - 1];
    }

    return ConstructParentFolder(target_path, table_file);
}

}

long GetMicroSecTimeStamp() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

    return micros;
}

Status CreateTablePath(const DBMetaOptions& options, const std::string& table_id) {
    std::string db_path = options.path;
    std::string table_path = db_path + TABLES_FOLDER + table_id;
    auto status = server::CommonUtil::CreateDirectory(table_path);
    if (status != 0) {
        ENGINE_LOG_ERROR << "Create directory " << table_path << " Error";
        return Status(DB_ERROR, "Failed to create table path");
    }

    for(auto& path : options.slave_paths) {
        table_path = path + TABLES_FOLDER + table_id;
        status = server::CommonUtil::CreateDirectory(table_path);
        if (status != 0) {
            ENGINE_LOG_ERROR << "Create directory " << table_path << " Error";
            return Status(DB_ERROR, "Failed to create table path");
        }
    }

    return Status::OK();
}

Status DeleteTablePath(const DBMetaOptions& options, const std::string& table_id, bool force) {
    std::vector<std::string> paths = options.slave_paths;
    paths.push_back(options.path);

    for(auto& path : paths) {
        std::string table_path = path + TABLES_FOLDER + table_id;
        if(force) {
            boost::filesystem::remove_all(table_path);
            ENGINE_LOG_DEBUG << "Remove table folder: " << table_path;
        } else if(boost::filesystem::exists(table_path) &&
                  boost::filesystem::is_empty(table_path)) {
            boost::filesystem::remove_all(table_path);
            ENGINE_LOG_DEBUG << "Remove table folder: " << table_path;
        }
    }

    return Status::OK();
}

Status CreateTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file) {
    std::string parent_path = GetTableFileParentFolder(options, table_file);

    auto status = server::CommonUtil::CreateDirectory(parent_path);
    if (status != 0) {
        ENGINE_LOG_ERROR << "Create directory " << parent_path << " Error";
        return Status(DB_ERROR, "Failed to create partition directory");
    }

    table_file.location_ = parent_path + "/" + table_file.file_id_;

    return Status::OK();
}

Status GetTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file) {
    std::string parent_path = ConstructParentFolder(options.path, table_file);
    std::string file_path = parent_path + "/" + table_file.file_id_;
    if(boost::filesystem::exists(file_path)) {
        table_file.location_ = file_path;
        return Status::OK();
    } else {
        for(auto& path : options.slave_paths) {
            parent_path = ConstructParentFolder(path, table_file);
            file_path = parent_path + "/" + table_file.file_id_;
            if(boost::filesystem::exists(file_path)) {
                table_file.location_ = file_path;
                return Status::OK();
            }
        }
    }

    std::string msg = "Table file doesn't exist: " + file_path;
    ENGINE_LOG_ERROR << msg;
    return Status(DB_ERROR, msg);
}

Status DeleteTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file) {
    utils::GetTableFilePath(options, table_file);
    boost::filesystem::remove(table_file.location_);
    return Status::OK();
}

bool IsSameIndex(const TableIndex& index1, const TableIndex& index2) {
    return index1.engine_type_ == index2.engine_type_
        && index1.nlist_ == index2.nlist_
        && index1.metric_type_ == index2.metric_type_;
}

meta::DateT GetDate(const std::time_t& t, int day_delta) {
    struct tm ltm;
    localtime_r(&t, &ltm);
    if (day_delta > 0) {
        do {
            ++ltm.tm_mday;
            --day_delta;
        } while(day_delta > 0);
        mktime(&ltm);
    } else if (day_delta < 0) {
        do {
            --ltm.tm_mday;
            ++day_delta;
        } while(day_delta < 0);
        mktime(&ltm);
    } else {
        ltm.tm_mday;
    }
    return ltm.tm_year*10000 + ltm.tm_mon*100 + ltm.tm_mday;
}

meta::DateT GetDateWithDelta(int day_delta) {
    return GetDate(std::time(nullptr), day_delta);
}

meta::DateT GetDate() {
    return GetDate(std::time(nullptr), 0);
}

} // namespace utils
} // namespace engine
} // namespace milvus
} // namespace zilliz
