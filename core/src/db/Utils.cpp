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

#include "db/Utils.h"
#include "server/Config.h"
#include "storage/s3/S3ClientWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"

#include <boost/filesystem.hpp>
#include <chrono>
#include <mutex>
#include <regex>
#include <vector>

namespace milvus {
namespace engine {
namespace utils {

namespace {

const char* TABLES_FOLDER = "/tables/";

uint64_t index_file_counter = 0;
std::mutex index_file_counter_mutex;

static std::string
ConstructParentFolder(const std::string& db_path, const meta::TableFileSchema& table_file) {
    std::string table_path = db_path + TABLES_FOLDER + table_file.table_id_;
    std::string partition_path = table_path + "/" + std::to_string(table_file.date_);
    return partition_path;
}

static std::string
GetTableFileParentFolder(const DBMetaOptions& options, const meta::TableFileSchema& table_file) {
    uint64_t path_count = options.slave_paths_.size() + 1;
    std::string target_path = options.path_;
    uint64_t index = 0;

    if (meta::TableFileSchema::NEW_INDEX == table_file.file_type_) {
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
        target_path = options.slave_paths_[index - 1];
    }

    return ConstructParentFolder(target_path, table_file);
}

}  // namespace

int64_t
GetMicroSecTimeStamp() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

    return micros;
}

Status
CreateTablePath(const DBMetaOptions& options, const std::string& table_id) {
    std::string db_path = options.path_;
    std::string table_path = db_path + TABLES_FOLDER + table_id;
    auto status = server::CommonUtil::CreateDirectory(table_path);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << status.message();
        return status;
    }

    for (auto& path : options.slave_paths_) {
        table_path = path + TABLES_FOLDER + table_id;
        status = server::CommonUtil::CreateDirectory(table_path);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << status.message();
            return status;
        }
    }

    return Status::OK();
}

Status
DeleteTablePath(const DBMetaOptions& options, const std::string& table_id, bool force) {
    std::vector<std::string> paths = options.slave_paths_;
    paths.push_back(options.path_);

    for (auto& path : paths) {
        std::string table_path = path + TABLES_FOLDER + table_id;
        if (force) {
            boost::filesystem::remove_all(table_path);
            ENGINE_LOG_DEBUG << "Remove table folder: " << table_path;
        } else if (boost::filesystem::exists(table_path) && boost::filesystem::is_empty(table_path)) {
            boost::filesystem::remove_all(table_path);
            ENGINE_LOG_DEBUG << "Remove table folder: " << table_path;
        }
    }

    bool minio_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigMinioEnable(minio_enable);

    if (minio_enable) {
        std::string table_path = options.path_ + TABLES_FOLDER + table_id;

        auto storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
        Status stat = storage_inst.DeleteObjects(table_path);
        if (!stat.ok()) {
            return stat;
        }
    }

    return Status::OK();
}

Status
CreateTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file) {
    std::string parent_path = GetTableFileParentFolder(options, table_file);

    auto status = server::CommonUtil::CreateDirectory(parent_path);
    if (!status.ok()) {
        ENGINE_LOG_ERROR << status.message();
        return status;
    }

    table_file.location_ = parent_path + "/" + table_file.file_id_;

    return Status::OK();
}

Status
GetTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file) {
    std::string parent_path = ConstructParentFolder(options.path_, table_file);
    std::string file_path = parent_path + "/" + table_file.file_id_;

    bool minio_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigMinioEnable(minio_enable);
    if (minio_enable) {
        /* need not check file existence */
        table_file.location_ = file_path;
        return Status::OK();
    }

    if (boost::filesystem::exists(file_path)) {
        table_file.location_ = file_path;
        return Status::OK();
    }

    for (auto& path : options.slave_paths_) {
        parent_path = ConstructParentFolder(path, table_file);
        file_path = parent_path + "/" + table_file.file_id_;
        if (boost::filesystem::exists(file_path)) {
            table_file.location_ = file_path;
            return Status::OK();
        }
    }

    std::string msg = "Table file doesn't exist: " + file_path;
    if (table_file.file_size_ > 0) {  // no need to pop error for empty file
        ENGINE_LOG_ERROR << msg << " in path: " << options.path_ << " for table: " << table_file.table_id_;
    }

    return Status(DB_ERROR, msg);
}

Status
DeleteTableFilePath(const DBMetaOptions& options, meta::TableFileSchema& table_file) {
    utils::GetTableFilePath(options, table_file);
    boost::filesystem::remove(table_file.location_);
    return Status::OK();
}

bool
IsSameIndex(const TableIndex& index1, const TableIndex& index2) {
    return index1.engine_type_ == index2.engine_type_ && index1.nlist_ == index2.nlist_ &&
           index1.metric_type_ == index2.metric_type_;
}

meta::DateT
GetDate(const std::time_t& t, int day_delta) {
    struct tm ltm;
    localtime_r(&t, &ltm);
    if (day_delta > 0) {
        do {
            ++ltm.tm_mday;
            --day_delta;
        } while (day_delta > 0);
        mktime(&ltm);
    } else if (day_delta < 0) {
        do {
            --ltm.tm_mday;
            ++day_delta;
        } while (day_delta < 0);
        mktime(&ltm);
    } else {
        ltm.tm_mday;
    }
    return ltm.tm_year * 10000 + ltm.tm_mon * 100 + ltm.tm_mday;
}

meta::DateT
GetDateWithDelta(int day_delta) {
    return GetDate(std::time(nullptr), day_delta);
}

meta::DateT
GetDate() {
    return GetDate(std::time(nullptr), 0);
}

// URI format: dialect://username:password@host:port/database
Status
ParseMetaUri(const std::string& uri, MetaUriInfo& info) {
    std::string dialect_regex = "(.*)";
    std::string username_tegex = "(.*)";
    std::string password_regex = "(.*)";
    std::string host_regex = "(.*)";
    std::string port_regex = "(.*)";
    std::string db_name_regex = "(.*)";
    std::string uri_regex_str = dialect_regex + "\\:\\/\\/" + username_tegex + "\\:" + password_regex + "\\@" +
                                host_regex + "\\:" + port_regex + "\\/" + db_name_regex;

    std::regex uri_regex(uri_regex_str);
    std::smatch pieces_match;

    if (std::regex_match(uri, pieces_match, uri_regex)) {
        info.dialect_ = pieces_match[1].str();
        info.username_ = pieces_match[2].str();
        info.password_ = pieces_match[3].str();
        info.host_ = pieces_match[4].str();
        info.port_ = pieces_match[5].str();
        info.db_name_ = pieces_match[6].str();

        // TODO(myh): verify host, port...
    } else {
        return Status(DB_INVALID_META_URI, "Invalid meta uri: " + uri);
    }

    return Status::OK();
}

}  // namespace utils
}  // namespace engine
}  // namespace milvus
