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

#include "db/Utils.h"

#include <fiu-local.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <chrono>
#include <mutex>
#include <regex>
#include <vector>

#include "config/Config.h"
#ifdef MILVUS_WITH_AWS
#include "storage/s3/S3ClientWrapper.h"
#endif
#ifdef MILVUS_WITH_OSS
#include "storage/oss/OSSClientWrapper.h"
#endif
#include <map>

#include "utils/CommonUtil.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {
namespace utils {

const char* RAWDATA_INDEX_NAME = "IDMAP";

namespace {

const char* TABLES_FOLDER = "/tables/";

uint64_t index_file_counter = 0;
std::mutex index_file_counter_mutex;

static std::string
ConstructParentFolder(const std::string& db_path, const meta::SegmentSchema& table_file) {
    std::string table_path = db_path + TABLES_FOLDER + table_file.collection_id_;
    std::string partition_path = table_path + "/" + table_file.segment_id_;
    return partition_path;
}

static std::string
GetCollectionFileParentFolder(const DBMetaOptions& options, const meta::SegmentSchema& table_file) {
    uint64_t path_count = options.slave_paths_.size() + 1;
    std::string target_path = options.path_;
    uint64_t index = 0;

    if (meta::SegmentSchema::NEW_INDEX == table_file.file_type_) {
        // index file is large file and to be persisted permanently
        // we need to distribute index files to each db_path averagely
        // round robin according to a file counter
        std::lock_guard<std::mutex> lock(index_file_counter_mutex);
        index = index_file_counter % path_count;
        ++index_file_counter;
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
CreateCollectionPath(const DBMetaOptions& options, const std::string& collection_id) {
    std::string db_path = options.path_;
    std::string table_path = db_path + TABLES_FOLDER + collection_id;
    auto status = server::CommonUtil::CreateDirectory(table_path);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    for (auto& path : options.slave_paths_) {
        table_path = path + TABLES_FOLDER + collection_id;
        status = server::CommonUtil::CreateDirectory(table_path);
        fiu_do_on("CreateCollectionPath.creat_slave_path", status = Status(DB_INVALID_PATH, ""));
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << status.message();
            return status;
        }
    }

    return Status::OK();
}

Status
DeleteCollectionPath(const DBMetaOptions& options, const std::string& collection_id, bool force) {
    std::vector<std::string> paths = options.slave_paths_;
    paths.push_back(options.path_);

    for (auto& path : paths) {
        std::string table_path = path + TABLES_FOLDER + collection_id;
        if (force) {
            boost::filesystem::remove_all(table_path);
            LOG_ENGINE_DEBUG_ << "Remove collection folder: " << table_path;
        } else if (boost::filesystem::exists(table_path) && boost::filesystem::is_empty(table_path)) {
            boost::filesystem::remove_all(table_path);
            LOG_ENGINE_DEBUG_ << "Remove collection folder: " << table_path;
        }
    }

#if defined(MILVUS_WITH_AWS) || defined(MILVUS_WITH_OSS)
    server::Config& config = server::Config::GetInstance();
#endif

#ifdef MILVUS_WITH_AWS
    bool s3_enable = false;
    config.GetStorageConfigS3Enable(s3_enable);

    if (s3_enable) {
        std::string table_path = options.path_ + TABLES_FOLDER + collection_id;

        auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
        return storage_inst.DeleteObjects(table_path);
    }
#endif

#ifdef MILVUS_WITH_OSS
    bool oss_enable = false;
    config.GetStorageConfigOSSEnable(oss_enable);

    if (oss_enable) {
        std::string table_path = options.path_ + TABLES_FOLDER + collection_id;

        auto& storage_inst = milvus::storage::OSSClientWrapper::GetInstance();
        return storage_inst.DeleteObjects(table_path);
    }
#endif

    return Status::OK();
}

Status
CreateCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    std::string parent_path = GetCollectionFileParentFolder(options, table_file);

    auto status = server::CommonUtil::CreateDirectory(parent_path);
    fiu_do_on("CreateCollectionFilePath.fail_create", status = Status(DB_INVALID_PATH, ""));
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    table_file.location_ = parent_path + "/" + table_file.file_id_;

    return Status::OK();
}

Status
GetCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    std::string parent_path = ConstructParentFolder(options.path_, table_file);
    table_file.location_ = parent_path + "/" + table_file.file_id_;
    return Status::OK();
}

Status
DeleteCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    utils::GetCollectionFilePath(options, table_file);
    boost::filesystem::remove(table_file.location_);

#if defined(MILVUS_WITH_AWS) || defined(MILVUS_WITH_OSS)
    server::Config& config = server::Config::GetInstance();
#endif

#if MILVUS_WITH_AWS
    bool s3_enable = false;
    config.GetStorageConfigS3Enable(s3_enable);

    if (s3_enable) {
        auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
        return storage_inst.DeleteObjects(table_file.location_);
    }
#endif

#if MILVUS_WITH_OSS
    bool oss_enable = false;
    config.GetStorageConfigOSSEnable(s3_enable);

    if (oss_enable) {
        auto& storage_inst = milvus::storage::OSSClientWrapper::GetInstance();
        return storage_inst.DeleteObjects(table_file.location_);
    }
#endif

    return Status::OK();
}

Status
DeleteSegment(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    utils::GetCollectionFilePath(options, table_file);
    std::string segment_dir;
    GetParentPath(table_file.location_, segment_dir);
    boost::filesystem::remove_all(segment_dir);

#if defined(MILVUS_WITH_AWS) || defined(MILVUS_WITH_OSS)
    server::Config& config = server::Config::GetInstance();
#endif

#ifdef MILVUS_WITH_AWS
    bool s3_enable = false;
    config.GetStorageConfigS3Enable(s3_enable);

    if (s3_enable) {
        auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
        return storage_inst.DeleteObjects(segment_dir);
    }
#endif

#ifdef MILVUS_WITH_OSS
    bool oss_enable = false;
    config.GetStorageConfigOSSEnable(oss_enable);

    if (oss_enable) {
        auto& storage_inst = milvus::storage::OSSClientWrapper::GetInstance();
        return storage_inst.DeleteObjects(segment_dir);
    }
#endif
    return Status::OK();
}

Status
GetParentPath(const std::string& path, std::string& parent_path) {
    boost::filesystem::path p(path);
    parent_path = p.parent_path().string();
    return Status::OK();
}

bool
IsSameIndex(const CollectionIndex& index1, const CollectionIndex& index2) {
    return index1.engine_type_ == index2.engine_type_ && index1.extra_params_ == index2.extra_params_ &&
           index1.metric_type_ == index2.metric_type_;
}

bool
IsRawIndexType(int32_t type) {
    return (type == (int32_t)EngineType::FAISS_IDMAP) || (type == (int32_t)EngineType::FAISS_BIN_IDMAP);
}

bool
IsBinaryMetricType(int32_t metric_type) {
    return (metric_type == (int32_t)engine::MetricType::HAMMING) ||
           (metric_type == (int32_t)engine::MetricType::JACCARD) ||
           (metric_type == (int32_t)engine::MetricType::SUBSTRUCTURE) ||
           (metric_type == (int32_t)engine::MetricType::SUPERSTRUCTURE) ||
           (metric_type == (int32_t)engine::MetricType::TANIMOTO);
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

std::string
GetIndexName(int32_t index_type) {
    static std::map<int32_t, std::string> index_type_name = {
        {(int32_t)engine::EngineType::FAISS_IDMAP, RAWDATA_INDEX_NAME},
        {(int32_t)engine::EngineType::FAISS_IVFFLAT, "IVFFLAT"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8, "IVFSQ8"},
        {(int32_t)engine::EngineType::NSG_MIX, "NSG"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8H, "IVFSQ8H"},
        {(int32_t)engine::EngineType::FAISS_PQ, "PQ"},
        {(int32_t)engine::EngineType::SPTAG_KDT, "KDT"},
        {(int32_t)engine::EngineType::SPTAG_BKT, "BKT"},
        {(int32_t)engine::EngineType::FAISS_BIN_IDMAP, RAWDATA_INDEX_NAME},
        {(int32_t)engine::EngineType::FAISS_BIN_IVFFLAT, "IVFFLAT"},
        {(int32_t)engine::EngineType::HNSW, "HNSW"},
        {(int32_t)engine::EngineType::ANNOY, "ANNOY"}};

    if (index_type_name.find(index_type) == index_type_name.end()) {
        return "Unknow";
    }

    return index_type_name[index_type];
}

void
SendExitSignal() {
    LOG_SERVER_INFO_ << "Send SIGUSR2 signal to exit";
    pid_t pid = getpid();
    kill(pid, SIGUSR2);
}

void
ExitOnWriteError(Status& status) {
    if (status.code() == SERVER_WRITE_ERROR) {
        utils::SendExitSignal();
    }
}

}  // namespace utils
}  // namespace engine
}  // namespace milvus
