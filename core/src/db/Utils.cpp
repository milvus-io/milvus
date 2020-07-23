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
#include <memory>
#include <mutex>
#include <regex>
#include <vector>

#include "cache/CpuCacheMgr.h"
#include "db/Types.h"

#ifdef MILVUS_GPU_VERSION
#include "cache/GpuCacheMgr.h"
#endif

#include "config/Config.h"
//#include "storage/s3/S3ClientWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"

#include <map>

namespace milvus {
namespace engine {
namespace utils {

namespace {

const char* TABLES_FOLDER = "/tables/";

static std::string
ConstructParentFolder(const std::string& db_path, const meta::SegmentSchema& table_file) {
    std::string table_path = db_path + TABLES_FOLDER + table_file.collection_id_;
    std::string partition_path = table_path + "/" + table_file.segment_id_;
    return partition_path;
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
    auto status = CommonUtil::CreateDirectory(table_path);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }
    return Status::OK();
}

Status
DeleteCollectionPath(const DBMetaOptions& options, const std::string& collection_id, bool force) {
    std::string table_path = options.path_ + TABLES_FOLDER + collection_id;
    if (force) {
        boost::filesystem::remove_all(table_path);
        LOG_ENGINE_DEBUG_ << "Remove collection folder: " << table_path;
    } else if (boost::filesystem::exists(table_path) && boost::filesystem::is_empty(table_path)) {
        boost::filesystem::remove_all(table_path);
        LOG_ENGINE_DEBUG_ << "Remove collection folder: " << table_path;
    }

    // bool s3_enable = false;
    // server::Config& config = server::Config::GetInstance();
    // config.GetStorageConfigS3Enable(s3_enable);

    // if (s3_enable) {
    //     std::string table_path = options.path_ + TABLES_FOLDER + collection_id;

    //     auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    //     Status stat = storage_inst.DeleteObjects(table_path);
    //     if (!stat.ok()) {
    //         return stat;
    //     }
    // }

    return Status::OK();
}

Status
CreateCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    std::string parent_path = ConstructParentFolder(options.path_, table_file);

    auto status = CommonUtil::CreateDirectory(parent_path);
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
    std::string file_path = parent_path + "/" + table_file.file_id_;

    // bool s3_enable = false;
    // server::Config& config = server::Config::GetInstance();
    // config.GetStorageConfigS3Enable(s3_enable);
    // fiu_do_on("GetCollectionFilePath.enable_s3", s3_enable = true);
    // if (s3_enable) {
    //     /* need not check file existence */
    //     table_file.location_ = file_path;
    //     return Status::OK();
    // }

    if (boost::filesystem::exists(parent_path)) {
        table_file.location_ = file_path;
        return Status::OK();
    }

    std::string msg = "Collection file doesn't exist: " + file_path;
    if (table_file.file_size_ > 0) {  // no need to pop error for empty file
        LOG_ENGINE_ERROR_ << msg << " in path: " << options.path_ << " for collection: " << table_file.collection_id_;
    }

    return Status(DB_ERROR, msg);
}

Status
DeleteCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    utils::GetCollectionFilePath(options, table_file);
    boost::filesystem::remove(table_file.location_);
    return Status::OK();
}

Status
DeleteSegment(const DBMetaOptions& options, meta::SegmentSchema& table_file) {
    utils::GetCollectionFilePath(options, table_file);
    std::string segment_dir;
    GetParentPath(table_file.location_, segment_dir);
    boost::filesystem::remove_all(segment_dir);
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
        {(int32_t)engine::EngineType::FAISS_IDMAP, "IDMAP"},
        {(int32_t)engine::EngineType::FAISS_IVFFLAT, "IVFFLAT"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8, "IVFSQ8"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8NR, "IVFSQ8NR"},
        {(int32_t)engine::EngineType::FAISS_IVFSQ8H, "IVFSQ8H"},
        {(int32_t)engine::EngineType::FAISS_PQ, "PQ"},
#ifdef MILVUS_SUPPORT_SPTAG
        {(int32_t)engine::EngineType::SPTAG_KDT, "KDT"},
        {(int32_t)engine::EngineType::SPTAG_BKT, "BKT"},
#endif
        {(int32_t)engine::EngineType::FAISS_BIN_IDMAP, "IDMAP"},
        {(int32_t)engine::EngineType::FAISS_BIN_IVFFLAT, "IVFFLAT"},
        {(int32_t)engine::EngineType::HNSW_SQ8NM, "HNSW_SQ8NM"},
        {(int32_t)engine::EngineType::HNSW, "HNSW"},
        {(int32_t)engine::EngineType::NSG_MIX, "NSG"},
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

void
EraseFromCache(const std::string& item_key) {
    if (item_key.empty()) {
        LOG_SERVER_ERROR_ << "Empty key cannot be erased from cache";
        return;
    }

    cache::CpuCacheMgr::GetInstance()->EraseItem(item_key);

#ifdef MILVUS_GPU_VERSION
    server::Config& config = server::Config::GetInstance();
    std::vector<int64_t> gpus;
    config.GetGpuResourceConfigSearchResources(gpus);
    for (auto& gpu : gpus) {
        cache::GpuCacheMgr::GetInstance(gpu)->EraseItem(item_key);
    }
#endif
}
}  // namespace utils
}  // namespace engine
}  // namespace milvus
