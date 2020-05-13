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

#include "db/DBImpl.h"

#include <assert.h>
#include <fiu-local.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <utility>

#include "Utils.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "db/IDGenerator.h"
#include "db/merge/MergeManagerFactory.h"
#include "engine/EngineFactory.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/BuilderSuspend.h"
#include "index/thirdparty/faiss/utils/distances.h"
#include "insert/MemManagerFactory.h"
#include "meta/MetaConsts.h"
#include "meta/MetaFactory.h"
#include "meta/SqliteMetaImpl.h"
#include "metrics/Metrics.h"
#include "scheduler/Definition.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/BuildIndexJob.h"
#include "scheduler/job/DeleteJob.h"
#include "scheduler/job/SearchJob.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"
#include "wal/WalDefinations.h"

#include "search/TaskInst.h"

namespace milvus {
namespace engine {

namespace {
constexpr uint64_t BACKGROUND_METRIC_INTERVAL = 1;
constexpr uint64_t BACKGROUND_INDEX_INTERVAL = 1;
constexpr uint64_t WAIT_BUILD_INDEX_INTERVAL = 5;

constexpr const char* JSON_ROW_COUNT = "row_count";
constexpr const char* JSON_PARTITIONS = "partitions";
constexpr const char* JSON_PARTITION_TAG = "tag";
constexpr const char* JSON_SEGMENTS = "segments";
constexpr const char* JSON_SEGMENT_NAME = "name";
constexpr const char* JSON_INDEX_NAME = "index_name";
constexpr const char* JSON_DATA_SIZE = "data_size";

static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");

}  // namespace

DBImpl::DBImpl(const DBOptions& options)
    : options_(options), initialized_(false), merge_thread_pool_(1, 1), index_thread_pool_(1, 1) {
    meta_ptr_ = MetaFactory::Build(options.meta_, options.mode_);
    mem_mgr_ = MemManagerFactory::Build(meta_ptr_, options_);
    merge_mgr_ptr_ = MergeManagerFactory::Build(meta_ptr_, options_);

    if (options_.wal_enable_) {
        wal::MXLogConfiguration mxlog_config;
        mxlog_config.recovery_error_ignore = options_.recovery_error_ignore_;
        // 2 buffers in the WAL
        mxlog_config.buffer_size = options_.buffer_size_ / 2;
        mxlog_config.mxlog_path = options_.mxlog_path_;
        wal_mgr_ = std::make_shared<wal::WalManager>(mxlog_config);
    }

    SetIdentity("DBImpl");
    AddCacheInsertDataListener();
    AddUseBlasThresholdListener();

    Start();
}

DBImpl::~DBImpl() {
    Stop();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// external api
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    // wal
    if (options_.wal_enable_) {
        auto error_code = DB_ERROR;
        if (wal_mgr_ != nullptr) {
            error_code = wal_mgr_->Init(meta_ptr_);
        }
        if (error_code != WAL_SUCCESS) {
            throw Exception(error_code, "Wal init error!");
        }

        // recovery
        while (1) {
            wal::MXLogRecord record;
            auto error_code = wal_mgr_->GetNextRecovery(record);
            if (error_code != WAL_SUCCESS) {
                throw Exception(error_code, "Wal recovery error!");
            }
            if (record.type == wal::MXLogType::None) {
                break;
            }
            ExecWalRecord(record);
        }

        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background wal thread
            bg_wal_thread_ = std::thread(&DBImpl::BackgroundWalThread, this);
        }
    } else {
        // for distribute version, some nodes are read only
        if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
            // background flush thread
            bg_flush_thread_ = std::thread(&DBImpl::BackgroundFlushThread, this);
        }
    }

    // for distribute version, some nodes are read only
    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        // background build index thread
        bg_index_thread_ = std::thread(&DBImpl::BackgroundIndexThread, this);
    }

    // background metric thread
    bg_metric_thread_ = std::thread(&DBImpl::BackgroundMetricThread, this);

    return Status::OK();
}

Status
DBImpl::Stop() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    initialized_.store(false, std::memory_order_release);

    if (options_.mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        if (options_.wal_enable_) {
            // wait wal thread finish
            swn_wal_.Notify();
            bg_wal_thread_.join();
        } else {
            // flush all without merge
            wal::MXLogRecord record;
            record.type = wal::MXLogType::Flush;
            ExecWalRecord(record);

            // wait flush thread finish
            swn_flush_.Notify();
            bg_flush_thread_.join();
        }

        WaitMergeFileFinish();

        swn_index_.Notify();
        bg_index_thread_.join();

        meta_ptr_->CleanUpShadowFiles();
    }

    // wait metric thread exit
    swn_metric_.Notify();
    bg_metric_thread_.join();

    // LOG_ENGINE_TRACE_ << "DB service stop";
    return Status::OK();
}

Status
DBImpl::DropAll() {
    return meta_ptr_->DropAll();
}

Status
DBImpl::CreateCollection(meta::CollectionSchema& collection_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::CollectionSchema temp_schema = collection_schema;
    temp_schema.index_file_size_ *= MB;  // store as MB
    if (options_.wal_enable_) {
        temp_schema.flush_lsn_ = wal_mgr_->CreateCollection(collection_schema.collection_id_);
    }

    return meta_ptr_->CreateCollection(temp_schema);
}

Status
DBImpl::CreateHybridCollection(meta::CollectionSchema& collection_schema, meta::hybrid::FieldsSchema& fields_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    meta::CollectionSchema temp_schema = collection_schema;
    temp_schema.index_file_size_ *= MB;
    if (options_.wal_enable_) {
        temp_schema.flush_lsn_ = wal_mgr_->CreateHybridCollection(collection_schema.collection_id_);
    }

    return meta_ptr_->CreateHybridCollection(temp_schema, fields_schema);
}

Status
DBImpl::DescribeHybridCollection(meta::CollectionSchema& collection_schema,
                                 milvus::engine::meta::hybrid::FieldsSchema& fields_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    auto stat = meta_ptr_->DescribeHybridCollection(collection_schema, fields_schema);
    return stat;
}

Status
DBImpl::DropCollection(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    if (options_.wal_enable_) {
        wal_mgr_->DropCollection(collection_id);
    }

    return DropCollectionRecursively(collection_id);
}

Status
DBImpl::DescribeCollection(meta::CollectionSchema& collection_schema) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    auto stat = meta_ptr_->DescribeCollection(collection_schema);
    collection_schema.index_file_size_ /= MB;  // return as MB
    return stat;
}

Status
DBImpl::HasCollection(const std::string& collection_id, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->HasCollection(collection_id, has_or_not, false);
}

Status
DBImpl::HasNativeCollection(const std::string& collection_id, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->HasCollection(collection_id, has_or_not, true);
}

Status
DBImpl::AllCollections(std::vector<meta::CollectionSchema>& collection_schema_array) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    std::vector<meta::CollectionSchema> all_collections;
    auto status = meta_ptr_->AllCollections(all_collections);

    // only return real collections, dont return partition collections
    collection_schema_array.clear();
    for (auto& schema : all_collections) {
        if (schema.owner_collection_.empty()) {
            collection_schema_array.push_back(schema);
        }
    }

    return status;
}

Status
DBImpl::GetCollectionInfo(const std::string& collection_id, std::string& collection_info) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step1: get all partition ids
    std::vector<meta::CollectionSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(collection_id, partition_array);

    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::INDEX};

    milvus::json json_info;
    milvus::json json_partitions;
    size_t total_row_count = 0;

    auto get_info = [&](const std::string& col_id, const std::string& tag) {
        meta::FilesHolder files_holder;
        status = meta_ptr_->FilesByType(col_id, file_types, files_holder);
        if (!status.ok()) {
            std::string err_msg = "Failed to get collection info: " + status.ToString();
            LOG_ENGINE_ERROR_ << err_msg;
            return Status(DB_ERROR, err_msg);
        }

        milvus::json json_partition;
        json_partition[JSON_PARTITION_TAG] = tag;

        milvus::json json_segments;
        size_t row_count = 0;
        milvus::engine::meta::SegmentsSchema& collection_files = files_holder.HoldFiles();
        for (auto& file : collection_files) {
            milvus::json json_segment;
            json_segment[JSON_SEGMENT_NAME] = file.segment_id_;
            json_segment[JSON_ROW_COUNT] = file.row_count_;
            json_segment[JSON_INDEX_NAME] = utils::GetIndexName(file.engine_type_);
            json_segment[JSON_DATA_SIZE] = (int64_t)file.file_size_;
            json_segments.push_back(json_segment);

            row_count += file.row_count_;
            total_row_count += file.row_count_;
        }

        json_partition[JSON_ROW_COUNT] = row_count;
        json_partition[JSON_SEGMENTS] = json_segments;

        json_partitions.push_back(json_partition);

        return Status::OK();
    };

    // step2: get default partition info
    status = get_info(collection_id, milvus::engine::DEFAULT_PARTITON_TAG);
    if (!status.ok()) {
        return status;
    }

    // step3: get partitions info
    for (auto& schema : partition_array) {
        status = get_info(schema.collection_id_, schema.partition_tag_);
        if (!status.ok()) {
            return status;
        }
    }

    json_info[JSON_ROW_COUNT] = total_row_count;
    json_info[JSON_PARTITIONS] = json_partitions;

    collection_info = json_info.dump();

    return Status::OK();
}

Status
DBImpl::PreloadCollection(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: get all collection files from parent collection
    meta::FilesHolder files_holder;
    auto status = meta_ptr_->FilesToSearch(collection_id, files_holder);
    if (!status.ok()) {
        return status;
    }

    // step 2: get files from partition collections
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = meta_ptr_->FilesToSearch(schema.collection_id_, files_holder);
    }

    int64_t size = 0;
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t available_size = cache_total - cache_usage;

    // step 3: load file one by one
    milvus::engine::meta::SegmentsSchema& files_array = files_holder.HoldFiles();
    LOG_ENGINE_DEBUG_ << "Begin pre-load collection:" + collection_id + ", totally " << files_array.size()
                      << " files need to be pre-loaded";
    TimeRecorderAuto rc("Pre-load collection:" + collection_id);
    for (auto& file : files_array) {
        EngineType engine_type;
        if (file.file_type_ == meta::SegmentSchema::FILE_TYPE::RAW ||
            file.file_type_ == meta::SegmentSchema::FILE_TYPE::TO_INDEX ||
            file.file_type_ == meta::SegmentSchema::FILE_TYPE::BACKUP) {
            engine_type =
                utils::IsBinaryMetricType(file.metric_type_) ? EngineType::FAISS_BIN_IDMAP : EngineType::FAISS_IDMAP;
        } else {
            engine_type = (EngineType)file.engine_type_;
        }

        auto json = milvus::json::parse(file.index_params_);
        ExecutionEnginePtr engine =
            EngineFactory::Build(file.dimension_, file.location_, engine_type, (MetricType)file.metric_type_, json);
        fiu_do_on("DBImpl.PreloadCollection.null_engine", engine = nullptr);
        if (engine == nullptr) {
            LOG_ENGINE_ERROR_ << "Invalid engine type";
            return Status(DB_ERROR, "Invalid engine type");
        }

        fiu_do_on("DBImpl.PreloadCollection.exceed_cache", size = available_size + 1);

        try {
            fiu_do_on("DBImpl.PreloadCollection.engine_throw_exception", throw std::exception());
            std::string msg = "Pre-loaded file: " + file.file_id_ + " size: " + std::to_string(file.file_size_);
            TimeRecorderAuto rc_1(msg);
            status = engine->Load(true);
            if (!status.ok()) {
                return status;
            }

            size += engine->Size();
            if (size > available_size) {
                LOG_ENGINE_DEBUG_ << "Pre-load cancelled since cache is almost full";
                return Status(SERVER_CACHE_FULL, "Cache is full");
            }
        } catch (std::exception& ex) {
            std::string msg = "Pre-load collection encounter exception: " + std::string(ex.what());
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }
    }

    return Status::OK();
}

Status
DBImpl::UpdateCollectionFlag(const std::string& collection_id, int64_t flag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->UpdateCollectionFlag(collection_id, flag);
}

Status
DBImpl::GetCollectionRowCount(const std::string& collection_id, uint64_t& row_count) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return GetCollectionRowCountRecursively(collection_id, row_count);
}

Status
DBImpl::CreatePartition(const std::string& collection_id, const std::string& partition_name,
                        const std::string& partition_tag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    uint64_t lsn = 0;
    meta_ptr_->GetCollectionFlushLSN(collection_id, lsn);
    return meta_ptr_->CreatePartition(collection_id, partition_name, partition_tag, lsn);
}

Status
DBImpl::HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // trim side-blank of tag, only compare valid characters
    // for example: " ab cd " is treated as "ab cd"
    std::string valid_tag = tag;
    server::StringHelpFunctions::TrimStringBlank(valid_tag);

    if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
        has_or_not = true;
        return Status::OK();
    }

    return meta_ptr_->HasPartition(collection_id, valid_tag, has_or_not);
}

Status
DBImpl::DropPartition(const std::string& partition_name) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    mem_mgr_->EraseMemVector(partition_name);                // not allow insert
    auto status = meta_ptr_->DropPartition(partition_name);  // soft delete collection
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    // scheduler will determine when to delete collection files
    auto nres = scheduler::ResMgrInst::GetInstance()->GetNumOfComputeResource();
    scheduler::DeleteJobPtr job = std::make_shared<scheduler::DeleteJob>(partition_name, meta_ptr_, nres);
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitAndDelete();

    return Status::OK();
}

Status
DBImpl::DropPartitionByTag(const std::string& collection_id, const std::string& partition_tag) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    std::string partition_name;
    auto status = meta_ptr_->GetPartitionName(collection_id, partition_tag, partition_name);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    return DropPartition(partition_name);
}

Status
DBImpl::ShowPartitions(const std::string& collection_id, std::vector<meta::CollectionSchema>& partition_schema_array) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->ShowPartitions(collection_id, partition_schema_array);
}

Status
DBImpl::InsertVectors(const std::string& collection_id, const std::string& partition_tag, VectorsData& vectors) {
    //    LOG_ENGINE_DEBUG_ << "Insert " << n << " vectors to cache";
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // insert vectors into target collection
    // (zhiru): generate ids
    if (vectors.id_array_.empty()) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        Status status = id_generator.GetNextIDNumbers(vectors.vector_count_, vectors.id_array_);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Get next id number fail: %s", "insert", 0, status.message().c_str());
            return status;
        }
    }

    Status status;
    if (options_.wal_enable_) {
        std::string target_collection_name;
        status = GetPartitionByTag(collection_id, partition_tag, target_collection_name);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Get partition fail: %s", "insert", 0, status.message().c_str());
            return status;
        }

        if (!vectors.float_data_.empty()) {
            wal_mgr_->Insert(collection_id, partition_tag, vectors.id_array_, vectors.float_data_);
        } else if (!vectors.binary_data_.empty()) {
            wal_mgr_->Insert(collection_id, partition_tag, vectors.id_array_, vectors.binary_data_);
        }
        swn_wal_.Notify();
    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.collection_id = collection_id;
        record.partition_tag = partition_tag;
        record.ids = vectors.id_array_.data();
        record.length = vectors.vector_count_;
        if (vectors.binary_data_.empty()) {
            record.type = wal::MXLogType::InsertVector;
            record.data = vectors.float_data_.data();
            record.data_size = vectors.float_data_.size() * sizeof(float);
        } else {
            record.type = wal::MXLogType::InsertBinary;
            record.ids = vectors.id_array_.data();
            record.length = vectors.vector_count_;
            record.data = vectors.binary_data_.data();
            record.data_size = vectors.binary_data_.size() * sizeof(uint8_t);
        }

        status = ExecWalRecord(record);
    }

    return status;
}

Status
CopyToAttr(std::vector<uint8_t>& record, uint64_t row_num, const std::vector<std::string>& field_names,
           std::unordered_map<std::string, meta::hybrid::DataType>& attr_types,
           std::unordered_map<std::string, std::vector<uint8_t>>& attr_datas,
           std::unordered_map<std::string, uint64_t>& attr_nbytes,
           std::unordered_map<std::string, uint64_t>& attr_data_size) {
    uint64_t offset = 0;
    for (auto name : field_names) {
        switch (attr_types.at(name)) {
            case meta::hybrid::DataType::INT8: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int8_t));

                std::vector<int64_t> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(int64_t));

                std::vector<int8_t> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(int8_t));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(int8_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int8_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::INT16: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int16_t));

                std::vector<int64_t> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(int64_t));

                std::vector<int16_t> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(int16_t));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(int16_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int16_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::INT32: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int32_t));

                std::vector<int64_t> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(int64_t));

                std::vector<int32_t> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(int32_t));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(int32_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int32_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::INT64: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(int64_t));
                memcpy(data.data(), record.data() + offset, row_num * sizeof(int64_t));
                attr_datas.insert(std::make_pair(name, data));

                std::vector<int64_t> test_data(row_num);
                memcpy(test_data.data(), record.data(), row_num * sizeof(int64_t));

                attr_nbytes.insert(std::make_pair(name, sizeof(int64_t)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(int64_t)));
                offset += row_num * sizeof(int64_t);
                break;
            }
            case meta::hybrid::DataType::FLOAT: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(float));

                std::vector<double> attr_value(row_num, 0);
                memcpy(attr_value.data(), record.data() + offset, row_num * sizeof(double));

                std::vector<float> raw_value(row_num, 0);
                for (uint64_t i = 0; i < row_num; ++i) {
                    raw_value[i] = attr_value[i];
                }

                memcpy(data.data(), raw_value.data(), row_num * sizeof(float));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(float)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(float)));
                offset += row_num * sizeof(double);
                break;
            }
            case meta::hybrid::DataType::DOUBLE: {
                std::vector<uint8_t> data;
                data.resize(row_num * sizeof(double));
                memcpy(data.data(), record.data() + offset, row_num * sizeof(double));
                attr_datas.insert(std::make_pair(name, data));

                attr_nbytes.insert(std::make_pair(name, sizeof(double)));
                attr_data_size.insert(std::make_pair(name, row_num * sizeof(double)));
                offset += row_num * sizeof(double);
                break;
            }
            default:
                break;
        }
    }
    return Status::OK();
}

Status
DBImpl::InsertEntities(const std::string& collection_id, const std::string& partition_tag,
                       const std::vector<std::string>& field_names, Entity& entity,
                       std::unordered_map<std::string, meta::hybrid::DataType>& attr_types) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // Generate id
    if (entity.id_array_.empty()) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        Status status = id_generator.GetNextIDNumbers(entity.entity_count_, entity.id_array_);
        if (!status.ok()) {
            return status;
        }
    }

    Status status;
    std::unordered_map<std::string, std::vector<uint8_t>> attr_data;
    std::unordered_map<std::string, uint64_t> attr_nbytes;
    std::unordered_map<std::string, uint64_t> attr_data_size;
    status = CopyToAttr(entity.attr_value_, entity.entity_count_, field_names, attr_types, attr_data, attr_nbytes,
                        attr_data_size);
    if (!status.ok()) {
        return status;
    }

    wal::MXLogRecord record;
    record.lsn = 0;
    record.collection_id = collection_id;
    record.partition_tag = partition_tag;
    record.ids = entity.id_array_.data();
    record.length = entity.entity_count_;

    auto vector_it = entity.vector_data_.begin();
    if (vector_it->second.binary_data_.empty()) {
        record.type = wal::MXLogType::Entity;
        record.data = vector_it->second.float_data_.data();
        record.data_size = vector_it->second.float_data_.size() * sizeof(float);
        record.attr_data = attr_data;
        record.attr_nbytes = attr_nbytes;
        record.attr_data_size = attr_data_size;
    } else {
        //        record.type = wal::MXLogType::InsertBinary;
        //        record.data = entities.vector_data_[0].binary_data_.data();
        //        record.length = entities.vector_data_[0].binary_data_.size() * sizeof(uint8_t);
    }

    status = ExecWalRecord(record);

#if 0
    if (options_.wal_enable_) {
        std::string target_collection_name;
        status = GetPartitionByTag(collection_id, partition_tag, target_collection_name);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Get partition fail: %s", "insert", 0, status.message().c_str());
            return status;
        }

        auto vector_it = entity.vector_data_.begin();
        if (!vector_it->second.binary_data_.empty()) {
            wal_mgr_->InsertEntities(collection_id, partition_tag, entity.id_array_, vector_it->second.binary_data_,
                                     attr_nbytes, attr_data);
        } else if (!vector_it->second.float_data_.empty()) {
            wal_mgr_->InsertEntities(collection_id, partition_tag, entity.id_array_, vector_it->second.float_data_,
                                     attr_nbytes, attr_data);
        }
        swn_wal_.Notify();
    } else {
        // insert entities: collection_name is field id
        wal::MXLogRecord record;
        record.lsn = 0;
        record.collection_id = collection_id;
        record.partition_tag = partition_tag;
        record.ids = entity.id_array_.data();
        record.length = entity.entity_count_;

        auto vector_it = entity.vector_data_.begin();
        if (vector_it->second.binary_data_.empty()) {
            record.type = wal::MXLogType::Entity;
            record.data = vector_it->second.float_data_.data();
            record.data_size = vector_it->second.float_data_.size() * sizeof(float);
            record.attr_data = attr_data;
            record.attr_nbytes = attr_nbytes;
            record.attr_data_size = attr_data_size;
        } else {
            //        record.type = wal::MXLogType::InsertBinary;
            //        record.data = entities.vector_data_[0].binary_data_.data();
            //        record.length = entities.vector_data_[0].binary_data_.size() * sizeof(uint8_t);
        }

        status = ExecWalRecord(record);
    }
#endif

    return status;
}

Status
DBImpl::DeleteVector(const std::string& collection_id, IDNumber vector_id) {
    IDNumbers ids;
    ids.push_back(vector_id);
    return DeleteVectors(collection_id, ids);
}

Status
DBImpl::DeleteVectors(const std::string& collection_id, IDNumbers vector_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    if (options_.wal_enable_) {
        wal_mgr_->DeleteById(collection_id, vector_ids);
        swn_wal_.Notify();
    } else {
        wal::MXLogRecord record;
        record.lsn = 0;  // need to get from meta ?
        record.type = wal::MXLogType::Delete;
        record.collection_id = collection_id;
        record.ids = vector_ids.data();
        record.length = vector_ids.size();

        status = ExecWalRecord(record);
    }

    return status;
}

Status
DBImpl::Flush(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    bool has_collection;
    status = HasCollection(collection_id, has_collection);
    if (!status.ok()) {
        return status;
    }
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection to flush does not exist: " << collection_id;
        return Status(DB_NOT_FOUND, "Collection to flush does not exist");
    }

    LOG_ENGINE_DEBUG_ << "Begin flush collection: " << collection_id;

    if (options_.wal_enable_) {
        LOG_ENGINE_DEBUG_ << "WAL flush";
        auto lsn = wal_mgr_->Flush(collection_id);
        if (lsn != 0) {
            swn_wal_.Notify();
            flush_req_swn_.Wait();
        }

    } else {
        LOG_ENGINE_DEBUG_ << "MemTable flush";
        InternalFlush(collection_id);
    }

    LOG_ENGINE_DEBUG_ << "End flush collection: " << collection_id;

    return status;
}

Status
DBImpl::Flush() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Begin flush all collections";

    Status status;
    if (options_.wal_enable_) {
        LOG_ENGINE_DEBUG_ << "WAL flush";
        auto lsn = wal_mgr_->Flush();
        if (lsn != 0) {
            swn_wal_.Notify();
            flush_req_swn_.Wait();
        }
    } else {
        LOG_ENGINE_DEBUG_ << "MemTable flush";
        InternalFlush();
    }

    LOG_ENGINE_DEBUG_ << "End flush all collections";

    return status;
}

Status
DBImpl::Compact(const std::string& collection_id, double threshold) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_id;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            LOG_ENGINE_ERROR_ << "Collection to compact does not exist: " << collection_id;
            return Status(DB_NOT_FOUND, "Collection to compact does not exist");
        } else {
            return status;
        }
    } else {
        if (!collection_schema.owner_collection_.empty()) {
            LOG_ENGINE_ERROR_ << "Collection to compact does not exist: " << collection_id;
            return Status(DB_NOT_FOUND, "Collection to compact does not exist");
        }
    }

    LOG_ENGINE_DEBUG_ << "Before compacting, wait for build index thread to finish...";

    // WaitBuildIndexFinish();

    const std::lock_guard<std::mutex> index_lock(build_index_mutex_);
    const std::lock_guard<std::mutex> merge_lock(flush_merge_compact_mutex_);

    LOG_ENGINE_DEBUG_ << "Compacting collection: " << collection_id;

    // Get files to compact from meta.
    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};
    meta::FilesHolder files_holder;
    status = meta_ptr_->FilesByType(collection_id, file_types, files_holder);
    if (!status.ok()) {
        std::string err_msg = "Failed to get files to compact: " + status.message();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    LOG_ENGINE_DEBUG_ << "Found " << files_holder.HoldFiles().size() << " segment to compact";

    Status compact_status;
    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files_to_compact = files_holder.HoldFiles();
    for (auto iter = files_to_compact.begin(); iter != files_to_compact.end();) {
        meta::SegmentSchema file = *iter;
        iter = files_to_compact.erase(iter);

        // Check if the segment needs compacting
        std::string segment_dir;
        utils::GetParentPath(file.location_, segment_dir);

        segment::SegmentReader segment_reader(segment_dir);
        size_t deleted_docs_size;
        status = segment_reader.ReadDeletedDocsSize(deleted_docs_size);
        if (!status.ok()) {
            files_holder.UnmarkFile(file);
            continue;  // skip this file and try compact next one
        }

        meta::SegmentsSchema files_to_update;
        if (deleted_docs_size != 0) {
            compact_status = CompactFile(collection_id, threshold, file, files_to_update);

            if (!compact_status.ok()) {
                LOG_ENGINE_ERROR_ << "Compact failed for segment " << file.segment_id_ << ": "
                                  << compact_status.message();
                files_holder.UnmarkFile(file);
                continue;  // skip this file and try compact next one
            }
        } else {
            files_holder.UnmarkFile(file);
            LOG_ENGINE_DEBUG_ << "Segment " << file.segment_id_ << " has no deleted data. No need to compact";
            continue;  // skip this file and try compact next one
        }

        LOG_ENGINE_DEBUG_ << "Updating meta after compaction...";
        status = meta_ptr_->UpdateCollectionFiles(files_to_update);
        files_holder.UnmarkFile(file);
        if (!status.ok()) {
            compact_status = status;
            break;  // meta error, could not go on
        }
    }

    if (compact_status.ok()) {
        LOG_ENGINE_DEBUG_ << "Finished compacting collection: " << collection_id;
    }

    return compact_status;
}

Status
DBImpl::CompactFile(const std::string& collection_id, double threshold, const meta::SegmentSchema& file,
                    meta::SegmentsSchema& files_to_update) {
    LOG_ENGINE_DEBUG_ << "Compacting segment " << file.segment_id_ << " for collection: " << collection_id;

    std::string segment_dir_to_merge;
    utils::GetParentPath(file.location_, segment_dir_to_merge);

    // no need to compact if deleted vectors are too few(less than threashold)
    if (file.row_count_ > 0 && threshold > 0.0) {
        segment::SegmentReader segment_reader_to_merge(segment_dir_to_merge);
        segment::DeletedDocsPtr deleted_docs_ptr;
        auto status = segment_reader_to_merge.LoadDeletedDocs(deleted_docs_ptr);
        if (status.ok()) {
            auto delete_items = deleted_docs_ptr->GetDeletedDocs();
            double delete_rate = (double)delete_items.size() / (double)file.row_count_;
            if (delete_rate < threshold) {
                LOG_ENGINE_DEBUG_ << "Delete rate less than " << threshold << ", no need to compact for"
                                  << segment_dir_to_merge;
                return Status::OK();
            }
        }
    }

    // Create new collection file
    meta::SegmentSchema compacted_file;
    compacted_file.collection_id_ = collection_id;
    // compacted_file.date_ = date;
    compacted_file.file_type_ = meta::SegmentSchema::NEW_MERGE;  // TODO: use NEW_MERGE for now
    auto status = meta_ptr_->CreateCollectionFile(compacted_file);

    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to create collection file: " << status.message();
        return status;
    }

    // Compact (merge) file to the newly created collection file

    std::string new_segment_dir;
    utils::GetParentPath(compacted_file.location_, new_segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);

    LOG_ENGINE_DEBUG_ << "Compacting begin...";
    segment_writer_ptr->Merge(segment_dir_to_merge, compacted_file.file_id_);

    // Serialize
    LOG_ENGINE_DEBUG_ << "Serializing compacted segment...";
    status = segment_writer_ptr->Serialize();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to serialize compacted segment: " << status.message();
        compacted_file.file_type_ = meta::SegmentSchema::TO_DELETE;
        auto mark_status = meta_ptr_->UpdateCollectionFile(compacted_file);
        if (mark_status.ok()) {
            LOG_ENGINE_DEBUG_ << "Mark file: " << compacted_file.file_id_ << " to to_delete";
        }

        return status;
    }

    // Update compacted file state, if origin file is backup or to_index, set compected file to to_index
    compacted_file.file_size_ = segment_writer_ptr->Size();
    compacted_file.row_count_ = segment_writer_ptr->VectorCount();
    if ((file.file_type_ == (int32_t)meta::SegmentSchema::BACKUP ||
         file.file_type_ == (int32_t)meta::SegmentSchema::TO_INDEX) &&
        (compacted_file.row_count_ > meta::BUILD_INDEX_THRESHOLD)) {
        compacted_file.file_type_ = meta::SegmentSchema::TO_INDEX;
    } else {
        compacted_file.file_type_ = meta::SegmentSchema::RAW;
    }

    if (compacted_file.row_count_ == 0) {
        LOG_ENGINE_DEBUG_ << "Compacted segment is empty. Mark it as TO_DELETE";
        compacted_file.file_type_ = meta::SegmentSchema::TO_DELETE;
    }

    files_to_update.emplace_back(compacted_file);

    // Set all files in segment to TO_DELETE
    auto& segment_id = file.segment_id_;
    meta::FilesHolder files_holder;
    status = meta_ptr_->GetCollectionFilesBySegmentId(segment_id, files_holder);
    if (!status.ok()) {
        return status;
    }

    milvus::engine::meta::SegmentsSchema& segment_files = files_holder.HoldFiles();
    for (auto& f : segment_files) {
        f.file_type_ = meta::SegmentSchema::FILE_TYPE::TO_DELETE;
        files_to_update.emplace_back(f);
    }
    files_holder.ReleaseFiles();

    LOG_ENGINE_DEBUG_ << "Compacted segment " << compacted_file.segment_id_ << " from "
                      << std::to_string(file.file_size_) << " bytes to " << std::to_string(compacted_file.file_size_)
                      << " bytes";

    if (options_.insert_cache_immediately_) {
        segment_writer_ptr->Cache();
    }

    return status;
}

Status
DBImpl::GetVectorsByID(const std::string& collection_id, const IDNumbers& id_array,
                       std::vector<engine::VectorsData>& vectors) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    bool has_collection;
    auto status = HasCollection(collection_id, has_collection);
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection " << collection_id << " does not exist: ";
        return Status(DB_NOT_FOUND, "Collection does not exist");
    }
    if (!status.ok()) {
        return status;
    }

    meta::FilesHolder files_holder;
    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};

    status = meta_ptr_->FilesByType(collection_id, file_types, files_holder);
    if (!status.ok()) {
        std::string err_msg = "Failed to get files for GetVectorsByID: " + status.message();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = meta_ptr_->FilesByType(schema.collection_id_, file_types, files_holder);
        if (!status.ok()) {
            std::string err_msg = "Failed to get files for GetVectorByID: " + status.message();
            LOG_ENGINE_ERROR_ << err_msg;
            return status;
        }
    }

    if (files_holder.HoldFiles().empty()) {
        LOG_ENGINE_DEBUG_ << "No files to get vector by id from";
        return Status(DB_NOT_FOUND, "Collection is empty");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();
    status = GetVectorsByIdHelper(collection_id, id_array, vectors, files_holder);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();

    return status;
}

Status
DBImpl::GetVectorIDs(const std::string& collection_id, const std::string& segment_id, IDNumbers& vector_ids) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // step 1: check collection existence
    bool has_collection;
    auto status = HasCollection(collection_id, has_collection);
    if (!has_collection) {
        LOG_ENGINE_ERROR_ << "Collection " << collection_id << " does not exist: ";
        return Status(DB_NOT_FOUND, "Collection does not exist");
    }
    if (!status.ok()) {
        return status;
    }

    //  step 2: find segment
    meta::FilesHolder files_holder;
    status = meta_ptr_->GetCollectionFilesBySegmentId(segment_id, files_holder);
    if (!status.ok()) {
        return status;
    }

    milvus::engine::meta::SegmentsSchema& collection_files = files_holder.HoldFiles();
    if (collection_files.empty()) {
        return Status(DB_NOT_FOUND, "Segment does not exist");
    }

    // check the segment is belong to this collection
    if (collection_files[0].collection_id_ != collection_id) {
        // the segment could be in a partition under this collection
        meta::CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_files[0].collection_id_;
        status = DescribeCollection(collection_schema);
        if (collection_schema.owner_collection_ != collection_id) {
            return Status(DB_NOT_FOUND, "Segment does not belong to this collection");
        }
    }

    // step 3: load segment ids and delete offset
    std::string segment_dir;
    engine::utils::GetParentPath(collection_files[0].location_, segment_dir);
    segment::SegmentReader segment_reader(segment_dir);

    std::vector<segment::doc_id_t> uids;
    status = segment_reader.LoadUids(uids);
    if (!status.ok()) {
        return status;
    }

    segment::DeletedDocsPtr deleted_docs_ptr;
    status = segment_reader.LoadDeletedDocs(deleted_docs_ptr);
    if (!status.ok()) {
        return status;
    }

    // step 4: construct id array
    // avoid duplicate offset and erase from max offset to min offset
    auto& deleted_offset = deleted_docs_ptr->GetDeletedDocs();
    std::set<segment::offset_t, std::greater<segment::offset_t>> ordered_offset;
    for (segment::offset_t offset : deleted_offset) {
        ordered_offset.insert(offset);
    }
    for (segment::offset_t offset : ordered_offset) {
        uids.erase(uids.begin() + offset);
    }
    vector_ids.swap(uids);

    return status;
}

Status
DBImpl::GetVectorsByIdHelper(const std::string& collection_id, const IDNumbers& id_array,
                             std::vector<engine::VectorsData>& vectors, meta::FilesHolder& files_holder) {
    // attention: this is a copy, not a reference, since the files_holder.UnMarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    LOG_ENGINE_DEBUG_ << "Getting vector by id in " << files.size() << " files, id count = " << id_array.size();

    // sometimes not all of id_array can be found, we need to return empty vector for id not found
    // for example:
    // id_array = [1, -1, 2, -1, 3]
    // vectors should return [valid_vector, empty_vector, valid_vector, empty_vector, valid_vector]
    // the ID2RAW is to ensure returned vector sequence is consist with id_array
    using ID2VECTOR = std::map<int64_t, VectorsData>;
    ID2VECTOR map_id2vector;

    vectors.clear();

    IDNumbers temp_ids = id_array;
    for (auto& file : files) {
        // Load bloom filter
        std::string segment_dir;
        engine::utils::GetParentPath(file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        segment_reader.LoadBloomFilter(id_bloom_filter_ptr);

        for (IDNumbers::iterator it = temp_ids.begin(); it != temp_ids.end();) {
            int64_t vector_id = *it;
            // each id must has a VectorsData
            // if vector not found for an id, its VectorsData's vector_count = 0, else 1
            VectorsData& vector_ref = map_id2vector[vector_id];

            // Check if the id is present in bloom filter.
            if (id_bloom_filter_ptr->Check(vector_id)) {
                // Load uids and check if the id is indeed present. If yes, find its offset.
                std::vector<segment::doc_id_t> uids;
                auto status = segment_reader.LoadUids(uids);
                if (!status.ok()) {
                    return status;
                }

                auto found = std::find(uids.begin(), uids.end(), vector_id);
                if (found != uids.end()) {
                    auto offset = std::distance(uids.begin(), found);

                    // Check whether the id has been deleted
                    segment::DeletedDocsPtr deleted_docs_ptr;
                    status = segment_reader.LoadDeletedDocs(deleted_docs_ptr);
                    if (!status.ok()) {
                        LOG_ENGINE_ERROR_ << status.message();
                        return status;
                    }
                    auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();

                    auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
                    if (deleted == deleted_docs.end()) {
                        // Load raw vector
                        bool is_binary = utils::IsBinaryMetricType(file.metric_type_);
                        size_t single_vector_bytes = is_binary ? file.dimension_ / 8 : file.dimension_ * sizeof(float);
                        std::vector<uint8_t> raw_vector;
                        status =
                            segment_reader.LoadVectors(offset * single_vector_bytes, single_vector_bytes, raw_vector);
                        if (!status.ok()) {
                            LOG_ENGINE_ERROR_ << status.message();
                            return status;
                        }

                        vector_ref.vector_count_ = 1;
                        if (is_binary) {
                            vector_ref.binary_data_.swap(raw_vector);
                        } else {
                            std::vector<float> float_vector;
                            float_vector.resize(file.dimension_);
                            memcpy(float_vector.data(), raw_vector.data(), single_vector_bytes);
                            vector_ref.float_data_.swap(float_vector);
                        }
                        temp_ids.erase(it);
                        continue;
                    }
                }
            }

            it++;
        }

        // unmark file, allow the file to be deleted
        files_holder.UnmarkFile(file);
    }

    for (auto id : id_array) {
        VectorsData& vector_ref = map_id2vector[id];

        VectorsData data;
        data.vector_count_ = vector_ref.vector_count_;
        if (data.vector_count_ > 0) {
            data.float_data_ = vector_ref.float_data_;    // copy data since there could be duplicated id
            data.binary_data_ = vector_ref.binary_data_;  // copy data since there could be duplicated id
        }
        vectors.emplace_back(data);
    }

    if (vectors.empty()) {
        std::string msg = "Vectors not found in collection " + collection_id;
        LOG_ENGINE_DEBUG_ << msg;
    }

    return Status::OK();
}

Status
DBImpl::CreateIndex(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                    const CollectionIndex& index) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // serialize memory data
    //    std::set<std::string> sync_collection_ids;
    //    auto status = SyncMemData(sync_collection_ids);
    auto status = Flush();

    {
        std::unique_lock<std::mutex> lock(build_index_mutex_);

        // step 1: check index difference
        CollectionIndex old_index;
        status = DescribeIndex(collection_id, old_index);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get collection index info for collection: " << collection_id;
            return status;
        }

        // step 2: update index info
        CollectionIndex new_index = index;
        new_index.metric_type_ = old_index.metric_type_;  // dont change metric type, it was defined by CreateCollection
        if (!utils::IsSameIndex(old_index, new_index)) {
            status = UpdateCollectionIndexRecursively(collection_id, new_index);
            if (!status.ok()) {
                return status;
            }
        }
    }

    // step 3: let merge file thread finish
    // to avoid duplicate data bug
    WaitMergeFileFinish();

    // step 4: wait and build index
    status = index_failed_checker_.CleanFailedIndexFileOfCollection(collection_id);
    status = WaitCollectionIndexRecursively(context, collection_id, index);

    return status;
}

Status
DBImpl::DescribeIndex(const std::string& collection_id, CollectionIndex& index) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->DescribeCollectionIndex(collection_id, index);
}

Status
DBImpl::DropIndex(const std::string& collection_id) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_id;
    return DropCollectionIndexRecursively(collection_id);
}

Status
DBImpl::QueryByIDs(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                   const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
                   const IDNumbers& id_array, ResultIds& result_ids, ResultDistances& result_distances) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    if (id_array.empty()) {
        return Status(DB_ERROR, "Empty id array during query by id");
    }

    TimeRecorder rc("Query by id in collection:" + collection_id);

    // get collection schema
    engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_id;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            std::string msg = "Collection to search does not exist: " + collection_id;
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_NOT_FOUND, msg);
        } else {
            return status;
        }
    } else {
        if (!collection_schema.owner_collection_.empty()) {
            std::string msg = "Collection to search does not exist: " + collection_id;
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_NOT_FOUND, msg);
        }
    }

    rc.RecordSection("get collection schema");

    // get target vectors data
    std::vector<milvus::engine::VectorsData> vectors;
    status = GetVectorsByID(collection_id, id_array, vectors);
    if (!status.ok()) {
        std::string msg = "Failed to get vector data for collection: " + collection_id;
        LOG_ENGINE_ERROR_ << msg;
        return status;
    }

    // some vectors could not be found, no need to search them
    uint64_t valid_count = 0;
    bool is_binary = utils::IsBinaryMetricType(collection_schema.metric_type_);
    for (auto& vector : vectors) {
        if (vector.vector_count_ > 0) {
            valid_count++;
        }
    }

    // copy valid vectors data for search input
    uint64_t dimension = collection_schema.dimension_;
    VectorsData valid_vectors;
    valid_vectors.vector_count_ = valid_count;
    if (is_binary) {
        valid_vectors.binary_data_.resize(valid_count * dimension / 8);
    } else {
        valid_vectors.float_data_.resize(valid_count * dimension * sizeof(float));
    }

    int64_t valid_index = 0;
    for (size_t i = 0; i < vectors.size(); i++) {
        if (vectors[i].vector_count_ == 0) {
            continue;
        }
        if (is_binary) {
            memcpy(valid_vectors.binary_data_.data() + valid_index * dimension / 8, vectors[i].binary_data_.data(),
                   vectors[i].binary_data_.size());
        } else {
            memcpy(valid_vectors.float_data_.data() + valid_index * dimension, vectors[i].float_data_.data(),
                   vectors[i].float_data_.size() * sizeof(float));
        }
        valid_index++;
    }

    rc.RecordSection("construct query input");

    // search valid vectors
    ResultIds valid_result_ids;
    ResultDistances valid_result_distances;
    status = Query(context, collection_id, partition_tags, k, extra_params, valid_vectors, valid_result_ids,
                   valid_result_distances);
    if (!status.ok()) {
        std::string msg = "Failed to query by id in collection " + collection_id + ", error: " + status.message();
        LOG_ENGINE_ERROR_ << msg;
        return status;
    }

    if (valid_result_ids.size() != valid_count * k || valid_result_distances.size() != valid_count * k) {
        std::string msg = "Failed to query by id in collection " + collection_id + ", result doesn't match id count";
        return Status(DB_ERROR, msg);
    }

    rc.RecordSection("query vealid vectors");

    // construct result
    if (valid_count == id_array.size()) {
        result_ids.swap(valid_result_ids);
        result_distances.swap(valid_result_distances);
    } else {
        result_ids.resize(vectors.size() * k);
        result_distances.resize(vectors.size() * k);
        int64_t valid_index = 0;
        for (uint64_t i = 0; i < vectors.size(); i++) {
            if (vectors[i].vector_count_ > 0) {
                memcpy(result_ids.data() + i * k, valid_result_ids.data() + valid_index * k, k * sizeof(int64_t));
                memcpy(result_distances.data() + i * k, valid_result_distances.data() + valid_index * k,
                       k * sizeof(float));
                valid_index++;
            } else {
                memset(result_ids.data() + i * k, -1, k * sizeof(int64_t));
                for (uint64_t j = i * k; j < i * k + k; j++) {
                    result_distances[j] = std::numeric_limits<float>::max();
                }
            }
        }
    }

    rc.RecordSection("construct result");

    return status;
}

Status
DBImpl::HybridQuery(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
                    const std::vector<std::string>& partition_tags,
                    context::HybridSearchContextPtr hybrid_search_context, query::GeneralQueryPtr general_query,
                    std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type, uint64_t& nq,
                    ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_ctx = context->Child("Query");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    meta::FilesHolder files_holder;
    if (partition_tags.empty()) {
        // no partition tag specified, means search in whole table
        // get all table files from parent table
        status = meta_ptr_->FilesToSearch(collection_id, files_holder);
        if (!status.ok()) {
            return status;
        }

        std::vector<meta::CollectionSchema> partition_array;
        status = meta_ptr_->ShowPartitions(collection_id, partition_array);
        if (!status.ok()) {
            return status;
        }
        for (auto& schema : partition_array) {
            status = meta_ptr_->FilesToSearch(schema.collection_id_, files_holder);
            if (!status.ok()) {
                return Status(DB_ERROR, "get files to search failed in HybridQuery");
            }
        }

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();  // no files to search
        }
    } else {
        // get files from specified partitions
        std::set<std::string> partition_name_array;
        GetPartitionsByTags(collection_id, partition_tags, partition_name_array);

        for (auto& partition_name : partition_name_array) {
            status = meta_ptr_->FilesToSearch(partition_name, files_holder);
            if (!status.ok()) {
                return Status(DB_ERROR, "get files to search failed in HybridQuery");
            }
        }

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = HybridQueryAsync(query_ctx, collection_id, files_holder, hybrid_search_context, general_query, attr_type,
                              nq, result_ids, result_distances);
    if (!status.ok()) {
        return status;
    }
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    query_ctx->GetTraceContext()->GetSpan()->Finish();

    return status;
}

Status
DBImpl::Query(const std::shared_ptr<server::Context>& context, const std::string& collection_id,
              const std::vector<std::string>& partition_tags, uint64_t k, const milvus::json& extra_params,
              const VectorsData& vectors, ResultIds& result_ids, ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    Status status;
    meta::FilesHolder files_holder;
    if (partition_tags.empty()) {
        // no partition tag specified, means search in whole collection
        // get all collection files from parent collection
        status = meta_ptr_->FilesToSearch(collection_id, files_holder);
        if (!status.ok()) {
            return status;
        }

        std::vector<meta::CollectionSchema> partition_array;
        status = meta_ptr_->ShowPartitions(collection_id, partition_array);
        for (auto& schema : partition_array) {
            status = meta_ptr_->FilesToSearch(schema.collection_id_, files_holder);
        }

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();  // no files to search
        }
    } else {
        // get files from specified partitions
        std::set<std::string> partition_name_array;
        status = GetPartitionsByTags(collection_id, partition_tags, partition_name_array);
        if (!status.ok()) {
            return status;  // didn't match any partition.
        }

        for (auto& partition_name : partition_name_array) {
            status = meta_ptr_->FilesToSearch(partition_name, files_holder);
        }

        if (files_holder.HoldFiles().empty()) {
            return Status::OK();  // no files to search
        }
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(tracer.Context(), files_holder, k, extra_params, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    return status;
}

Status
DBImpl::QueryByFileID(const std::shared_ptr<server::Context>& context, const std::vector<std::string>& file_ids,
                      uint64_t k, const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
                      ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query by file id");

    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    // get specified files
    std::vector<size_t> ids;
    for (auto& id : file_ids) {
        std::string::size_type sz;
        ids.push_back(std::stoul(id, &sz));
    }

    meta::FilesHolder files_holder;
    auto status = meta_ptr_->FilesByID(ids, files_holder);
    if (!status.ok()) {
        return status;
    }

    milvus::engine::meta::SegmentsSchema& search_files = files_holder.HoldFiles();
    if (search_files.empty()) {
        return Status(DB_ERROR, "Invalid file id");
    }

    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info before query
    status = QueryAsync(tracer.Context(), files_holder, k, extra_params, vectors, result_ids, result_distances);
    cache::CpuCacheMgr::GetInstance()->PrintInfo();  // print cache info after query

    return status;
}

Status
DBImpl::Size(uint64_t& result) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return SHUTDOWN_ERROR;
    }

    return meta_ptr_->Size(result);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// internal methods
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Status
DBImpl::QueryAsync(const std::shared_ptr<server::Context>& context, meta::FilesHolder& files_holder, uint64_t k,
                   const milvus::json& extra_params, const VectorsData& vectors, ResultIds& result_ids,
                   ResultDistances& result_distances) {
    milvus::server::ContextChild tracer(context, "Query Async");
    server::CollectQueryMetrics metrics(vectors.vector_count_);

    milvus::engine::meta::SegmentsSchema& files = files_holder.HoldFiles();
    if (files.size() > milvus::scheduler::TASK_TABLE_MAX_COUNT) {
        std::string msg =
            "Search files count exceed scheduler limit: " + std::to_string(milvus::scheduler::TASK_TABLE_MAX_COUNT);
        LOG_ENGINE_ERROR_ << msg;
        return Status(DB_ERROR, msg);
    }

    TimeRecorder rc("");

    // step 1: construct search job
    LOG_ENGINE_DEBUG_ << LogOut("Engine query begin, index file count: %ld", files.size());
    scheduler::SearchJobPtr job = std::make_shared<scheduler::SearchJob>(tracer.Context(), k, extra_params, vectors);
    for (auto& file : files) {
        scheduler::SegmentSchemaPtr file_ptr = std::make_shared<meta::SegmentSchema>(file);
        job->AddIndexFile(file_ptr);
    }

    // Suspend builder
    SuspendIfFirst();

    // step 2: put search job to scheduler and wait result
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitResult();

    // Resume builder
    ResumeIfLast();

    files_holder.ReleaseFiles();
    if (!job->GetStatus().ok()) {
        return job->GetStatus();
    }

    // step 3: construct results
    result_ids = job->GetResultIds();
    result_distances = job->GetResultDistances();
    rc.ElapseFromBegin("Engine query totally cost");

    return Status::OK();
}

Status
DBImpl::HybridQueryAsync(const std::shared_ptr<server::Context>& context, const std::string& table_id,
                         meta::FilesHolder& files_holder, context::HybridSearchContextPtr hybrid_search_context,
                         query::GeneralQueryPtr general_query,
                         std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type, uint64_t& nq,
                         ResultIds& result_ids, ResultDistances& result_distances) {
    auto query_async_ctx = context->Child("Query Async");

#if 0
    // Construct tasks
    for (auto file : files) {
        std::unordered_map<std::string, engine::DataType> types;
        auto it = attr_type.begin();
        for (; it != attr_type.end(); it++) {
            types.insert(std::make_pair(it->first, (engine::DataType)it->second));
        }

        auto file_ptr = std::make_shared<meta::TableFileSchema>(file);
        search::TaskPtr
            task = std::make_shared<search::Task>(context, file_ptr, general_query, types, hybrid_search_context);
        search::TaskInst::GetInstance().load_queue().push(task);
        search::TaskInst::GetInstance().load_cv().notify_one();
        hybrid_search_context->tasks_.emplace_back(task);
    }

#endif

    //#if 0
    TimeRecorder rc("");

    // step 1: construct search job
    VectorsData vectors;
    milvus::engine::meta::SegmentsSchema& files = files_holder.HoldFiles();
    LOG_ENGINE_DEBUG_ << LogOut("Engine query begin, index file count: %ld", files_holder.HoldFiles().size());
    scheduler::SearchJobPtr job =
        std::make_shared<scheduler::SearchJob>(query_async_ctx, general_query, attr_type, vectors);
    for (auto& file : files) {
        scheduler::SegmentSchemaPtr file_ptr = std::make_shared<meta::SegmentSchema>(file);
        job->AddIndexFile(file_ptr);
    }

    // step 2: put search job to scheduler and wait result
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitResult();

    files_holder.ReleaseFiles();
    if (!job->GetStatus().ok()) {
        return job->GetStatus();
    }

    // step 3: construct results
    nq = job->vector_count();
    result_ids = job->GetResultIds();
    result_distances = job->GetResultDistances();
    rc.ElapseFromBegin("Engine query totally cost");

    query_async_ctx->GetTraceContext()->GetSpan()->Finish();
    //#endif

    return Status::OK();
}

void
DBImpl::BackgroundIndexThread() {
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            WaitMergeFileFinish();
            WaitBuildIndexFinish();

            LOG_ENGINE_DEBUG_ << "DB background thread exit";
            break;
        }

        swn_index_.Wait_For(std::chrono::seconds(BACKGROUND_INDEX_INTERVAL));

        WaitMergeFileFinish();
        StartBuildIndexTask();
    }
}

void
DBImpl::WaitMergeFileFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitMergeFileFinish";
    std::lock_guard<std::mutex> lck(merge_result_mutex_);
    for (auto& iter : merge_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitMergeFileFinish";
}

void
DBImpl::WaitBuildIndexFinish() {
    //    LOG_ENGINE_DEBUG_ << "Begin WaitBuildIndexFinish";
    std::lock_guard<std::mutex> lck(index_result_mutex_);
    for (auto& iter : index_thread_results_) {
        iter.wait();
    }
    //    LOG_ENGINE_DEBUG_ << "End WaitBuildIndexFinish";
}

void
DBImpl::StartMetricTask() {
    server::Metrics::GetInstance().KeepingAliveCounterIncrement(BACKGROUND_METRIC_INTERVAL);
    int64_t cache_usage = cache::CpuCacheMgr::GetInstance()->CacheUsage();
    int64_t cache_total = cache::CpuCacheMgr::GetInstance()->CacheCapacity();
    fiu_do_on("DBImpl.StartMetricTask.InvalidTotalCache", cache_total = 0);

    if (cache_total > 0) {
        double cache_usage_double = cache_usage;
        server::Metrics::GetInstance().CpuCacheUsageGaugeSet(cache_usage_double * 100 / cache_total);
    } else {
        server::Metrics::GetInstance().CpuCacheUsageGaugeSet(0);
    }

    server::Metrics::GetInstance().GpuCacheUsageGaugeSet();
    uint64_t size;
    Size(size);
    server::Metrics::GetInstance().DataFileSizeGaugeSet(size);
    server::Metrics::GetInstance().CPUUsagePercentSet();
    server::Metrics::GetInstance().RAMUsagePercentSet();
    server::Metrics::GetInstance().GPUPercentGaugeSet();
    server::Metrics::GetInstance().GPUMemoryUsageGaugeSet();
    server::Metrics::GetInstance().OctetsSet();

    server::Metrics::GetInstance().CPUCoreUsagePercentSet();
    server::Metrics::GetInstance().GPUTemperature();
    server::Metrics::GetInstance().CPUTemperature();
    server::Metrics::GetInstance().PushToGateway();
}

void
DBImpl::StartMergeTask() {
    // LOG_ENGINE_DEBUG_ << "Begin StartMergeTask";
    // merge task has been finished?
    {
        std::lock_guard<std::mutex> lck(merge_result_mutex_);
        if (!merge_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (merge_thread_results_.back().wait_for(span) == std::future_status::ready) {
                merge_thread_results_.pop_back();
            }
        }
    }

    // add new merge task
    {
        std::lock_guard<std::mutex> lck(merge_result_mutex_);
        if (merge_thread_results_.empty()) {
            // collect merge files for all collections(if merge_collection_ids_ is empty) for two reasons:
            // 1. other collections may still has un-merged files
            // 2. server may be closed unexpected, these un-merge files need to be merged when server restart
            if (merge_collection_ids_.empty()) {
                std::vector<meta::CollectionSchema> collection_schema_array;
                meta_ptr_->AllCollections(collection_schema_array);
                for (auto& schema : collection_schema_array) {
                    merge_collection_ids_.insert(schema.collection_id_);
                }
            }

            // start merge file thread
            merge_thread_results_.push_back(
                merge_thread_pool_.enqueue(&DBImpl::BackgroundMerge, this, merge_collection_ids_));
            merge_collection_ids_.clear();
        }
    }

    // LOG_ENGINE_DEBUG_ << "End StartMergeTask";
}

Status
DBImpl::MergeHybridFiles(const std::string& collection_id, meta::FilesHolder& files_holder) {
    // const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

    LOG_ENGINE_DEBUG_ << "Merge files for collection: " << collection_id;

    // step 1: create table file
    meta::SegmentSchema table_file;
    table_file.collection_id_ = collection_id;
    table_file.file_type_ = meta::SegmentSchema::NEW_MERGE;
    Status status = meta_ptr_->CreateHybridCollectionFile(table_file);

    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to create collection: " << status.ToString();
        return status;
    }

    // step 2: merge files
    /*
    ExecutionEnginePtr index =
        EngineFactory::Build(table_file.dimension_, table_file.location_, (EngineType)table_file.engine_type_,
                             (MetricType)table_file.metric_type_, table_file.nlist_);
*/
    meta::SegmentsSchema updated;

    std::string new_segment_dir;
    utils::GetParentPath(table_file.location_, new_segment_dir);
    auto segment_writer_ptr = std::make_shared<segment::SegmentWriter>(new_segment_dir);

    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    for (auto& file : files) {
        server::CollectMergeFilesMetrics metrics;
        std::string segment_dir_to_merge;
        utils::GetParentPath(file.location_, segment_dir_to_merge);
        segment_writer_ptr->Merge(segment_dir_to_merge, table_file.file_id_);

        files_holder.UnmarkFile(file);

        auto file_schema = file;
        file_schema.file_type_ = meta::SegmentSchema::TO_DELETE;
        updated.push_back(file_schema);
        auto size = segment_writer_ptr->Size();
        if (size >= file_schema.index_file_size_) {
            break;
        }
    }

    // step 3: serialize to disk
    try {
        status = segment_writer_ptr->Serialize();
        fiu_do_on("DBImpl.MergeFiles.Serialize_ThrowException", throw std::exception());
        fiu_do_on("DBImpl.MergeFiles.Serialize_ErrorStatus", status = Status(DB_ERROR, ""));
    } catch (std::exception& ex) {
        std::string msg = "Serialize merged index encounter exception: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << msg;
        status = Status(DB_ERROR, msg);
    }

    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to persist merged segment: " << new_segment_dir << ". Error: " << status.message();

        // if failed to serialize merge file to disk
        // typical error: out of disk space, out of memory or permission denied
        table_file.file_type_ = meta::SegmentSchema::TO_DELETE;
        status = meta_ptr_->UpdateCollectionFile(table_file);
        LOG_ENGINE_DEBUG_ << "Failed to update file to index, mark file: " << table_file.file_id_ << " to to_delete";

        return status;
    }

    // step 4: update table files state
    // if index type isn't IDMAP, set file type to TO_INDEX if file size exceed index_file_size
    // else set file type to RAW, no need to build index
    if (!utils::IsRawIndexType(table_file.engine_type_)) {
        table_file.file_type_ = (segment_writer_ptr->Size() >= table_file.index_file_size_)
                                    ? meta::SegmentSchema::TO_INDEX
                                    : meta::SegmentSchema::RAW;
    } else {
        table_file.file_type_ = meta::SegmentSchema::RAW;
    }
    table_file.file_size_ = segment_writer_ptr->Size();
    table_file.row_count_ = segment_writer_ptr->VectorCount();
    updated.push_back(table_file);
    status = meta_ptr_->UpdateCollectionFiles(updated);
    LOG_ENGINE_DEBUG_ << "New merged segment " << table_file.segment_id_ << " of size " << segment_writer_ptr->Size()
                      << " bytes";

    if (options_.insert_cache_immediately_) {
        segment_writer_ptr->Cache();
    }

    return status;
}

void
DBImpl::BackgroundMerge(std::set<std::string> collection_ids) {
    // LOG_ENGINE_TRACE_ << " Background merge thread start";

    Status status;
    for (auto& collection_id : collection_ids) {
        const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);

        auto status = merge_mgr_ptr_->MergeFiles(collection_id);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to get merge files for collection: " << collection_id
                              << " reason:" << status.message();
        }

        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "Server will shutdown, skip merge action for collection: " << collection_id;
            break;
        }
    }

    meta_ptr_->Archive();

    {
        uint64_t timeout = (options_.file_cleanup_timeout_ >= 0) ? options_.file_cleanup_timeout_ : 10;
        uint64_t ttl = timeout * meta::SECOND;  // default: file will be hard-deleted few seconds after soft-deleted
        meta_ptr_->CleanUpFilesWithTTL(ttl);
    }

    // LOG_ENGINE_TRACE_ << " Background merge thread exit";
}

void
DBImpl::StartBuildIndexTask() {
    // build index has been finished?
    {
        std::lock_guard<std::mutex> lck(index_result_mutex_);
        if (!index_thread_results_.empty()) {
            std::chrono::milliseconds span(10);
            if (index_thread_results_.back().wait_for(span) == std::future_status::ready) {
                index_thread_results_.pop_back();
            }
        }
    }

    // add new build index task
    {
        std::lock_guard<std::mutex> lck(index_result_mutex_);
        if (index_thread_results_.empty()) {
            index_thread_results_.push_back(index_thread_pool_.enqueue(&DBImpl::BackgroundBuildIndex, this));
        }
    }
}

void
DBImpl::BackgroundBuildIndex() {
    std::unique_lock<std::mutex> lock(build_index_mutex_);
    meta::FilesHolder files_holder;
    meta_ptr_->FilesToIndex(files_holder);

    milvus::engine::meta::SegmentsSchema& to_index_files = files_holder.HoldFiles();
    Status status = index_failed_checker_.IgnoreFailedIndexFiles(to_index_files);

    if (!to_index_files.empty()) {
        LOG_ENGINE_DEBUG_ << "Background build index thread begin " << to_index_files.size() << " files";

        // step 2: put build index task to scheduler
        std::vector<std::pair<scheduler::BuildIndexJobPtr, scheduler::SegmentSchemaPtr>> job2file_map;
        for (auto& file : to_index_files) {
            scheduler::BuildIndexJobPtr job = std::make_shared<scheduler::BuildIndexJob>(meta_ptr_, options_);
            scheduler::SegmentSchemaPtr file_ptr = std::make_shared<meta::SegmentSchema>(file);
            job->AddToIndexFiles(file_ptr);
            scheduler::JobMgrInst::GetInstance()->Put(job);
            job2file_map.push_back(std::make_pair(job, file_ptr));
        }

        // step 3: wait build index finished and mark failed files
        for (auto iter = job2file_map.begin(); iter != job2file_map.end(); ++iter) {
            scheduler::BuildIndexJobPtr job = iter->first;
            meta::SegmentSchema& file_schema = *(iter->second.get());
            job->WaitBuildIndexFinish();
            if (!job->GetStatus().ok()) {
                Status status = job->GetStatus();
                LOG_ENGINE_ERROR_ << "Building index job " << job->id() << " failed: " << status.ToString();

                index_failed_checker_.MarkFailedIndexFile(file_schema, status.message());
            } else {
                LOG_ENGINE_DEBUG_ << "Building index job " << job->id() << " succeed.";

                index_failed_checker_.MarkSucceedIndexFile(file_schema);
            }
            status = files_holder.UnmarkFile(file_schema);
            LOG_ENGINE_DEBUG_ << "Finish build index file " << file_schema.file_id_;
        }

        LOG_ENGINE_DEBUG_ << "Background build index thread finished";
        index_req_swn_.Notify();  // notify CreateIndex check circle
    }
}

Status
DBImpl::GetFilesToBuildIndex(const std::string& collection_id, const std::vector<int>& file_types,
                             meta::FilesHolder& files_holder) {
    files_holder.ReleaseFiles();
    auto status = meta_ptr_->FilesByType(collection_id, file_types, files_holder);

    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
    for (const milvus::engine::meta::SegmentSchema& file : files) {
        if (file.file_type_ == static_cast<int>(meta::SegmentSchema::RAW) &&
            file.row_count_ < meta::BUILD_INDEX_THRESHOLD) {
            // skip build index for files that row count less than certain threshold
            files_holder.UnmarkFile(file);
        } else if (index_failed_checker_.IsFailedIndexFile(file)) {
            // skip build index for files that failed before
            files_holder.UnmarkFile(file);
        }
    }

    return Status::OK();
}

Status
DBImpl::GetPartitionByTag(const std::string& collection_id, const std::string& partition_tag,
                          std::string& partition_name) {
    Status status;

    if (partition_tag.empty()) {
        partition_name = collection_id;

    } else {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = partition_tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name = collection_id;
            return status;
        }

        status = meta_ptr_->GetPartitionName(collection_id, partition_tag, partition_name);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << status.message();
        }
    }

    return status;
}

Status
DBImpl::GetPartitionsByTags(const std::string& collection_id, const std::vector<std::string>& partition_tags,
                            std::set<std::string>& partition_name_array) {
    std::vector<meta::CollectionSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(collection_id, partition_array);

    for (auto& tag : partition_tags) {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        if (valid_tag == milvus::engine::DEFAULT_PARTITON_TAG) {
            partition_name_array.insert(collection_id);
            return status;
        }

        for (auto& schema : partition_array) {
            if (server::StringHelpFunctions::IsRegexMatch(schema.partition_tag_, valid_tag)) {
                partition_name_array.insert(schema.collection_id_);
            }
        }
    }

    if (partition_name_array.empty()) {
        return Status(DB_PARTITION_NOT_FOUND, "The specified partiton does not exist");
    }

    return Status::OK();
}

Status
DBImpl::DropCollectionRecursively(const std::string& collection_id) {
    // dates partly delete files of the collection but currently we don't support
    LOG_ENGINE_DEBUG_ << "Prepare to delete collection " << collection_id;

    Status status;
    if (options_.wal_enable_) {
        wal_mgr_->DropCollection(collection_id);
    }

    status = mem_mgr_->EraseMemVector(collection_id);   // not allow insert
    status = meta_ptr_->DropCollection(collection_id);  // soft delete collection
    index_failed_checker_.CleanFailedIndexFileOfCollection(collection_id);

    // scheduler will determine when to delete collection files
    auto nres = scheduler::ResMgrInst::GetInstance()->GetNumOfComputeResource();
    scheduler::DeleteJobPtr job = std::make_shared<scheduler::DeleteJob>(collection_id, meta_ptr_, nres);
    scheduler::JobMgrInst::GetInstance()->Put(job);
    job->WaitAndDelete();

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = DropCollectionRecursively(schema.collection_id_);
        fiu_do_on("DBImpl.DropCollectionRecursively.failed", status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::UpdateCollectionIndexRecursively(const std::string& collection_id, const CollectionIndex& index) {
    DropIndex(collection_id);

    auto status = meta_ptr_->UpdateCollectionIndex(collection_id, index);
    fiu_do_on("DBImpl.UpdateCollectionIndexRecursively.fail_update_collection_index",
              status = Status(DB_META_TRANSACTION_FAILED, ""));
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to update collection index info for collection: " << collection_id;
        return status;
    }

    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    if (!status.ok()) {
        return status;
    }
    for (auto& schema : partition_array) {
        status = UpdateCollectionIndexRecursively(schema.collection_id_, index);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::WaitCollectionIndexRecursively(const std::shared_ptr<server::Context>& context,
                                       const std::string& collection_id, const CollectionIndex& index) {
    // for IDMAP type, only wait all NEW file converted to RAW file
    // for other type, wait NEW/RAW/NEW_MERGE/NEW_INDEX/TO_INDEX files converted to INDEX files
    std::vector<int> file_types;
    if (utils::IsRawIndexType(index.engine_type_)) {
        file_types = {
            static_cast<int32_t>(meta::SegmentSchema::NEW),
            static_cast<int32_t>(meta::SegmentSchema::NEW_MERGE),
        };
    } else {
        file_types = {
            static_cast<int32_t>(meta::SegmentSchema::RAW),       static_cast<int32_t>(meta::SegmentSchema::NEW),
            static_cast<int32_t>(meta::SegmentSchema::NEW_MERGE), static_cast<int32_t>(meta::SegmentSchema::NEW_INDEX),
            static_cast<int32_t>(meta::SegmentSchema::TO_INDEX),
        };
    }

    // get files to build index
    {
        meta::FilesHolder files_holder;
        auto status = GetFilesToBuildIndex(collection_id, file_types, files_holder);
        int times = 1;
        uint64_t repeat = 0;
        while (!files_holder.HoldFiles().empty()) {
            if (repeat % WAIT_BUILD_INDEX_INTERVAL == 0) {
                LOG_ENGINE_DEBUG_ << files_holder.HoldFiles().size() << " non-index files detected! Will build index "
                                  << times;
                if (!utils::IsRawIndexType(index.engine_type_)) {
                    status = meta_ptr_->UpdateCollectionFilesToIndex(collection_id);
                }
            }

            index_req_swn_.Wait_For(std::chrono::seconds(1));

            // client break the connection, no need to block, check every 1 second
            if (context->IsConnectionBroken()) {
                LOG_ENGINE_DEBUG_ << "Client connection broken, build index in background";
                break;  // just break, not return, continue to update partitions files to to_index
            }

            // check to_index files every 5 seconds
            repeat++;
            if (repeat % WAIT_BUILD_INDEX_INTERVAL == 0) {
                GetFilesToBuildIndex(collection_id, file_types, files_holder);
                ++times;
            }
        }
    }

    // build index for partition
    std::vector<meta::CollectionSchema> partition_array;
    auto status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = WaitCollectionIndexRecursively(context, schema.collection_id_, index);
        fiu_do_on("DBImpl.WaitCollectionIndexRecursively.fail_build_collection_Index_for_partition",
                  status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
    }

    // failed to build index for some files, return error
    std::string err_msg;
    index_failed_checker_.GetErrMsgForCollection(collection_id, err_msg);
    fiu_do_on("DBImpl.WaitCollectionIndexRecursively.not_empty_err_msg", err_msg.append("fiu"));
    if (!err_msg.empty()) {
        return Status(DB_ERROR, err_msg);
    }

    LOG_ENGINE_DEBUG_ << "WaitCollectionIndexRecursively finished";

    return Status::OK();
}

Status
DBImpl::DropCollectionIndexRecursively(const std::string& collection_id) {
    LOG_ENGINE_DEBUG_ << "Drop index for collection: " << collection_id;
    index_failed_checker_.CleanFailedIndexFileOfCollection(collection_id);
    auto status = meta_ptr_->DropCollectionIndex(collection_id);
    if (!status.ok()) {
        return status;
    }

    // drop partition index
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        status = DropCollectionIndexRecursively(schema.collection_id_);
        fiu_do_on("DBImpl.DropCollectionIndexRecursively.fail_drop_collection_Index_for_partition",
                  status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
DBImpl::GetCollectionRowCountRecursively(const std::string& collection_id, uint64_t& row_count) {
    row_count = 0;
    auto status = meta_ptr_->Count(collection_id, row_count);
    if (!status.ok()) {
        return status;
    }

    // get partition row count
    std::vector<meta::CollectionSchema> partition_array;
    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
    for (auto& schema : partition_array) {
        uint64_t partition_row_count = 0;
        status = GetCollectionRowCountRecursively(schema.collection_id_, partition_row_count);
        fiu_do_on("DBImpl.GetCollectionRowCountRecursively.fail_get_collection_rowcount_for_partition",
                  status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        row_count += partition_row_count;
    }

    return Status::OK();
}

Status
DBImpl::ExecWalRecord(const wal::MXLogRecord& record) {
    fiu_return_on("DBImpl.ExexWalRecord.return", Status(););

    auto collections_flushed = [&](const std::set<std::string>& collection_ids) -> uint64_t {
        if (collection_ids.empty()) {
            return 0;
        }

        uint64_t max_lsn = 0;
        if (options_.wal_enable_) {
            for (auto& collection : collection_ids) {
                uint64_t lsn = 0;
                meta_ptr_->GetCollectionFlushLSN(collection, lsn);
                wal_mgr_->CollectionFlushed(collection, lsn);
                if (lsn > max_lsn) {
                    max_lsn = lsn;
                }
            }
        }

        std::lock_guard<std::mutex> lck(merge_result_mutex_);
        for (auto& collection : collection_ids) {
            merge_collection_ids_.insert(collection);
        }
        return max_lsn;
    };

    Status status;

    switch (record.type) {
        case wal::MXLogType::Entity: {
            std::string target_collection_name;
            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
                return status;
            }

            std::set<std::string> flushed_collections;
            status = mem_mgr_->InsertEntities(target_collection_name, record.length, record.ids,
                                              (record.data_size / record.length / sizeof(float)),
                                              (const float*)record.data, record.attr_nbytes, record.attr_data_size,
                                              record.attr_data, record.lsn, flushed_collections);
            collections_flushed(flushed_collections);

            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }
        case wal::MXLogType::InsertBinary: {
            std::string target_collection_name;
            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
                return status;
            }

            std::set<std::string> flushed_collections;
            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(uint8_t)),
                                             (const u_int8_t*)record.data, record.lsn, flushed_collections);
            // even though !status.ok, run
            collections_flushed(flushed_collections);

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::InsertVector: {
            std::string target_collection_name;
            status = GetPartitionByTag(record.collection_id, record.partition_tag, target_collection_name);
            if (!status.ok()) {
                LOG_WAL_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << "Get partition fail: " << status.message();
                return status;
            }

            std::set<std::string> flushed_collections;
            status = mem_mgr_->InsertVectors(target_collection_name, record.length, record.ids,
                                             (record.data_size / record.length / sizeof(float)),
                                             (const float*)record.data, record.lsn, flushed_collections);
            // even though !status.ok, run
            collections_flushed(flushed_collections);

            // metrics
            milvus::server::CollectInsertMetrics metrics(record.length, status);
            break;
        }

        case wal::MXLogType::Delete: {
            std::vector<meta::CollectionSchema> partition_array;
            status = meta_ptr_->ShowPartitions(record.collection_id, partition_array);
            if (!status.ok()) {
                return status;
            }

            std::vector<std::string> collection_ids{record.collection_id};
            for (auto& partition : partition_array) {
                auto& partition_collection_id = partition.collection_id_;
                collection_ids.emplace_back(partition_collection_id);
            }

            if (record.length == 1) {
                for (auto& collection_id : collection_ids) {
                    status = mem_mgr_->DeleteVector(collection_id, *record.ids, record.lsn);
                    if (!status.ok()) {
                        return status;
                    }
                }
            } else {
                for (auto& collection_id : collection_ids) {
                    status = mem_mgr_->DeleteVectors(collection_id, record.length, record.ids, record.lsn);
                    if (!status.ok()) {
                        return status;
                    }
                }
            }
            break;
        }

        case wal::MXLogType::Flush: {
            if (!record.collection_id.empty()) {
                // flush one collection
                std::vector<meta::CollectionSchema> partition_array;
                status = meta_ptr_->ShowPartitions(record.collection_id, partition_array);
                if (!status.ok()) {
                    return status;
                }

                std::vector<std::string> collection_ids{record.collection_id};
                for (auto& partition : partition_array) {
                    auto& partition_collection_id = partition.collection_id_;
                    collection_ids.emplace_back(partition_collection_id);
                }

                std::set<std::string> flushed_collections;
                for (auto& collection_id : collection_ids) {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(collection_id);
                    if (!status.ok()) {
                        break;
                    }
                    flushed_collections.insert(collection_id);
                }

                collections_flushed(flushed_collections);

            } else {
                // flush all collections
                std::set<std::string> collection_ids;
                {
                    const std::lock_guard<std::mutex> lock(flush_merge_compact_mutex_);
                    status = mem_mgr_->Flush(collection_ids);
                }

                uint64_t lsn = collections_flushed(collection_ids);
                if (options_.wal_enable_) {
                    wal_mgr_->RemoveOldFiles(lsn);
                }
            }
            break;
        }
    }

    return status;
}

void
DBImpl::InternalFlush(const std::string& collection_id) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_id;
    ExecWalRecord(record);

    StartMergeTask();
}

void
DBImpl::BackgroundWalThread() {
    SetThreadName("wal_thread");
    server::SystemInfo::GetInstance().Init();

    std::chrono::system_clock::time_point next_auto_flush_time;
    auto get_next_auto_flush_time = [&]() {
        return std::chrono::system_clock::now() + std::chrono::seconds(options_.auto_flush_interval_);
    };
    if (options_.auto_flush_interval_ > 0) {
        next_auto_flush_time = get_next_auto_flush_time();
    }

    while (true) {
        if (options_.auto_flush_interval_ > 0) {
            if (std::chrono::system_clock::now() >= next_auto_flush_time) {
                InternalFlush();
                next_auto_flush_time = get_next_auto_flush_time();
            }
        }

        wal::MXLogRecord record;
        auto error_code = wal_mgr_->GetNextRecord(record);
        if (error_code != WAL_SUCCESS) {
            LOG_ENGINE_ERROR_ << "WAL background GetNextRecord error";
            break;
        }

        if (record.type != wal::MXLogType::None) {
            ExecWalRecord(record);
            if (record.type == wal::MXLogType::Flush) {
                // notify flush request to return
                flush_req_swn_.Notify();

                // if user flush all manually, update auto flush also
                if (record.collection_id.empty() && options_.auto_flush_interval_ > 0) {
                    next_auto_flush_time = get_next_auto_flush_time();
                }
            }

        } else {
            if (!initialized_.load(std::memory_order_acquire)) {
                InternalFlush();
                flush_req_swn_.Notify();
                WaitMergeFileFinish();
                WaitBuildIndexFinish();
                LOG_ENGINE_DEBUG_ << "WAL background thread exit";
                break;
            }

            if (options_.auto_flush_interval_ > 0) {
                swn_wal_.Wait_Until(next_auto_flush_time);
            } else {
                swn_wal_.Wait();
            }
        }
    }
}

void
DBImpl::BackgroundFlushThread() {
    SetThreadName("flush_thread");
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background flush thread exit";
            break;
        }

        InternalFlush();
        if (options_.auto_flush_interval_ > 0) {
            swn_flush_.Wait_For(std::chrono::seconds(options_.auto_flush_interval_));
        } else {
            swn_flush_.Wait();
        }
    }
}

void
DBImpl::BackgroundMetricThread() {
    server::SystemInfo::GetInstance().Init();
    while (true) {
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG_ENGINE_DEBUG_ << "DB background metric thread exit";
            break;
        }

        swn_metric_.Wait_For(std::chrono::seconds(BACKGROUND_METRIC_INTERVAL));
        StartMetricTask();
        meta::FilesHolder::PrintInfo();
    }
}

void
DBImpl::OnCacheInsertDataChanged(bool value) {
    options_.insert_cache_immediately_ = value;
}

void
DBImpl::OnUseBlasThresholdChanged(int64_t threshold) {
    faiss::distance_compute_blas_threshold = threshold;
}

void
DBImpl::SuspendIfFirst() {
    std::lock_guard<std::mutex> lock(suspend_build_mutex_);
    if (++live_search_num_ == 1) {
        LOG_ENGINE_TRACE_ << "live_search_num_: " << live_search_num_;
        knowhere::BuilderSuspend();
    }
}

void
DBImpl::ResumeIfLast() {
    std::lock_guard<std::mutex> lock(suspend_build_mutex_);
    if (--live_search_num_ == 0) {
        LOG_ENGINE_TRACE_ << "live_search_num_: " << live_search_num_;
        knowhere::BuildResume();
    }
}

}  // namespace engine
}  // namespace milvus
