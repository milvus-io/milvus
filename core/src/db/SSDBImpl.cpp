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

#include "db/SSDBImpl.h"
#include "cache/CpuCacheMgr.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Snapshots.h"
#include "metrics/Metrics.h"
#include "metrics/SystemInfo.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Exception.h"
#include "wal/WalDefinations.h"

#include <fiu-local.h>
#include <limits>
#include <utility>

namespace milvus {
namespace engine {

namespace {
constexpr int64_t BACKGROUND_METRIC_INTERVAL = 1;

static const Status SHUTDOWN_ERROR = Status(DB_ERROR, "Milvus server is shutdown!");
}  // namespace

#define CHECK_INITIALIZED                                \
    if (!initialized_.load(std::memory_order_acquire)) { \
        return SHUTDOWN_ERROR;                           \
    }

SSDBImpl::SSDBImpl(const DBOptions& options) : options_(options), initialized_(false) {
    if (options_.wal_enable_) {
        wal::MXLogConfiguration mxlog_config;
        mxlog_config.recovery_error_ignore = options_.recovery_error_ignore_;
        // 2 buffers in the WAL
        mxlog_config.buffer_size = options_.buffer_size_ / 2;
        mxlog_config.mxlog_path = options_.mxlog_path_;
        wal_mgr_ = std::make_shared<wal::WalManager>(mxlog_config);
    }
    Start();
}

SSDBImpl::~SSDBImpl() {
    Stop();
}

////////////////////////////////////////////////////////////////////////////////
// External APIs
////////////////////////////////////////////////////////////////////////////////
Status
SSDBImpl::Start() {
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    // LOG_ENGINE_TRACE_ << "DB service start";
    initialized_.store(true, std::memory_order_release);

    return Status::OK();
}

Status
SSDBImpl::Stop() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    initialized_.store(false, std::memory_order_release);

    // LOG_ENGINE_TRACE_ << "DB service stop";
    return Status::OK();
}

Status
SSDBImpl::CreateCollection(const snapshot::CreateCollectionContext& context) {
    CHECK_INITIALIZED;

    auto ctx = context;
    if (options_.wal_enable_) {
        ctx.lsn = wal_mgr_->CreateCollection(context.collection->GetName());
    }
    auto op = std::make_shared<snapshot::CreateCollectionOperation>(ctx);
    return op->Push();
}

Status
SSDBImpl::DescribeCollection(const std::string& collection_name, snapshot::CollectionPtr& collection,
                             std::map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    collection = ss->GetCollection();
    auto& fields = ss->GetResources<snapshot::Field>();
    for (auto& kv : fields) {
        fields_schema[kv.second.Get()] = ss->GetFieldElementsByField(kv.second->GetName());
    }
    return Status::OK();
}

Status
SSDBImpl::DropCollection(const std::string& name) {
    CHECK_INITIALIZED;

    LOG_ENGINE_DEBUG_ << "Prepare to delete collection " << name;

    snapshot::ScopedSnapshotT ss;
    auto& snapshots = snapshot::Snapshots::GetInstance();
    STATUS_CHECK(snapshots.GetSnapshot(ss, name));

    if (options_.wal_enable_) {
        // SS TODO
        /* wal_mgr_->DropCollection(ss->GetCollectionId()); */
    }

    return snapshots.DropCollection(ss->GetCollectionId(), std::numeric_limits<snapshot::LSN_TYPE>::max());
}

Status
SSDBImpl::HasCollection(const std::string& collection_name, bool& has_or_not) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    has_or_not = status.ok();

    return status;
}

Status
SSDBImpl::AllCollections(std::vector<std::string>& names) {
    CHECK_INITIALIZED;

    names.clear();
    return snapshot::Snapshots::GetInstance().GetCollectionNames(names);
}

Status
SSDBImpl::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    snapshot::LSN_TYPE lsn = 0;
    if (options_.wal_enable_) {
        // SS TODO
        /* lsn = wal_mgr_->CreatePartition(collection_id, partition_tag); */
    } else {
        lsn = ss->GetCollection()->GetLsn();
    }

    snapshot::OperationContext context;
    context.lsn = lsn;
    auto op = std::make_shared<snapshot::CreatePartitionOperation>(context, ss);

    snapshot::PartitionContext p_ctx;
    p_ctx.name = partition_name;
    snapshot::PartitionPtr partition;
    STATUS_CHECK(op->CommitNewPartition(p_ctx, partition));
    return op->Push();
}

Status
SSDBImpl::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    // SS TODO: Is below step needed? Or How to implement it?
    /* mem_mgr_->EraseMemVector(partition_name); */

    snapshot::PartitionContext context;
    context.name = partition_name;
    auto op = std::make_shared<snapshot::DropPartitionOperation>(context, ss);
    return op->Push();
}

Status
SSDBImpl::ShowPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    partition_names = std::move(ss->GetPartitionNames());
    return Status::OK();
}

Status
SSDBImpl::PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                            bool force) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    auto handler = std::make_shared<LoadVectorFieldHandler>(context, ss);
    handler->Iterate();

    return handler->GetStatus();
}

Status
SSDBImpl::GetVectorsByID(const std::string& collection_name, const IDNumbers& id_array,
                       std::vector<engine::VectorsData>& vectors) {
    CHECK_INITIALIZED;

    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_name));

    meta::FilesHolder files_holder;

    std::vector<std::string> partition_names;
    partition_names = std::move(ss->GetPartitionNames());
    partition_names.push_back(collection_name);

    cache::CpuCacheMgr::GetInstance()->PrintInfo();
    STATUS_CHECK(GetVectorsByIdHelper(id_array, vectors, files_holder));
    cache::CpuCacheMgr::GetInstance()->PrintInfo();

    return Status::OK();
}

//Status
//SSDBImpl::GetEntitiesByID(const std::string& collection_id, const milvus::engine::IDNumbers& id_array,
//                        std::vector<engine::VectorsData>& vectors, std::vector<engine::AttrsData>& attrs) {
//    if (!initialized_.load(std::memory_order_acquire)) {
//        return SHUTDOWN_ERROR;
//    }
//
//    bool has_collection;
//    auto status = HasCollection(collection_id, has_collection);
//    if (!has_collection) {
//        LOG_ENGINE_ERROR_ << "Collection " << collection_id << " does not exist: ";
//        return Status(DB_NOT_FOUND, "Collection does not exist");
//    }
//    if (!status.ok()) {
//        return status;
//    }
//
//    engine::meta::CollectionSchema collection_schema;
//    engine::meta::hybrid::FieldsSchema fields_schema;
//    collection_schema.collection_id_ = collection_id;
//    status = meta_ptr_->DescribeHybridCollection(collection_schema, fields_schema);
//    if (!status.ok()) {
//        return status;
//    }
//    std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_type;
//    for (auto schema : fields_schema.fields_schema_) {
//        if (schema.field_type_ == (int32_t)engine::meta::hybrid::DataType::VECTOR) {
//            continue;
//        }
//        attr_type.insert(std::make_pair(schema.field_name_, (engine::meta::hybrid::DataType)schema.field_type_));
//    }
//
//    meta::FilesHolder files_holder;
//    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
//                                meta::SegmentSchema::FILE_TYPE::BACKUP};
//
//    status = meta_ptr_->FilesByType(collection_id, file_types, files_holder);
//    if (!status.ok()) {
//        std::string err_msg = "Failed to get files for GetEntitiesByID: " + status.message();
//        LOG_ENGINE_ERROR_ << err_msg;
//        return status;
//    }
//
//    std::vector<meta::CollectionSchema> partition_array;
//    status = meta_ptr_->ShowPartitions(collection_id, partition_array);
//    if (!status.ok()) {
//        std::string err_msg = "Failed to get partitions for GetEntitiesByID: " + status.message();
//        LOG_ENGINE_ERROR_ << err_msg;
//        return status;
//    }
//    for (auto& schema : partition_array) {
//        status = meta_ptr_->FilesByType(schema.collection_id_, file_types, files_holder);
//        if (!status.ok()) {
//            std::string err_msg = "Failed to get files for GetEntitiesByID: " + status.message();
//            LOG_ENGINE_ERROR_ << err_msg;
//            return status;
//        }
//    }
//
//    if (files_holder.HoldFiles().empty()) {
//        LOG_ENGINE_DEBUG_ << "No files to get vector by id from";
//        return Status(DB_NOT_FOUND, "Collection is empty");
//    }
//
//    cache::CpuCacheMgr::GetInstance()->PrintInfo();
//    status = GetEntitiesByIdHelper(collection_id, id_array, attr_type, vectors, attrs, files_holder);
//    cache::CpuCacheMgr::GetInstance()->PrintInfo();
//
//    return status;
//}

////////////////////////////////////////////////////////////////////////////////
// Internal APIs
////////////////////////////////////////////////////////////////////////////////
Status
SSDBImpl::GetVectorsByIdHelper(const IDNumbers& id_array, std::vector<engine::VectorsData>& vectors,
                             meta::FilesHolder& files_holder) {
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
        if (temp_ids.empty()) {
            break;  // all vectors found, no need to continue
        }
        // Load bloom filter
        std::string segment_dir;
        engine::utils::GetParentPath(file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        auto status = segment_reader.LoadBloomFilter(id_bloom_filter_ptr);
        if (!status.ok()) {
            return status;
        }

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

    return Status::OK();
}

//Status
//SSDBImpl::GetEntitiesByIdHelper(const std::string& collection_id, const milvus::engine::IDNumbers& id_array,
//                              std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_type,
//                              std::vector<engine::VectorsData>& vectors, std::vector<engine::AttrsData>& attrs,
//                              milvus::engine::meta::FilesHolder& files_holder) {
//    // attention: this is a copy, not a reference, since the files_holder.UnMarkFile will change the array internal
//    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();
//    LOG_ENGINE_DEBUG_ << "Getting vector by id in " << files.size() << " files, id count = " << id_array.size();
//
//    // sometimes not all of id_array can be found, we need to return empty vector for id not found
//    // for example:
//    // id_array = [1, -1, 2, -1, 3]
//    // vectors should return [valid_vector, empty_vector, valid_vector, empty_vector, valid_vector]
//    // the ID2RAW is to ensure returned vector sequence is consist with id_array
//    using ID2ATTR = std::map<int64_t, engine::AttrsData>;
//    using ID2VECTOR = std::map<int64_t, engine::VectorsData>;
//    ID2ATTR map_id2attr;
//    ID2VECTOR map_id2vector;
//
//    IDNumbers temp_ids = id_array;
//    for (auto& file : files) {
//        // Load bloom filter
//        std::string segment_dir;
//        engine::utils::GetParentPath(file.location_, segment_dir);
//        segment::SegmentReader segment_reader(segment_dir);
//        segment::IdBloomFilterPtr id_bloom_filter_ptr;
//        segment_reader.LoadBloomFilter(id_bloom_filter_ptr);
//
//        for (IDNumbers::iterator it = temp_ids.begin(); it != temp_ids.end();) {
//            int64_t vector_id = *it;
//            // each id must has a VectorsData
//            // if vector not found for an id, its VectorsData's vector_count = 0, else 1
//            AttrsData& attr_ref = map_id2attr[vector_id];
//            VectorsData& vector_ref = map_id2vector[vector_id];
//
//            // Check if the id is present in bloom filter.
//            if (id_bloom_filter_ptr->Check(vector_id)) {
//                // Load uids and check if the id is indeed present. If yes, find its offset.
//                std::vector<segment::doc_id_t> uids;
//                auto status = segment_reader.LoadUids(uids);
//                if (!status.ok()) {
//                    return status;
//                }
//
//                auto found = std::find(uids.begin(), uids.end(), vector_id);
//                if (found != uids.end()) {
//                    auto offset = std::distance(uids.begin(), found);
//
//                    // Check whether the id has been deleted
//                    segment::DeletedDocsPtr deleted_docs_ptr;
//                    status = segment_reader.LoadDeletedDocs(deleted_docs_ptr);
//                    if (!status.ok()) {
//                        LOG_ENGINE_ERROR_ << status.message();
//                        return status;
//                    }
//                    auto& deleted_docs = deleted_docs_ptr->GetDeletedDocs();
//
//                    auto deleted = std::find(deleted_docs.begin(), deleted_docs.end(), offset);
//                    if (deleted == deleted_docs.end()) {
//                        // Load raw vector
//                        bool is_binary = utils::IsBinaryMetricType(file.metric_type_);
//                        size_t single_vector_bytes = is_binary ? file.dimension_ / 8 : file.dimension_ * sizeof(float);
//                        std::vector<uint8_t> raw_vector;
//                        status =
//                            segment_reader.LoadVectors(offset * single_vector_bytes, single_vector_bytes, raw_vector);
//                        if (!status.ok()) {
//                            LOG_ENGINE_ERROR_ << status.message();
//                            return status;
//                        }
//
//                        std::unordered_map<std::string, std::vector<uint8_t>> raw_attrs;
//                        auto attr_it = attr_type.begin();
//                        for (; attr_it != attr_type.end(); attr_it++) {
//                            size_t num_bytes;
//                            switch (attr_it->second) {
//                                case engine::meta::hybrid::DataType::INT8: {
//                                    num_bytes = 1;
//                                    break;
//                                }
//                                case engine::meta::hybrid::DataType::INT16: {
//                                    num_bytes = 2;
//                                    break;
//                                }
//                                case engine::meta::hybrid::DataType::INT32: {
//                                    num_bytes = 4;
//                                    break;
//                                }
//                                case engine::meta::hybrid::DataType::INT64: {
//                                    num_bytes = 8;
//                                    break;
//                                }
//                                case engine::meta::hybrid::DataType::FLOAT: {
//                                    num_bytes = 4;
//                                    break;
//                                }
//                                case engine::meta::hybrid::DataType::DOUBLE: {
//                                    num_bytes = 8;
//                                    break;
//                                }
//                                default: {
//                                    std::string msg = "Field type of " + attr_it->first + " is wrong";
//                                    return Status{DB_ERROR, msg};
//                                }
//                            }
//                            std::vector<uint8_t> raw_attr;
//                            status = segment_reader.LoadAttrs(attr_it->first, offset * num_bytes, num_bytes, raw_attr);
//                            if (!status.ok()) {
//                                LOG_ENGINE_ERROR_ << status.message();
//                                return status;
//                            }
//                            raw_attrs.insert(std::make_pair(attr_it->first, raw_attr));
//                        }
//
//                        vector_ref.vector_count_ = 1;
//                        if (is_binary) {
//                            vector_ref.binary_data_.swap(raw_vector);
//                        } else {
//                            std::vector<float> float_vector;
//                            float_vector.resize(file.dimension_);
//                            memcpy(float_vector.data(), raw_vector.data(), single_vector_bytes);
//                            vector_ref.float_data_.swap(float_vector);
//                        }
//
//                        attr_ref.attr_count_ = 1;
//                        attr_ref.attr_data_ = raw_attrs;
//                        attr_ref.attr_type_ = attr_type;
//                        temp_ids.erase(it);
//                        continue;
//                    }
//                }
//            }
//            it++;
//        }
//
//        // unmark file, allow the file to be deleted
//        files_holder.UnmarkFile(file);
//    }
//
//    for (auto id : id_array) {
//        VectorsData& vector_ref = map_id2vector[id];
//
//        VectorsData data;
//        data.vector_count_ = vector_ref.vector_count_;
//        if (data.vector_count_ > 0) {
//            data.float_data_ = vector_ref.float_data_;    // copy data since there could be duplicated id
//            data.binary_data_ = vector_ref.binary_data_;  // copy data since there could be duplicated id
//        }
//        vectors.emplace_back(data);
//
//        attrs.emplace_back(map_id2attr[id]);
//    }
//
//    if (vectors.empty()) {
//        std::string msg = "Vectors not found in collection " + collection_id;
//        LOG_ENGINE_DEBUG_ << msg;
//    }
//
//    return Status::OK();
//}

void
SSDBImpl::InternalFlush(const std::string& collection_id) {
    wal::MXLogRecord record;
    record.type = wal::MXLogType::Flush;
    record.collection_id = collection_id;
    ExecWalRecord(record);
}

void
SSDBImpl::BackgroundFlushThread() {
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
SSDBImpl::StartMetricTask() {
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
    /* SS TODO */
    // uint64_t size;
    // Size(size);
    // server::Metrics::GetInstance().DataFileSizeGaugeSet(size);
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
SSDBImpl::BackgroundMetricThread() {
    SetThreadName("metric_thread");
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

Status
SSDBImpl::ExecWalRecord(const wal::MXLogRecord& record) {
    return Status::OK();
}

void
SSDBImpl::BackgroundWalThread() {
    SetThreadName("wal_thread");
    server::SystemInfo::GetInstance().Init();

    std::chrono::system_clock::time_point next_auto_flush_time;
    auto get_next_auto_flush_time = [&]() {
        return std::chrono::system_clock::now() + std::chrono::seconds(options_.auto_flush_interval_);
    };
    if (options_.auto_flush_interval_ > 0) {
        next_auto_flush_time = get_next_auto_flush_time();
    }

    InternalFlush();
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
                // SS TODO
                // WaitMergeFileFinish();
                // WaitBuildIndexFinish();
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

}  // namespace engine
}  // namespace milvus
