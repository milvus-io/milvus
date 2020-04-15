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

#include "db/meta/SqliteMetaImpl.h"

#include <sqlite_orm.h>
#include <unistd.h>

#include <fiu-local.h>
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>

#include "MetaConsts.h"
#include "db/IDGenerator.h"
#include "db/OngoingFileChecker.h"
#include "db/Utils.h"
#include "metrics/Metrics.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace engine {
namespace meta {

using namespace sqlite_orm;

namespace {

Status
HandleException(const std::string& desc, const char* what = nullptr) {
    if (what == nullptr) {
        LOG_ENGINE_ERROR_ << desc;
        return Status(DB_META_TRANSACTION_FAILED, desc);
    } else {
        std::string msg = desc + ":" + what;
        LOG_ENGINE_ERROR_ << msg;
        return Status(DB_META_TRANSACTION_FAILED, msg);
    }
}

}  // namespace

inline auto
StoragePrototype(const std::string& path) {
    return make_storage(
        path,
        make_table(META_ENVIRONMENT, make_column("global_lsn", &EnvironmentSchema::global_lsn_, default_value(0))),
        make_table(META_TABLES, make_column("id", &CollectionSchema::id_, primary_key()),
                   make_column("table_id", &CollectionSchema::collection_id_, unique()),
                   make_column("state", &CollectionSchema::state_), make_column("dimension", &CollectionSchema::dimension_),
                   make_column("created_on", &CollectionSchema::created_on_),
                   make_column("flag", &CollectionSchema::flag_, default_value(0)),
                   make_column("index_file_size", &CollectionSchema::index_file_size_),
                   make_column("engine_type", &CollectionSchema::engine_type_),
                   make_column("index_params", &CollectionSchema::index_params_),
                   make_column("metric_type", &CollectionSchema::metric_type_),
                   make_column("owner_table", &CollectionSchema::owner_collection_, default_value("")),
                   make_column("partition_tag", &CollectionSchema::partition_tag_, default_value("")),
                   make_column("version", &CollectionSchema::version_, default_value(CURRENT_VERSION)),
                   make_column("flush_lsn", &CollectionSchema::flush_lsn_)),
        make_table(META_FIELDS, make_column("collection_id", &hybrid::FieldSchema::collection_id_),
                   make_column("field_name", &hybrid::FieldSchema::field_name_),
                   make_column("field_type", &hybrid::FieldSchema::field_type_),
                   make_column("field_params", &hybrid::FieldSchema::field_params_)),
        make_table(
            META_TABLEFILES, make_column("id", &SegmentSchema::id_, primary_key()),
            make_column("table_id", &SegmentSchema::collection_id_),
            make_column("segment_id", &SegmentSchema::segment_id_, default_value("")),
            make_column("engine_type", &SegmentSchema::engine_type_),
            make_column("file_id", &SegmentSchema::file_id_), make_column("file_type", &SegmentSchema::file_type_),
            make_column("file_size", &SegmentSchema::file_size_, default_value(0)),
            make_column("row_count", &SegmentSchema::row_count_, default_value(0)),
            make_column("updated_time", &SegmentSchema::updated_time_),
            make_column("created_on", &SegmentSchema::created_on_), make_column("date", &SegmentSchema::date_),
            make_column("flush_lsn", &SegmentSchema::flush_lsn_)));
}

inline auto
CollectionPrototype(const std::string& path) {
    return make_storage(
        path,
        make_table(META_ENVIRONMENT, make_column("global_lsn", &EnvironmentSchema::global_lsn_, default_value(0))),
        make_table(META_COLLECTIONS, make_column("id", &hybrid::CollectionSchema::id_, primary_key()),
                   make_column("collection_id", &hybrid::CollectionSchema::collection_id_, unique()),
                   make_column("state", &hybrid::CollectionSchema::state_),
                   make_column("field_num", &hybrid::CollectionSchema::field_num),
                   make_column("created_on", &hybrid::CollectionSchema::created_on_),
                   make_column("flag", &hybrid::CollectionSchema::flag_, default_value(0)),
                   make_column("owner_collection", &hybrid::CollectionSchema::owner_collection_, default_value("")),
                   make_column("partition_tag", &hybrid::CollectionSchema::partition_tag_, default_value("")),
                   make_column("version", &hybrid::CollectionSchema::version_, default_value(CURRENT_VERSION)),
                   make_column("flush_lsn", &hybrid::CollectionSchema::flush_lsn_)),
        make_table(META_FIELDS, make_column("collection_id", &hybrid::FieldSchema::collection_id_),
                   make_column("field_name", &hybrid::FieldSchema::field_name_),
                   make_column("field_type", &hybrid::FieldSchema::field_type_),
                   make_column("field_params", &hybrid::FieldSchema::field_params_)),
        make_table(
            META_COLLECTIONFILES,
            make_column("id", &hybrid::CollectionFileSchema::id_, primary_key()),
            make_column("collection_id", &hybrid::CollectionFileSchema::collection_id_),
            make_column("segment_id", &hybrid::CollectionFileSchema::segment_id_, default_value("")),
            make_column("file_id", &hybrid::CollectionFileSchema::file_id_),
            make_column("file_type", &hybrid::CollectionFileSchema::file_type_),
            make_column("file_size", &hybrid::CollectionFileSchema::file_size_, default_value(0)),
            make_column("row_count", &hybrid::CollectionFileSchema::row_count_, default_value(0)),
            make_column("updated_time", &hybrid::CollectionFileSchema::updated_time_),
            make_column("created_on", &hybrid::CollectionFileSchema::created_on_),
            make_column("date", &hybrid::CollectionFileSchema::date_),
            make_column("flush_lsn", &hybrid::CollectionFileSchema::flush_lsn_)));
}

using ConnectorT = decltype(StoragePrototype("table"));
static std::unique_ptr<ConnectorT> ConnectorPtr;

using CollectionConnectT = decltype(CollectionPrototype(""));
static std::unique_ptr<CollectionConnectT> CollectionConnectPtr;

SqliteMetaImpl::SqliteMetaImpl(const DBMetaOptions& options) : options_(options) {
    Initialize();
}

SqliteMetaImpl::~SqliteMetaImpl() {
}

Status
SqliteMetaImpl::NextCollectionId(std::string& collection_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    ss << id_generator.GetNextIDNumber();
    collection_id = ss.str();
    return Status::OK();
}

Status
SqliteMetaImpl::NextFileId(std::string& file_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    ss << id_generator.GetNextIDNumber();
    file_id = ss.str();
    return Status::OK();
}

void
SqliteMetaImpl::ValidateMetaSchema() {
    bool is_null_connector{ConnectorPtr == nullptr};
    fiu_do_on("SqliteMetaImpl.ValidateMetaSchema.NullConnection", is_null_connector = true);
    if (is_null_connector) {
        return;
    }

    // old meta could be recreated since schema changed, throw exception if meta schema is not compatible
    auto ret = ConnectorPtr->sync_schema_simulate();
    if (ret.find(META_TABLES) != ret.end() &&
        sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_TABLES]) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }
    if (ret.find(META_FIELDS) != ret.end()
        && sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_FIELDS]) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }
    if (ret.find(META_TABLEFILES) != ret.end() &&
        sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_TABLEFILES]) {
        throw Exception(DB_INCOMPATIB_META, "Meta TableFiles schema is created by Milvus old version");
    }
}

void
SqliteMetaImpl::ValidateCollectionMetaSchema() {
    bool is_null_connector{CollectionConnectPtr == nullptr};
    fiu_do_on("SqliteMetaImpl.ValidateMetaSchema.NullConnection", is_null_connector = true);
    if (is_null_connector) {
        return;
    }

    // old meta could be recreated since schema changed, throw exception if meta schema is not compatible
    auto ret = CollectionConnectPtr->sync_schema_simulate();
    if (ret.find(META_COLLECTIONS) != ret.end() &&
        sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_COLLECTIONS]) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }
    if (ret.find(META_FIELDS) != ret.end()
        && sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_FIELDS]) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }
    if (ret.find(META_COLLECTIONFILES) != ret.end() &&
        sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_TABLEFILES]) {
        throw Exception(DB_INCOMPATIB_META, "Meta TableFiles schema is created by Milvus old version");
    }
}

Status
SqliteMetaImpl::Initialize() {
    if (!boost::filesystem::is_directory(options_.path_)) {
        auto ret = boost::filesystem::create_directory(options_.path_);
        fiu_do_on("SqliteMetaImpl.Initialize.fail_create_directory", ret = false);
        if (!ret) {
            std::string msg = "Failed to create db directory " + options_.path_;
            LOG_ENGINE_ERROR_ << msg;
            throw Exception(DB_INVALID_PATH, msg);
        }
    }

    ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(options_.path_ + "/meta.sqlite"));

    ValidateMetaSchema();

    ConnectorPtr->sync_schema();
    ConnectorPtr->open_forever();                          // thread safe option
    ConnectorPtr->pragma.journal_mode(journal_mode::WAL);  // WAL => write ahead log

    CollectionConnectPtr = std::make_unique<CollectionConnectT>(CollectionPrototype(options_.path_ + "/metah.sqlite"));

    ValidateCollectionMetaSchema();

    CollectionConnectPtr->sync_schema();
    CollectionConnectPtr->open_forever();
    CollectionConnectPtr->pragma.journal_mode(journal_mode::WAL);  // WAL => write ahead log

    CleanUpShadowFiles();

    return Status::OK();
}

Status
SqliteMetaImpl::CreateCollection(CollectionSchema& collection_schema) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        if (collection_schema.collection_id_ == "") {
            NextCollectionId(collection_schema.collection_id_);
        } else {
            fiu_do_on("SqliteMetaImpl.CreateCollection.throw_exception", throw std::exception());
            auto collection = ConnectorPtr->select(columns(&CollectionSchema::state_),
                                              where(c(&CollectionSchema::collection_id_) == collection_schema.collection_id_));
            if (collection.size() == 1) {
                if (CollectionSchema::TO_DELETE == std::get<0>(collection[0])) {
                    return Status(DB_ERROR, "Collection already exists and it is in delete state, please wait a second");
                } else {
                    // Change from no error to already exist.
                    return Status(DB_ALREADY_EXIST, "Collection already exists");
                }
            }
        }

        collection_schema.id_ = -1;
        collection_schema.created_on_ = utils::GetMicroSecTimeStamp();

        try {
            fiu_do_on("SqliteMetaImpl.CreateCollection.insert_throw_exception", throw std::exception());
            auto id = ConnectorPtr->insert(collection_schema);
            collection_schema.id_ = id;
        } catch (std::exception& e) {
            return HandleException("Encounter exception when create collection", e.what());
        }

        LOG_ENGINE_DEBUG_ << "Successfully create collection: " << collection_schema.collection_id_;

        return utils::CreateCollectionPath(options_, collection_schema.collection_id_);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection", e.what());
    }
}

Status
SqliteMetaImpl::DescribeCollection(CollectionSchema& collection_schema) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);
        fiu_do_on("SqliteMetaImpl.DescribeCollection.throw_exception", throw std::exception());
        auto groups = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::state_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::engine_type_,
                    &CollectionSchema::index_params_, &CollectionSchema::metric_type_, &CollectionSchema::owner_collection_,
                    &CollectionSchema::partition_tag_, &CollectionSchema::version_, &CollectionSchema::flush_lsn_),
            where(c(&CollectionSchema::collection_id_) == collection_schema.collection_id_ and
                  c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        if (groups.size() == 1) {
            collection_schema.id_ = std::get<0>(groups[0]);
            collection_schema.state_ = std::get<1>(groups[0]);
            collection_schema.dimension_ = std::get<2>(groups[0]);
            collection_schema.created_on_ = std::get<3>(groups[0]);
            collection_schema.flag_ = std::get<4>(groups[0]);
            collection_schema.index_file_size_ = std::get<5>(groups[0]);
            collection_schema.engine_type_ = std::get<6>(groups[0]);
            collection_schema.index_params_ = std::get<7>(groups[0]);
            collection_schema.metric_type_ = std::get<8>(groups[0]);
            collection_schema.owner_collection_ = std::get<9>(groups[0]);
            collection_schema.partition_tag_ = std::get<10>(groups[0]);
            collection_schema.version_ = std::get<11>(groups[0]);
            collection_schema.flush_lsn_ = std::get<12>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_schema.collection_id_ + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::HasCollection(const std::string& collection_id, bool& has_or_not) {
    has_or_not = false;

    try {
        fiu_do_on("SqliteMetaImpl.HasCollection.throw_exception", throw std::exception());
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);
        auto collections = ConnectorPtr->select(
            columns(&CollectionSchema::id_),
            where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));
        if (collections.size() == 1) {
            has_or_not = true;
        } else {
            has_or_not = false;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::AllCollections(std::vector<CollectionSchema>& collection_schema_array) {
    try {
        fiu_do_on("SqliteMetaImpl.AllCollections.throw_exception", throw std::exception());
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);
        auto selected = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::collection_id_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::engine_type_,
                    &CollectionSchema::index_params_, &CollectionSchema::metric_type_, &CollectionSchema::owner_collection_,
                    &CollectionSchema::partition_tag_, &CollectionSchema::version_, &CollectionSchema::flush_lsn_),
            where(c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE and c(&CollectionSchema::owner_collection_) == ""));
        for (auto& collection : selected) {
            CollectionSchema schema;
            schema.id_ = std::get<0>(collection);
            schema.collection_id_ = std::get<1>(collection);
            schema.dimension_ = std::get<2>(collection);
            schema.created_on_ = std::get<3>(collection);
            schema.flag_ = std::get<4>(collection);
            schema.index_file_size_ = std::get<5>(collection);
            schema.engine_type_ = std::get<6>(collection);
            schema.index_params_ = std::get<7>(collection);
            schema.metric_type_ = std::get<8>(collection);
            schema.owner_collection_ = std::get<9>(collection);
            schema.partition_tag_ = std::get<10>(collection);
            schema.version_ = std::get<11>(collection);
            schema.flush_lsn_ = std::get<12>(collection);

            collection_schema_array.emplace_back(schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup all collections", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropCollection(const std::string& collection_id) {
    try {
        fiu_do_on("SqliteMetaImpl.DropCollection.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // soft delete collection
        ConnectorPtr->update_all(
            set(c(&CollectionSchema::state_) = (int)CollectionSchema::TO_DELETE),
            where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        LOG_ENGINE_DEBUG_ << "Successfully delete collection, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DeleteCollectionFiles(const std::string& collection_id) {
    try {
        fiu_do_on("SqliteMetaImpl.DeleteCollectionFiles.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // soft delete collection files
        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_DELETE,
                                     c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));

        LOG_ENGINE_DEBUG_ << "Successfully delete collection files, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateCollectionFile(SegmentSchema& file_schema) {
    if (file_schema.date_ == EmptyDate) {
        file_schema.date_ = utils::GetDate();
    }
    CollectionSchema collection_schema;
    collection_schema.collection_id_ = file_schema.collection_id_;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        fiu_do_on("SqliteMetaImpl.CreateCollectionFile.throw_exception", throw std::exception());
        server::MetricCollector metric;

        NextFileId(file_schema.file_id_);
        if (file_schema.segment_id_.empty()) {
            file_schema.segment_id_ = file_schema.file_id_;
        }
        file_schema.dimension_ = collection_schema.dimension_;
        file_schema.file_size_ = 0;
        file_schema.row_count_ = 0;
        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
        file_schema.updated_time_ = file_schema.created_on_;
        file_schema.index_file_size_ = collection_schema.index_file_size_;
        file_schema.index_params_ = collection_schema.index_params_;
        file_schema.engine_type_ = collection_schema.engine_type_;
        file_schema.metric_type_ = collection_schema.metric_type_;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto id = ConnectorPtr->insert(file_schema);
        file_schema.id_ = id;

        LOG_ENGINE_DEBUG_ << "Successfully create collection file, file id = " << file_schema.file_id_;
        return utils::CreateCollectionFilePath(options_, file_schema);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetCollectionFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                              SegmentsSchema& collection_files) {
    try {
        fiu_do_on("SqliteMetaImpl.GetCollectionFiles.throw_exception", throw std::exception());

        collection_files.clear();

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        auto select_columns = columns(&SegmentSchema::id_, &SegmentSchema::segment_id_, &SegmentSchema::file_id_,
                                        &SegmentSchema::file_type_, &SegmentSchema::file_size_, &SegmentSchema::row_count_,
                                        &SegmentSchema::date_, &SegmentSchema::engine_type_, &SegmentSchema::created_on_);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns,
                where(c(&SegmentSchema::collection_id_) == collection_id and in(&SegmentSchema::id_, ids) and
                      c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));
        }

        Status result;
        for (auto& file : selected) {
            SegmentSchema file_schema;
            file_schema.collection_id_ = collection_id;
            file_schema.id_ = std::get<0>(file);
            file_schema.segment_id_ = std::get<1>(file);
            file_schema.file_id_ = std::get<2>(file);
            file_schema.file_type_ = std::get<3>(file);
            file_schema.file_size_ = std::get<4>(file);
            file_schema.row_count_ = std::get<5>(file);
            file_schema.date_ = std::get<6>(file);
            file_schema.engine_type_ = std::get<7>(file);
            file_schema.created_on_ = std::get<8>(file);
            file_schema.dimension_ = collection_schema.dimension_;
            file_schema.index_file_size_ = collection_schema.index_file_size_;
            file_schema.index_params_ = collection_schema.index_params_;
            file_schema.metric_type_ = collection_schema.metric_type_;

            utils::GetCollectionFilePath(options_, file_schema);

            collection_files.emplace_back(file_schema);
        }

        LOG_ENGINE_DEBUG_ << "Get collection files by id";
        return result;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection files", e.what());
    }
}

Status
SqliteMetaImpl::GetCollectionFilesBySegmentId(const std::string& segment_id,
                                         milvus::engine::meta::SegmentsSchema& collection_files) {
    try {
        collection_files.clear();

        auto select_columns = columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                                      &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                                      &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_,
                                      &SegmentSchema::created_on_);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns,
                where(c(&SegmentSchema::segment_id_) == segment_id and
                      c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));
        }

        if (!selected.empty()) {
            CollectionSchema collection_schema;
            collection_schema.collection_id_ = std::get<1>(selected[0]);
            auto status = DescribeCollection(collection_schema);
            if (!status.ok()) {
                return status;
            }

            for (auto& file : selected) {
                SegmentSchema file_schema;
                file_schema.collection_id_ = collection_schema.collection_id_;
                file_schema.id_ = std::get<0>(file);
                file_schema.segment_id_ = std::get<2>(file);
                file_schema.file_id_ = std::get<3>(file);
                file_schema.file_type_ = std::get<4>(file);
                file_schema.file_size_ = std::get<5>(file);
                file_schema.row_count_ = std::get<6>(file);
                file_schema.date_ = std::get<7>(file);
                file_schema.engine_type_ = std::get<8>(file);
                file_schema.created_on_ = std::get<9>(file);
                file_schema.dimension_ = collection_schema.dimension_;
                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;

                utils::GetCollectionFilePath(options_, file_schema);
                collection_files.emplace_back(file_schema);
            }
        }

        LOG_ENGINE_DEBUG_ << "Get collection files by segment id";
        return Status::OK();
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection files by segment id", e.what());
    }
}

Status
SqliteMetaImpl::UpdateCollectionFlag(const std::string& collection_id, int64_t flag) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFlag.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // set all backup file to raw
        ConnectorPtr->update_all(set(c(&CollectionSchema::flag_) = flag), where(c(&CollectionSchema::collection_id_) == collection_id));
        LOG_ENGINE_DEBUG_ << "Successfully update collection flag, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection flag: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFlushLSN(const std::string& collection_id, uint64_t flush_lsn) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(set(c(&CollectionSchema::flush_lsn_) = flush_lsn),
                                 where(c(&CollectionSchema::collection_id_) == collection_id));
        LOG_ENGINE_DEBUG_ << "Successfully update collection flush_lsn, collection id = " << collection_id << " flush_lsn = " << flush_lsn;;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection lsn: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetCollectionFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto selected =
            ConnectorPtr->select(columns(&CollectionSchema::flush_lsn_), where(c(&CollectionSchema::collection_id_) == collection_id));

        if (selected.size() > 0) {
            flush_lsn = std::get<0>(selected[0]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }

    } catch (std::exception& e) {
        return HandleException("Encounter exception when getting collection files by flush_lsn", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFile(SegmentSchema& file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFile.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto collections = ConnectorPtr->select(columns(&CollectionSchema::state_),
                                           where(c(&CollectionSchema::collection_id_) == file_schema.collection_id_));

        // if the collection has been deleted, just mark the collection file as TO_DELETE
        // clean thread will delete the file later
        if (collections.size() < 1 || std::get<0>(collections[0]) == (int)CollectionSchema::TO_DELETE) {
            file_schema.file_type_ = SegmentSchema::TO_DELETE;
        }

        ConnectorPtr->update(file_schema);

        LOG_ENGINE_DEBUG_ << "Update single collection file, file id = " << file_schema.file_id_;
    } catch (std::exception& e) {
        std::string msg =
            "Exception update collection file: collection_id = " + file_schema.collection_id_ + " file_id = " + file_schema.file_id_;
        return HandleException(msg, e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFiles(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFiles.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::map<std::string, bool> has_collections;
        for (auto& file : files) {
            if (has_collections.find(file.collection_id_) != has_collections.end()) {
                continue;
            }
            auto collections = ConnectorPtr->select(columns(&CollectionSchema::id_),
                                               where(c(&CollectionSchema::collection_id_) == file.collection_id_ and
                                                     c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));
            if (collections.size() >= 1) {
                has_collections[file.collection_id_] = true;
            } else {
                has_collections[file.collection_id_] = false;
            }
        }

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& file : files) {
                if (!has_collections[file.collection_id_]) {
                    file.file_type_ = SegmentSchema::TO_DELETE;
                }

                file.updated_time_ = utils::GetMicroSecTimeStamp();
                ConnectorPtr->update(file);
            }
            return true;
        });
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFiles.fail_commited", commited = false);

        if (!commited) {
            return HandleException("UpdateCollectionFiles error: sqlite transaction failed");
        }

        LOG_ENGINE_DEBUG_ << "Update " << files.size() << " collection files";
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFilesRowCount(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        for (auto& file : files) {
            ConnectorPtr->update_all(set(c(&SegmentSchema::row_count_) = file.row_count_,
                                         c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                     where(c(&SegmentSchema::file_id_) == file.file_id_));
            LOG_ENGINE_DEBUG_ << "Update file " << file.file_id_ << " row count to " << file.row_count_;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files row count", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionIndex(const std::string& collection_id, const CollectionIndex& index) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionIndex.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto collections = ConnectorPtr->select(

        columns(&CollectionSchema::id_, &CollectionSchema::state_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::owner_collection_,
                &CollectionSchema::partition_tag_, &CollectionSchema::version_, &CollectionSchema::flush_lsn_),
        where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        if (collections.size() > 0) {
            meta::CollectionSchema collection_schema;
            collection_schema.id_ = std::get<0>(collections[0]);
            collection_schema.collection_id_ = collection_id;
            collection_schema.state_ = std::get<1>(collections[0]);
            collection_schema.dimension_ = std::get<2>(collections[0]);
            collection_schema.created_on_ = std::get<3>(collections[0]);
            collection_schema.flag_ = std::get<4>(collections[0]);
            collection_schema.index_file_size_ = std::get<5>(collections[0]);
            collection_schema.owner_collection_ = std::get<6>(collections[0]);
            collection_schema.partition_tag_ = std::get<7>(collections[0]);
            collection_schema.version_ = std::get<8>(collections[0]);
            collection_schema.flush_lsn_ = std::get<9>(collections[0]);
            collection_schema.engine_type_ = index.engine_type_;
            collection_schema.index_params_ = index.extra_params_.dump();
            collection_schema.metric_type_ = index.metric_type_;

            ConnectorPtr->update(collection_schema);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }

        // set all backup file to raw
        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::RAW,
                                     c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::file_type_) == (int)SegmentSchema::BACKUP));

        LOG_ENGINE_DEBUG_ << "Successfully update collection index, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection index: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFilesToIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFilesToIndex.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_INDEX),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::row_count_) >= meta::BUILD_INDEX_THRESHOLD and
                                       c(&SegmentSchema::file_type_) == (int)SegmentSchema::RAW));

        LOG_ENGINE_DEBUG_ << "Update files to to_index, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files to to_index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DescribeCollectionIndex(const std::string& collection_id, CollectionIndex& index) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DescribeCollectionIndex.throw_exception", throw std::exception());

        auto groups = ConnectorPtr->select(
            columns(&CollectionSchema::engine_type_, &CollectionSchema::index_params_, &CollectionSchema::metric_type_),
            where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        if (groups.size() == 1) {
            index.engine_type_ = std::get<0>(groups[0]);
            index.extra_params_ = milvus::json::parse(std::get<1>(groups[0]));
            index.metric_type_ = std::get<2>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropCollectionIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DropCollectionIndex.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // soft delete index files
        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_DELETE,
                                     c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::file_type_) == (int)SegmentSchema::INDEX));

        // set all backup file to raw
        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::RAW,
                                     c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::file_type_) == (int)SegmentSchema::BACKUP));

        // set collection index type to raw
        auto groups = ConnectorPtr->select(columns(&CollectionSchema::metric_type_),
                                           where(c(&CollectionSchema::collection_id_) == collection_id));

        int32_t raw_engine_type = DEFAULT_ENGINE_TYPE;
        if (groups.size() == 1) {
            int32_t metric_type_ = std::get<0>(groups[0]);
            if (engine::utils::IsBinaryMetricType(metric_type_)) {
                raw_engine_type = (int32_t)EngineType::FAISS_BIN_IDMAP;
            }
        }
        ConnectorPtr->update_all(
            set(c(&CollectionSchema::engine_type_) = raw_engine_type, c(&CollectionSchema::index_params_) = "{}"),
            where(c(&CollectionSchema::collection_id_) == collection_id));

        LOG_ENGINE_DEBUG_ << "Successfully drop collection index, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreatePartition(const std::string& collection_id, const std::string& partition_name, const std::string& tag,
                                uint64_t lsn) {
    server::MetricCollector metric;

    CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_id;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        return status;
    }

    // not allow create partition under partition
    if (!collection_schema.owner_collection_.empty()) {
        return Status(DB_ERROR, "Nested partition is not allowed");
    }

    // trim side-blank of tag, only compare valid characters
    // for example: " ab cd " is treated as "ab cd"
    std::string valid_tag = tag;
    server::StringHelpFunctions::TrimStringBlank(valid_tag);

    // not allow duplicated partition
    std::string exist_partition;
    GetPartitionName(collection_id, valid_tag, exist_partition);
    if (!exist_partition.empty()) {
        return Status(DB_ERROR, "Duplicate partition is not allowed");
    }

    if (partition_name == "") {
        // generate unique partition name
        NextCollectionId(collection_schema.collection_id_);
    } else {
        collection_schema.collection_id_ = partition_name;
    }

    collection_schema.id_ = -1;
    collection_schema.flag_ = 0;
    collection_schema.created_on_ = utils::GetMicroSecTimeStamp();
    collection_schema.owner_collection_ = collection_id;
    collection_schema.partition_tag_ = valid_tag;
    collection_schema.flush_lsn_ = lsn;

    status = CreateCollection(collection_schema);
    if (status.code() == DB_ALREADY_EXIST) {
        return Status(DB_ALREADY_EXIST, "Partition already exists");
    }

    return status;
}

Status
SqliteMetaImpl::DropPartition(const std::string& partition_name) {
    return DropCollection(partition_name);
}

Status
SqliteMetaImpl::ShowPartitions(const std::string& collection_id, std::vector<meta::CollectionSchema>& partition_schema_array) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.ShowPartitions.throw_exception", throw std::exception());

        auto partitions = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::state_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::engine_type_,
                    &CollectionSchema::index_params_, &CollectionSchema::metric_type_, &CollectionSchema::partition_tag_,
                    &CollectionSchema::version_, &CollectionSchema::collection_id_),
            where(c(&CollectionSchema::owner_collection_) == collection_id and
                  c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        for (size_t i = 0; i < partitions.size(); i++) {
            meta::CollectionSchema partition_schema;
            partition_schema.id_ = std::get<0>(partitions[i]);
            partition_schema.state_ = std::get<1>(partitions[i]);
            partition_schema.dimension_ = std::get<2>(partitions[i]);
            partition_schema.created_on_ = std::get<3>(partitions[i]);
            partition_schema.flag_ = std::get<4>(partitions[i]);
            partition_schema.index_file_size_ = std::get<5>(partitions[i]);
            partition_schema.engine_type_ = std::get<6>(partitions[i]);
            partition_schema.index_params_ = std::get<7>(partitions[i]);
            partition_schema.metric_type_ = std::get<8>(partitions[i]);
            partition_schema.owner_collection_ = collection_id;
            partition_schema.partition_tag_ = std::get<9>(partitions[i]);
            partition_schema.version_ = std::get<10>(partitions[i]);
            partition_schema.collection_id_ = std::get<11>(partitions[i]);
            partition_schema_array.emplace_back(partition_schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when show partitions", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetPartitionName(const std::string& collection_id, const std::string& tag, std::string& partition_name) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.GetPartitionName.throw_exception", throw std::exception());

        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        auto name = ConnectorPtr->select(
            columns(&CollectionSchema::collection_id_),
            where(c(&CollectionSchema::owner_collection_) == collection_id and c(&CollectionSchema::partition_tag_) == valid_tag and
                  c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));
        if (name.size() > 0) {
            partition_name = std::get<0>(name[0]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + "'s partition " + valid_tag + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when get partition name", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::FilesToSearch(const std::string& collection_id, SegmentsSchema& files) {
    files.clear();

    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.FilesToSearch.throw_exception", throw std::exception());

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        // perform query
        auto select_columns =
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_);

        auto match_collectionid = c(&SegmentSchema::collection_id_) == collection_id;

        std::vector<int> file_types = {(int)SegmentSchema::RAW, (int)SegmentSchema::TO_INDEX,
                                       (int)SegmentSchema::INDEX};
        auto match_type = in(&SegmentSchema::file_type_, file_types);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            auto filter = where(match_collectionid and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        }

        Status ret;
        for (auto& file : selected) {
            SegmentSchema collection_file;
            collection_file.id_ = std::get<0>(file);
            collection_file.collection_id_ = std::get<1>(file);
            collection_file.segment_id_ = std::get<2>(file);
            collection_file.file_id_ = std::get<3>(file);
            collection_file.file_type_ = std::get<4>(file);
            collection_file.file_size_ = std::get<5>(file);
            collection_file.row_count_ = std::get<6>(file);
            collection_file.date_ = std::get<7>(file);
            collection_file.engine_type_ = std::get<8>(file);
            collection_file.dimension_ = collection_schema.dimension_;
            collection_file.index_file_size_ = collection_schema.index_file_size_;
            collection_file.index_params_ = collection_schema.index_params_;
            collection_file.metric_type_ = collection_schema.metric_type_;

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                ret = status;
            }

            files.emplace_back(collection_file);
        }
        if (files.empty()) {
            LOG_ENGINE_ERROR_ << "No file to search for collection: " << collection_id;
        }

        if (selected.size() > 0) {
            LOG_ENGINE_DEBUG_ << "Collect " << selected.size() << " to-search files";
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when iterate index files", e.what());
    }
}

Status
SqliteMetaImpl::FilesToMerge(const std::string& collection_id, SegmentsSchema& files) {
    files.clear();

    try {
        fiu_do_on("SqliteMetaImpl.FilesToMerge.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // check collection existence
        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        // get files to merge
        auto select_columns = columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                                      &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                                      &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::created_on_);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns,
                where(c(&SegmentSchema::file_type_) == (int)SegmentSchema::RAW and
                      c(&SegmentSchema::collection_id_) == collection_id),
                order_by(&SegmentSchema::file_size_).desc());
        }

        Status result;
        int64_t to_merge_files = 0;
        for (auto& file : selected) {
            SegmentSchema collection_file;
            collection_file.file_size_ = std::get<5>(file);
            if (collection_file.file_size_ >= collection_schema.index_file_size_) {
                continue;  // skip large file
            }

            collection_file.id_ = std::get<0>(file);
            collection_file.collection_id_ = std::get<1>(file);
            collection_file.segment_id_ = std::get<2>(file);
            collection_file.file_id_ = std::get<3>(file);
            collection_file.file_type_ = std::get<4>(file);
            collection_file.row_count_ = std::get<6>(file);
            collection_file.date_ = std::get<7>(file);
            collection_file.created_on_ = std::get<8>(file);
            collection_file.dimension_ = collection_schema.dimension_;
            collection_file.index_file_size_ = collection_schema.index_file_size_;
            collection_file.index_params_ = collection_schema.index_params_;
            collection_file.metric_type_ = collection_schema.metric_type_;

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                result = status;
            }

            files.emplace_back(collection_file);
            ++to_merge_files;
        }

        if (to_merge_files > 0) {
            LOG_ENGINE_TRACE_ << "Collect " << to_merge_files << " to-merge files";
        }
        return result;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when iterate merge files", e.what());
    }
}

Status
SqliteMetaImpl::FilesToIndex(SegmentsSchema& files) {
    files.clear();

    try {
        fiu_do_on("SqliteMetaImpl.FilesToIndex.throw_exception", throw std::exception());

        server::MetricCollector metric;

        auto select_columns = columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                                      &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                                      &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_,
                                      &SegmentSchema::created_on_);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns,
                where(c(&SegmentSchema::file_type_) == (int)SegmentSchema::TO_INDEX));
        }

        std::map<std::string, CollectionSchema> groups;
        SegmentSchema collection_file;

        Status ret;
        for (auto& file : selected) {
            collection_file.id_ = std::get<0>(file);
            collection_file.collection_id_ = std::get<1>(file);
            collection_file.segment_id_ = std::get<2>(file);
            collection_file.file_id_ = std::get<3>(file);
            collection_file.file_type_ = std::get<4>(file);
            collection_file.file_size_ = std::get<5>(file);
            collection_file.row_count_ = std::get<6>(file);
            collection_file.date_ = std::get<7>(file);
            collection_file.engine_type_ = std::get<8>(file);
            collection_file.created_on_ = std::get<9>(file);

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                ret = status;
            }
            auto groupItr = groups.find(collection_file.collection_id_);
            if (groupItr == groups.end()) {
                CollectionSchema collection_schema;
                collection_schema.collection_id_ = collection_file.collection_id_;
                auto status = DescribeCollection(collection_schema);
                fiu_do_on("SqliteMetaImpl_FilesToIndex_CollectionNotFound",
                          status = Status(DB_NOT_FOUND, "collection not found"));
                if (!status.ok()) {
                    return status;
                }
                groups[collection_file.collection_id_] = collection_schema;
            }
            collection_file.dimension_ = groups[collection_file.collection_id_].dimension_;
            collection_file.index_file_size_ = groups[collection_file.collection_id_].index_file_size_;
            collection_file.index_params_ = groups[collection_file.collection_id_].index_params_;
            collection_file.metric_type_ = groups[collection_file.collection_id_].metric_type_;
            files.push_back(collection_file);
        }

        if (selected.size() > 0) {
            LOG_ENGINE_DEBUG_ << "Collect " << selected.size() << " to-index files";
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when iterate raw files", e.what());
    }
}

Status
SqliteMetaImpl::FilesByType(const std::string& collection_id, const std::vector<int>& file_types, SegmentsSchema& files) {
    if (file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    Status ret = Status::OK();

    try {
        fiu_do_on("SqliteMetaImpl.FilesByType.throw_exception", throw std::exception());
        files.clear();

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        // get files by type
        auto select_columns = columns(&SegmentSchema::id_, &SegmentSchema::segment_id_,
                                      &SegmentSchema::file_id_,
                                      &SegmentSchema::file_type_,
                                      &SegmentSchema::file_size_,
                                      &SegmentSchema::row_count_,
                                      &SegmentSchema::date_,
                                      &SegmentSchema::engine_type_,
                                      &SegmentSchema::created_on_);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns,
                where(in(&SegmentSchema::file_type_, file_types) and c(&SegmentSchema::collection_id_) == collection_id));
        }

        if (selected.size() >= 1) {
            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
            int to_index_count = 0, index_count = 0, backup_count = 0;
            for (auto& file : selected) {
                SegmentSchema file_schema;
                file_schema.collection_id_ = collection_id;
                file_schema.id_ = std::get<0>(file);
                file_schema.segment_id_ = std::get<1>(file);
                file_schema.file_id_ = std::get<2>(file);
                file_schema.file_type_ = std::get<3>(file);
                file_schema.file_size_ = std::get<4>(file);
                file_schema.row_count_ = std::get<5>(file);
                file_schema.date_ = std::get<6>(file);
                file_schema.engine_type_ = std::get<7>(file);
                file_schema.created_on_ = std::get<8>(file);

                file_schema.dimension_ = collection_schema.dimension_;
                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;

                switch (file_schema.file_type_) {
                    case (int)SegmentSchema::RAW:++raw_count;
                        break;
                    case (int)SegmentSchema::NEW:++new_count;
                        break;
                    case (int)SegmentSchema::NEW_MERGE:++new_merge_count;
                        break;
                    case (int)SegmentSchema::NEW_INDEX:++new_index_count;
                        break;
                    case (int)SegmentSchema::TO_INDEX:++to_index_count;
                        break;
                    case (int)SegmentSchema::INDEX:++index_count;
                        break;
                    case (int)SegmentSchema::BACKUP:++backup_count;
                        break;
                    default:break;
                }

                auto status = utils::GetCollectionFilePath(options_, file_schema);
                if (!status.ok()) {
                    ret = status;
                }

                files.emplace_back(file_schema);
            }

            std::string msg = "Get collection files by type.";
            for (int file_type : file_types) {
                switch (file_type) {
                    case (int)SegmentSchema::RAW:msg = msg + " raw files:" + std::to_string(raw_count);
                        break;
                    case (int)SegmentSchema::NEW:msg = msg + " new files:" + std::to_string(new_count);
                        break;
                    case (int)SegmentSchema::NEW_MERGE:
                        msg = msg + " new_merge files:"
                              + std::to_string(new_merge_count);
                        break;
                    case (int)SegmentSchema::NEW_INDEX:
                        msg = msg + " new_index files:"
                              + std::to_string(new_index_count);
                        break;
                    case (int)SegmentSchema::TO_INDEX:msg = msg + " to_index files:" + std::to_string(to_index_count);
                        break;
                    case (int)SegmentSchema::INDEX:msg = msg + " index files:" + std::to_string(index_count);
                        break;
                    case (int)SegmentSchema::BACKUP:msg = msg + " backup files:" + std::to_string(backup_count);
                        break;
                    default:break;
                }
            }
            LOG_ENGINE_DEBUG_ << msg;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when check non index files", e.what());
    }

    return ret;
}

Status
SqliteMetaImpl::FilesByID(const std::vector<size_t>& ids, SegmentsSchema& files) {
    files.clear();

    if (ids.empty()) {
        return Status::OK();
    }

    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.FilesByID.throw_exception", throw std::exception());

        auto select_columns =
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_);

        std::vector<int> file_types = {(int)SegmentSchema::RAW, (int)SegmentSchema::TO_INDEX,
                                       (int)SegmentSchema::INDEX};
        auto match_type = in(&SegmentSchema::file_type_, file_types);

        // perform query
        decltype(ConnectorPtr->select(select_columns)) selected;
        auto match_fileid = in(&SegmentSchema::id_, ids);
        auto filter = where(match_fileid and match_type);
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns, filter);
        }

        std::map<std::string, meta::CollectionSchema> collections;
        Status ret;
        for (auto& file : selected) {
            SegmentSchema collection_file;
            collection_file.id_ = std::get<0>(file);
            collection_file.collection_id_ = std::get<1>(file);
            collection_file.segment_id_ = std::get<2>(file);
            collection_file.file_id_ = std::get<3>(file);
            collection_file.file_type_ = std::get<4>(file);
            collection_file.file_size_ = std::get<5>(file);
            collection_file.row_count_ = std::get<6>(file);
            collection_file.date_ = std::get<7>(file);
            collection_file.engine_type_ = std::get<8>(file);

            if (collections.find(collection_file.collection_id_) == collections.end()) {
                CollectionSchema collection_schema;
                collection_schema.collection_id_ = collection_file.collection_id_;
                auto status = DescribeCollection(collection_schema);
                if (!status.ok()) {
                    return status;
                }
                collections.insert(std::make_pair(collection_file.collection_id_, collection_schema));
            }

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                ret = status;
            }

            files.emplace_back(collection_file);
        }

        for (auto& collection_file : files) {
            CollectionSchema& collection_schema = collections[collection_file.collection_id_];
            collection_file.dimension_ = collection_schema.dimension_;
            collection_file.index_file_size_ = collection_schema.index_file_size_;
            collection_file.index_params_ = collection_schema.index_params_;
            collection_file.metric_type_ = collection_schema.metric_type_;
        }

        if (files.empty()) {
            LOG_ENGINE_ERROR_ << "No file to search in file id list";
        } else {
            LOG_ENGINE_DEBUG_ << "Collect " << selected.size() << " files by id";
        }

        return ret;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when iterate index files", e.what());
    }
    return Status::OK();
}

// TODO(myh): Support swap to cloud storage
Status
SqliteMetaImpl::Archive() {
    auto& criterias = options_.archive_conf_.GetCriterias();
    if (criterias.size() == 0) {
        return Status::OK();
    }

    for (auto kv : criterias) {
        auto& criteria = kv.first;
        auto& limit = kv.second;
        if (criteria == engine::ARCHIVE_CONF_DAYS) {
            int64_t usecs = limit * DAY * US_PS;
            int64_t now = utils::GetMicroSecTimeStamp();
            try {
                fiu_do_on("SqliteMetaImpl.Archive.throw_exception", throw std::exception());

                // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_DELETE),
                                         where(c(&SegmentSchema::created_on_) < (int64_t)(now - usecs) and
                                               c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));
            } catch (std::exception& e) {
                return HandleException("Encounter exception when update collection files", e.what());
            }

            LOG_ENGINE_DEBUG_ << "Archive old files";
        }
        if (criteria == engine::ARCHIVE_CONF_DISK) {
            uint64_t sum = 0;
            Size(sum);

            int64_t to_delete = (int64_t)sum - limit * G;
            DiscardFiles(to_delete);

            LOG_ENGINE_DEBUG_ << "Archive files to free disk";
        }
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Size(uint64_t& result) {
    result = 0;
    try {
        fiu_do_on("SqliteMetaImpl.Size.throw_exception", throw std::exception());

        auto selected = ConnectorPtr->select(columns(sum(&SegmentSchema::file_size_)),
                                             where(c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));
        for (auto& total_size : selected) {
            if (!std::get<0>(total_size)) {
                continue;
            }
            result += (uint64_t)(*std::get<0>(total_size));
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when calculate db size", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpShadowFiles() {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::vector<int> file_types = {(int)SegmentSchema::NEW, (int)SegmentSchema::NEW_INDEX,
                                       (int)SegmentSchema::NEW_MERGE};
        auto files =
            ConnectorPtr->select(columns(&SegmentSchema::id_), where(in(&SegmentSchema::file_type_, file_types)));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& file : files) {
                LOG_ENGINE_DEBUG_ << "Remove collection file type as NEW";
                ConnectorPtr->remove<SegmentSchema>(std::get<0>(file));
            }
            return true;
        });

        fiu_do_on("SqliteMetaImpl.CleanUpShadowFiles.fail_commited", commited = false);
        fiu_do_on("SqliteMetaImpl.CleanUpShadowFiles.throw_exception", throw std::exception());
        if (!commited) {
            return HandleException("CleanUp error: sqlite transaction failed");
        }

        if (files.size() > 0) {
            LOG_ENGINE_DEBUG_ << "Clean " << files.size() << " files";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpFilesWithTTL(uint64_t seconds /*, CleanUpFilter* filter*/) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> collection_ids;
    std::map<std::string, SegmentSchema> segment_ids;

    // remove to_delete files
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_ThrowException", throw std::exception());

        server::MetricCollector metric;

        std::vector<int> file_types = {
            (int)SegmentSchema::TO_DELETE,
            (int)SegmentSchema::BACKUP,
        };

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // collect files to be deleted
        auto files = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::engine_type_, &SegmentSchema::file_id_, &SegmentSchema::file_type_,
                    &SegmentSchema::date_),
            where(in(&SegmentSchema::file_type_, file_types) and
                  c(&SegmentSchema::updated_time_) < now - seconds * US_PS));

        int64_t clean_files = 0;
        auto commited = ConnectorPtr->transaction([&]() mutable {
            SegmentSchema collection_file;
            for (auto& file : files) {
                collection_file.id_ = std::get<0>(file);
                collection_file.collection_id_ = std::get<1>(file);
                collection_file.segment_id_ = std::get<2>(file);
                collection_file.engine_type_ = std::get<3>(file);
                collection_file.file_id_ = std::get<4>(file);
                collection_file.file_type_ = std::get<5>(file);
                collection_file.date_ = std::get<6>(file);

                // check if the file can be deleted
                if (OngoingFileChecker::GetInstance().IsIgnored(collection_file)) {
                    LOG_ENGINE_DEBUG_ << "File:" << collection_file.file_id_
                                     << " currently is in use, not able to delete now";
                    continue;  // ignore this file, don't delete it
                }

                // erase from cache, must do this before file deleted,
                // because GetCollectionFilePath won't able to generate file path after the file is deleted
                // TODO(zhiru): clean up
                utils::GetCollectionFilePath(options_, collection_file);
                server::CommonUtil::EraseFromCache(collection_file.location_);

                if (collection_file.file_type_ == (int)SegmentSchema::TO_DELETE) {
                    // delete file from meta
                    ConnectorPtr->remove<SegmentSchema>(collection_file.id_);

                    // delete file from disk storage
                    utils::DeleteCollectionFilePath(options_, collection_file);

                    LOG_ENGINE_DEBUG_ << "Remove file id:" << collection_file.file_id_ << " location:"
                                     << collection_file.location_;
                    collection_ids.insert(collection_file.collection_id_);
                    segment_ids.insert(std::make_pair(collection_file.segment_id_, collection_file));

                    ++clean_files;
                }
            }
            return true;
        });
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_FailCommited", commited = false);

        if (!commited) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }

        if (clean_files > 0) {
            LOG_ENGINE_DEBUG_ << "Clean " << clean_files << " files expired in " << seconds << " seconds";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection files", e.what());
    }

    // remove to_delete collections
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_ThrowException", throw std::exception());
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto collections = ConnectorPtr->select(columns(&CollectionSchema::id_, &CollectionSchema::collection_id_),
                                           where(c(&CollectionSchema::state_) == (int)CollectionSchema::TO_DELETE));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& collection : collections) {
                utils::DeleteCollectionPath(options_, std::get<1>(collection), false);  // only delete empty folder
                ConnectorPtr->remove<CollectionSchema>(std::get<0>(collection));
            }

            return true;
        });
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_Failcommited", commited = false);

        if (!commited) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }

        if (collections.size() > 0) {
            LOG_ENGINE_DEBUG_ << "Remove " << collections.size() << " collections from meta";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection files", e.what());
    }

    // remove deleted collection folder
    // don't remove collection folder until all its files has been deleted
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollectionFolder_ThrowException", throw std::exception());
        server::MetricCollector metric;

        int64_t remove_collections = 0;
        for (auto& collection_id : collection_ids) {
            auto selected = ConnectorPtr->select(columns(&SegmentSchema::file_id_),
                                                 where(c(&SegmentSchema::collection_id_) == collection_id));
            if (selected.size() == 0) {
                utils::DeleteCollectionPath(options_, collection_id);
                ++remove_collections;
            }
        }

        if (remove_collections) {
            LOG_ENGINE_DEBUG_ << "Remove " << remove_collections << " collections folder";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    // remove deleted segment folder
    // don't remove segment folder until all its files has been deleted
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveSegmentFolder_ThrowException", throw std::exception());
        server::MetricCollector metric;

        int64_t remove_segments = 0;
        for (auto& segment_id : segment_ids) {
            auto selected = ConnectorPtr->select(columns(&SegmentSchema::id_),
                                                 where(c(&SegmentSchema::segment_id_) == segment_id.first));
            if (selected.size() == 0) {
                utils::DeleteSegment(options_, segment_id.second);
                std::string segment_dir;
                utils::GetParentPath(segment_id.second.location_, segment_dir);
                LOG_ENGINE_DEBUG_ << "Remove segment directory: " << segment_dir;
                ++remove_segments;
            }
        }

        if (remove_segments > 0) {
            LOG_ENGINE_DEBUG_ << "Remove " << remove_segments << " segments folder";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Count(const std::string& collection_id, uint64_t& result) {
    try {
        fiu_do_on("SqliteMetaImpl.Count.throw_exception", throw std::exception());

        server::MetricCollector metric;

        std::vector<int> file_types = {(int)SegmentSchema::RAW, (int)SegmentSchema::TO_INDEX,
                                       (int)SegmentSchema::INDEX};
        auto select_columns = columns(&SegmentSchema::row_count_);
        decltype(ConnectorPtr->select(select_columns)) selected;
        {
            // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);
            selected = ConnectorPtr->select(select_columns,
                where(in(&SegmentSchema::file_type_, file_types) and c(&SegmentSchema::collection_id_) == collection_id));
        }

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);

        if (!status.ok()) {
            return status;
        }

        result = 0;
        for (auto& file : selected) {
            result += std::get<0>(file);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when calculate collection file size", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::DropAll() {
    LOG_ENGINE_DEBUG_ << "Drop all sqlite meta";

    try {
        ConnectorPtr->drop_table(META_TABLES);
        ConnectorPtr->drop_table(META_TABLEFILES);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when drop all meta", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DiscardFiles(int64_t to_discard_size) {
    if (to_discard_size <= 0) {
        return Status::OK();
    }

    LOG_ENGINE_DEBUG_ << "About to discard size=" << to_discard_size;

    try {
        fiu_do_on("SqliteMetaImpl.DiscardFiles.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto commited = ConnectorPtr->transaction([&]() mutable {
            auto selected =
                ConnectorPtr->select(columns(&SegmentSchema::id_, &SegmentSchema::file_size_),
                                     where(c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE),
                                     order_by(&SegmentSchema::id_), limit(10));

            std::vector<int> ids;
            SegmentSchema collection_file;

            for (auto& file : selected) {
                if (to_discard_size <= 0)
                    break;
                collection_file.id_ = std::get<0>(file);
                collection_file.file_size_ = std::get<1>(file);
                ids.push_back(collection_file.id_);
                LOG_ENGINE_DEBUG_ << "Discard file id=" << collection_file.file_id_
                                 << " file size=" << collection_file.file_size_;
                to_discard_size -= collection_file.file_size_;
            }

            if (ids.size() == 0) {
                return true;
            }

            ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_DELETE,
                                         c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                     where(in(&SegmentSchema::id_, ids)));

            return true;
        });
        fiu_do_on("SqliteMetaImpl.DiscardFiles.fail_commited", commited = false);
        if (!commited) {
            return HandleException("DiscardFiles error: sqlite transaction failed");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when discard collection file", e.what());
    }

    return DiscardFiles(to_discard_size);
}

Status
SqliteMetaImpl::SetGlobalLastLSN(uint64_t lsn) {
    try {
        server::MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&EnvironmentSchema::global_lsn_));
        if (selected.size() == 0) {
            EnvironmentSchema env;
            env.global_lsn_ = lsn;
            ConnectorPtr->insert(env);
        } else {
            uint64_t last_lsn = std::get<0>(selected[0]);
            if (lsn == last_lsn) {
                return Status::OK();
            }

            ConnectorPtr->update_all(set(c(&EnvironmentSchema::global_lsn_) = lsn));
        }

        LOG_ENGINE_DEBUG_ << "Update global lsn = " << lsn;
    } catch (std::exception& e) {
        std::string msg = "Exception update global lsn = " + lsn;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetGlobalLastLSN(uint64_t& lsn) {
    try {
        server::MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&EnvironmentSchema::global_lsn_));
        if (selected.size() == 0) {
            lsn = 0;
        } else {
            lsn = std::get<0>(selected[0]);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateHybridCollection(meta::CollectionSchema& collection_schema,
                                       meta::hybrid::FieldsSchema& fields_schema) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        if (collection_schema.collection_id_ == "") {
            NextCollectionId(collection_schema.collection_id_);
        } else {
            fiu_do_on("SqliteMetaImpl.CreateCollection.throw_exception", throw std::exception());
            auto collection = ConnectorPtr->select(columns(&CollectionSchema::state_),
                                                   where(c(&CollectionSchema::collection_id_)
                                                         == collection_schema.collection_id_));
            if (collection.size() == 1) {
                if (CollectionSchema::TO_DELETE == std::get<0>(collection[0])) {
                    return Status(DB_ERROR, "Collection already exists and it is in delete state, please wait a second");
                } else {
                    // Change from no error to already exist.
                    return Status(DB_ALREADY_EXIST, "Collection already exists");
                }
            }
        }

        collection_schema.id_ = -1;
        collection_schema.created_on_ = utils::GetMicroSecTimeStamp();

        try {
            fiu_do_on("SqliteMetaImpl.CreateHybridCollection.insert_throw_exception", throw std::exception());
            auto id = ConnectorPtr->insert(collection_schema);
            collection_schema.id_ = id;
        } catch (std::exception& e) {
            return HandleException("Encounter exception when create collection", e.what());
        }

        LOG_ENGINE_DEBUG_ << "Successfully create collection collection: " << collection_schema.collection_id_;

        Status status = utils::CreateCollectionPath(options_, collection_schema.collection_id_);
        if (!status.ok()) {
            return status;
        }

        try {
            for (uint64_t i = 0; i < fields_schema.fields_schema_.size(); ++i) {
                hybrid::FieldSchema schema = fields_schema.fields_schema_[i];
                auto field_id = ConnectorPtr->insert(schema);
                LOG_ENGINE_DEBUG_ << "Successfully create collection field" << field_id;
            }
        } catch (std::exception& e) {
            return HandleException("Encounter exception when create collection field", e.what());
        }

        return status;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection", e.what());
    }
}

Status
SqliteMetaImpl::DescribeHybridCollection(milvus::engine::meta::CollectionSchema& collection_schema,
                                         milvus::engine::meta::hybrid::FieldsSchema& fields_schema) {

    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DescriCollection.throw_exception", throw std::exception());
        auto groups = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::state_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::engine_type_,
                    &CollectionSchema::index_params_, &CollectionSchema::metric_type_, &CollectionSchema::owner_collection_,
                    &CollectionSchema::partition_tag_, &CollectionSchema::version_, &CollectionSchema::flush_lsn_),
            where(c(&CollectionSchema::collection_id_) == collection_schema.collection_id_ and
                  c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        if (groups.size() == 1) {
            collection_schema.id_ = std::get<0>(groups[0]);
            collection_schema.state_ = std::get<1>(groups[0]);
            collection_schema.dimension_ = std::get<2>(groups[0]);
            collection_schema.created_on_ = std::get<3>(groups[0]);
            collection_schema.flag_ = std::get<4>(groups[0]);
            collection_schema.index_file_size_ = std::get<5>(groups[0]);
            collection_schema.engine_type_ = std::get<6>(groups[0]);
            collection_schema.index_params_ = std::get<7>(groups[0]);
            collection_schema.metric_type_ = std::get<8>(groups[0]);
            collection_schema.owner_collection_ = std::get<9>(groups[0]);
            collection_schema.partition_tag_ = std::get<10>(groups[0]);
            collection_schema.version_ = std::get<11>(groups[0]);
            collection_schema.flush_lsn_ = std::get<12>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_schema.collection_id_ + " not found");
        }

        auto field_groups = ConnectorPtr->select(
            columns(&hybrid::FieldSchema::collection_id_,
                    &hybrid::FieldSchema::field_name_,
                    &hybrid::FieldSchema::field_type_,
                    &hybrid::FieldSchema::field_params_),
            where(c(&hybrid::FieldSchema::collection_id_) == collection_schema.collection_id_));

        if (field_groups.size() >= 1) {
            fields_schema.fields_schema_.resize(field_groups.size());
            for (uint64_t i = 0; i < field_groups.size(); ++i) {
                fields_schema.fields_schema_[i].collection_id_ = std::get<0>(field_groups[i]);
                fields_schema.fields_schema_[i].field_name_ = std::get<1>(field_groups[i]);
                fields_schema.fields_schema_[i].field_type_ = std::get<2>(field_groups[i]);
                fields_schema.fields_schema_[i].field_params_ = std::get<3>(field_groups[i]);
            }
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_schema.collection_id_ + " fields not found");
        }

    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateHybridCollectionFile(SegmentSchema& file_schema) {

    if (file_schema.date_ == EmptyDate) {
        file_schema.date_ = utils::GetDate();
    }
    CollectionSchema collection_schema;
    hybrid::FieldsSchema fields_schema;
    collection_schema.collection_id_ = file_schema.collection_id_;
    auto status = DescribeHybridCollection(collection_schema, fields_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        fiu_do_on("SqliteMetaImpl.CreateCollectionFile.throw_exception", throw std::exception());
        server::MetricCollector metric;

        NextFileId(file_schema.file_id_);
        if (file_schema.segment_id_.empty()) {
            file_schema.segment_id_ = file_schema.file_id_;
        }
        file_schema.dimension_ = collection_schema.dimension_;
        file_schema.file_size_ = 0;
        file_schema.row_count_ = 0;
        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
        file_schema.updated_time_ = file_schema.created_on_;
        file_schema.index_file_size_ = collection_schema.index_file_size_;
        file_schema.index_params_ = collection_schema.index_params_;
        file_schema.engine_type_ = collection_schema.engine_type_;
        file_schema.metric_type_ = collection_schema.metric_type_;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto id = ConnectorPtr->insert(file_schema);
        file_schema.id_ = id;

        for (auto field_schema : fields_schema.fields_schema_) {
            ConnectorPtr->insert(field_schema);
        }

        LOG_ENGINE_DEBUG_ << "Successfully create collection file, file id = " << file_schema.file_id_;
        return utils::CreateCollectionFilePath(options_, file_schema);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection file", e.what());
    }

    return Status::OK();
}

}  // namespace meta
}  // namespace engine
}  // namespace milvus
