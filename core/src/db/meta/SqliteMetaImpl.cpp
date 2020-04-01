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
        ENGINE_LOG_ERROR << desc;
        return Status(DB_META_TRANSACTION_FAILED, desc);
    } else {
        std::string msg = desc + ":" + what;
        ENGINE_LOG_ERROR << msg;
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
                   make_column("owner_table", &CollectionSchema::owner_table_, default_value("")),
                   make_column("partition_tag", &CollectionSchema::partition_tag_, default_value("")),
                   make_column("version", &CollectionSchema::version_, default_value(CURRENT_VERSION)),
                   make_column("flush_lsn", &CollectionSchema::flush_lsn_)),
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

using ConnectorT = decltype(StoragePrototype(""));
static std::unique_ptr<ConnectorT> ConnectorPtr;

SqliteMetaImpl::SqliteMetaImpl(const DBMetaOptions& options) : options_(options) {
    Initialize();
}

SqliteMetaImpl::~SqliteMetaImpl() {
}

Status
SqliteMetaImpl::NextTableId(std::string& collection_id) {
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
    if (ret.find(META_TABLEFILES) != ret.end() &&
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
            ENGINE_LOG_ERROR << msg;
            throw Exception(DB_INVALID_PATH, msg);
        }
    }

    ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(options_.path_ + "/meta.sqlite"));

    ValidateMetaSchema();

    ConnectorPtr->sync_schema();
    ConnectorPtr->open_forever();                          // thread safe option
    ConnectorPtr->pragma.journal_mode(journal_mode::WAL);  // WAL => write ahead log

    CleanUpShadowFiles();

    return Status::OK();
}

Status
SqliteMetaImpl::CreateTable(CollectionSchema& table_schema) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        if (table_schema.collection_id_ == "") {
            NextTableId(table_schema.collection_id_);
        } else {
            fiu_do_on("SqliteMetaImpl.CreateTable.throw_exception", throw std::exception());
            auto collection = ConnectorPtr->select(columns(&CollectionSchema::state_),
                                              where(c(&CollectionSchema::collection_id_) == table_schema.collection_id_));
            if (collection.size() == 1) {
                if (CollectionSchema::TO_DELETE == std::get<0>(collection[0])) {
                    return Status(DB_ERROR, "Collection already exists and it is in delete state, please wait a second");
                } else {
                    // Change from no error to already exist.
                    return Status(DB_ALREADY_EXIST, "Collection already exists");
                }
            }
        }

        table_schema.id_ = -1;
        table_schema.created_on_ = utils::GetMicroSecTimeStamp();

        try {
            fiu_do_on("SqliteMetaImpl.CreateTable.insert_throw_exception", throw std::exception());
            auto id = ConnectorPtr->insert(table_schema);
            table_schema.id_ = id;
        } catch (std::exception& e) {
            return HandleException("Encounter exception when create collection", e.what());
        }

        ENGINE_LOG_DEBUG << "Successfully create collection: " << table_schema.collection_id_;

        return utils::CreateTablePath(options_, table_schema.collection_id_);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection", e.what());
    }
}

Status
SqliteMetaImpl::DescribeTable(CollectionSchema& table_schema) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);
        fiu_do_on("SqliteMetaImpl.DescribeTable.throw_exception", throw std::exception());
        auto groups = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::state_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::engine_type_,
                    &CollectionSchema::index_params_, &CollectionSchema::metric_type_, &CollectionSchema::owner_table_,
                    &CollectionSchema::partition_tag_, &CollectionSchema::version_, &CollectionSchema::flush_lsn_),
            where(c(&CollectionSchema::collection_id_) == table_schema.collection_id_ and
                  c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        if (groups.size() == 1) {
            table_schema.id_ = std::get<0>(groups[0]);
            table_schema.state_ = std::get<1>(groups[0]);
            table_schema.dimension_ = std::get<2>(groups[0]);
            table_schema.created_on_ = std::get<3>(groups[0]);
            table_schema.flag_ = std::get<4>(groups[0]);
            table_schema.index_file_size_ = std::get<5>(groups[0]);
            table_schema.engine_type_ = std::get<6>(groups[0]);
            table_schema.index_params_ = std::get<7>(groups[0]);
            table_schema.metric_type_ = std::get<8>(groups[0]);
            table_schema.owner_table_ = std::get<9>(groups[0]);
            table_schema.partition_tag_ = std::get<10>(groups[0]);
            table_schema.version_ = std::get<11>(groups[0]);
            table_schema.flush_lsn_ = std::get<12>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + table_schema.collection_id_ + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::HasTable(const std::string& collection_id, bool& has_or_not) {
    has_or_not = false;

    try {
        fiu_do_on("SqliteMetaImpl.HasTable.throw_exception", throw std::exception());
        server::MetricCollector metric;
        auto tables = ConnectorPtr->select(
            columns(&CollectionSchema::id_),
            where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));
        if (tables.size() == 1) {
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
SqliteMetaImpl::AllTables(std::vector<CollectionSchema>& table_schema_array) {
    try {
        fiu_do_on("SqliteMetaImpl.AllTables.throw_exception", throw std::exception());
        server::MetricCollector metric;
        auto selected = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::collection_id_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::engine_type_,
                    &CollectionSchema::index_params_, &CollectionSchema::metric_type_, &CollectionSchema::owner_table_,
                    &CollectionSchema::partition_tag_, &CollectionSchema::version_, &CollectionSchema::flush_lsn_),
            where(c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE and c(&CollectionSchema::owner_table_) == ""));
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
            schema.owner_table_ = std::get<9>(collection);
            schema.partition_tag_ = std::get<10>(collection);
            schema.version_ = std::get<11>(collection);
            schema.flush_lsn_ = std::get<12>(collection);

            table_schema_array.emplace_back(schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup all tables", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropTable(const std::string& collection_id) {
    try {
        fiu_do_on("SqliteMetaImpl.DropTable.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // soft delete collection
        ConnectorPtr->update_all(
            set(c(&CollectionSchema::state_) = (int)CollectionSchema::TO_DELETE),
            where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        ENGINE_LOG_DEBUG << "Successfully delete collection, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DeleteTableFiles(const std::string& collection_id) {
    try {
        fiu_do_on("SqliteMetaImpl.DeleteTableFiles.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // soft delete collection files
        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_DELETE,
                                     c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));

        ENGINE_LOG_DEBUG << "Successfully delete collection files, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateTableFile(SegmentSchema& file_schema) {
    if (file_schema.date_ == EmptyDate) {
        file_schema.date_ = utils::GetDate();
    }
    CollectionSchema table_schema;
    table_schema.collection_id_ = file_schema.collection_id_;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        fiu_do_on("SqliteMetaImpl.CreateTableFile.throw_exception", throw std::exception());
        server::MetricCollector metric;

        NextFileId(file_schema.file_id_);
        if (file_schema.segment_id_.empty()) {
            file_schema.segment_id_ = file_schema.file_id_;
        }
        file_schema.dimension_ = table_schema.dimension_;
        file_schema.file_size_ = 0;
        file_schema.row_count_ = 0;
        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
        file_schema.updated_time_ = file_schema.created_on_;
        file_schema.index_file_size_ = table_schema.index_file_size_;
        file_schema.index_params_ = table_schema.index_params_;
        file_schema.engine_type_ = table_schema.engine_type_;
        file_schema.metric_type_ = table_schema.metric_type_;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto id = ConnectorPtr->insert(file_schema);
        file_schema.id_ = id;

        ENGINE_LOG_DEBUG << "Successfully create collection file, file id = " << file_schema.file_id_;
        return utils::CreateTableFilePath(options_, file_schema);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetTableFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                              SegmentsSchema& table_files) {
    try {
        fiu_do_on("SqliteMetaImpl.GetTableFiles.throw_exception", throw std::exception());

        table_files.clear();
        auto files = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::segment_id_, &SegmentSchema::file_id_,
                    &SegmentSchema::file_type_, &SegmentSchema::file_size_, &SegmentSchema::row_count_,
                    &SegmentSchema::date_, &SegmentSchema::engine_type_, &SegmentSchema::created_on_),
            where(c(&SegmentSchema::collection_id_) == collection_id and in(&SegmentSchema::id_, ids) and
                  c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));
        CollectionSchema table_schema;
        table_schema.collection_id_ = collection_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        Status result;
        for (auto& file : files) {
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
            file_schema.dimension_ = table_schema.dimension_;
            file_schema.index_file_size_ = table_schema.index_file_size_;
            file_schema.index_params_ = table_schema.index_params_;
            file_schema.metric_type_ = table_schema.metric_type_;

            utils::GetTableFilePath(options_, file_schema);

            table_files.emplace_back(file_schema);
        }

        ENGINE_LOG_DEBUG << "Get collection files by id";
        return result;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection files", e.what());
    }
}

Status
SqliteMetaImpl::GetTableFilesBySegmentId(const std::string& segment_id,
                                         milvus::engine::meta::SegmentsSchema& table_files) {
    try {
        table_files.clear();
        auto files = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_,
                    &SegmentSchema::created_on_),
            where(c(&SegmentSchema::segment_id_) == segment_id and
                  c(&SegmentSchema::file_type_) != (int)SegmentSchema::TO_DELETE));

        if (!files.empty()) {
            CollectionSchema table_schema;
            table_schema.collection_id_ = std::get<1>(files[0]);
            auto status = DescribeTable(table_schema);
            if (!status.ok()) {
                return status;
            }

            for (auto& file : files) {
                SegmentSchema file_schema;
                file_schema.collection_id_ = table_schema.collection_id_;
                file_schema.id_ = std::get<0>(file);
                file_schema.segment_id_ = std::get<2>(file);
                file_schema.file_id_ = std::get<3>(file);
                file_schema.file_type_ = std::get<4>(file);
                file_schema.file_size_ = std::get<5>(file);
                file_schema.row_count_ = std::get<6>(file);
                file_schema.date_ = std::get<7>(file);
                file_schema.engine_type_ = std::get<8>(file);
                file_schema.created_on_ = std::get<9>(file);
                file_schema.dimension_ = table_schema.dimension_;
                file_schema.index_file_size_ = table_schema.index_file_size_;
                file_schema.index_params_ = table_schema.index_params_;
                file_schema.metric_type_ = table_schema.metric_type_;

                utils::GetTableFilePath(options_, file_schema);
                table_files.emplace_back(file_schema);
            }
        }

        ENGINE_LOG_DEBUG << "Get collection files by segment id";
        return Status::OK();
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection files by segment id", e.what());
    }
}

Status
SqliteMetaImpl::UpdateTableFlag(const std::string& collection_id, int64_t flag) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateTableFlag.throw_exception", throw std::exception());

        // set all backup file to raw
        ConnectorPtr->update_all(set(c(&CollectionSchema::flag_) = flag), where(c(&CollectionSchema::collection_id_) == collection_id));
        ENGINE_LOG_DEBUG << "Successfully update collection flag, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection flag: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFlushLSN(const std::string& collection_id, uint64_t flush_lsn) {
    try {
        server::MetricCollector metric;

        ConnectorPtr->update_all(set(c(&CollectionSchema::flush_lsn_) = flush_lsn),
                                 where(c(&CollectionSchema::collection_id_) == collection_id));
        ENGINE_LOG_DEBUG << "Successfully update collection flush_lsn, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection lsn: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetTableFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) {
    try {
        server::MetricCollector metric;

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
SqliteMetaImpl::GetTableFilesByFlushLSN(uint64_t flush_lsn, SegmentsSchema& table_files) {
    table_files.clear();

    try {
        server::MetricCollector metric;

        auto selected = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_,
                    &SegmentSchema::created_on_),
            where(c(&SegmentSchema::flush_lsn_) == flush_lsn));

        std::map<std::string, CollectionSchema> groups;
        SegmentSchema table_file;

        Status ret;
        for (auto& file : selected) {
            table_file.id_ = std::get<0>(file);
            table_file.collection_id_ = std::get<1>(file);
            table_file.segment_id_ = std::get<2>(file);
            table_file.file_id_ = std::get<3>(file);
            table_file.file_type_ = std::get<4>(file);
            table_file.file_size_ = std::get<5>(file);
            table_file.row_count_ = std::get<6>(file);
            table_file.date_ = std::get<7>(file);
            table_file.engine_type_ = std::get<8>(file);
            table_file.created_on_ = std::get<9>(file);

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                ret = status;
            }
            auto groupItr = groups.find(table_file.collection_id_);
            if (groupItr == groups.end()) {
                CollectionSchema table_schema;
                table_schema.collection_id_ = table_file.collection_id_;
                auto status = DescribeTable(table_schema);
                if (!status.ok()) {
                    return status;
                }
                groups[table_file.collection_id_] = table_schema;
            }
            table_file.dimension_ = groups[table_file.collection_id_].dimension_;
            table_file.index_file_size_ = groups[table_file.collection_id_].index_file_size_;
            table_file.index_params_ = groups[table_file.collection_id_].index_params_;
            table_file.metric_type_ = groups[table_file.collection_id_].metric_type_;
            table_files.push_back(table_file);
        }

        if (selected.size() > 0) {
            ENGINE_LOG_DEBUG << "Collect " << selected.size() << " files with flush_lsn = " << flush_lsn;
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when getting collection files by flush_lsn", e.what());
    }
}

Status
SqliteMetaImpl::UpdateTableFile(SegmentSchema& file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateTableFile.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&CollectionSchema::state_),
                                           where(c(&CollectionSchema::collection_id_) == file_schema.collection_id_));

        // if the collection has been deleted, just mark the collection file as TO_DELETE
        // clean thread will delete the file later
        if (tables.size() < 1 || std::get<0>(tables[0]) == (int)CollectionSchema::TO_DELETE) {
            file_schema.file_type_ = SegmentSchema::TO_DELETE;
        }

        ConnectorPtr->update(file_schema);

        ENGINE_LOG_DEBUG << "Update single collection file, file id = " << file_schema.file_id_;
    } catch (std::exception& e) {
        std::string msg =
            "Exception update collection file: collection_id = " + file_schema.collection_id_ + " file_id = " + file_schema.file_id_;
        return HandleException(msg, e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFiles(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateTableFiles.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::map<std::string, bool> has_tables;
        for (auto& file : files) {
            if (has_tables.find(file.collection_id_) != has_tables.end()) {
                continue;
            }
            auto tables = ConnectorPtr->select(columns(&CollectionSchema::id_),
                                               where(c(&CollectionSchema::collection_id_) == file.collection_id_ and
                                                     c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));
            if (tables.size() >= 1) {
                has_tables[file.collection_id_] = true;
            } else {
                has_tables[file.collection_id_] = false;
            }
        }

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& file : files) {
                if (!has_tables[file.collection_id_]) {
                    file.file_type_ = SegmentSchema::TO_DELETE;
                }

                file.updated_time_ = utils::GetMicroSecTimeStamp();
                ConnectorPtr->update(file);
            }
            return true;
        });
        fiu_do_on("SqliteMetaImpl.UpdateTableFiles.fail_commited", commited = false);

        if (!commited) {
            return HandleException("UpdateTableFiles error: sqlite transaction failed");
        }

        ENGINE_LOG_DEBUG << "Update " << files.size() << " collection files";
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFilesRowCount(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        for (auto& file : files) {
            ConnectorPtr->update_all(set(c(&SegmentSchema::row_count_) = file.row_count_,
                                         c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                     where(c(&SegmentSchema::file_id_) == file.file_id_));
            ENGINE_LOG_DEBUG << "Update file " << file.file_id_ << " row count to " << file.row_count_;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files row count", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableIndex(const std::string& collection_id, const TableIndex& index) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateTableIndex.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(
            columns(&CollectionSchema::id_, &CollectionSchema::state_, &CollectionSchema::dimension_, &CollectionSchema::created_on_,
                    &CollectionSchema::flag_, &CollectionSchema::index_file_size_, &CollectionSchema::owner_table_,
                    &CollectionSchema::partition_tag_, &CollectionSchema::version_),
            where(c(&CollectionSchema::collection_id_) == collection_id and c(&CollectionSchema::state_) != (int)CollectionSchema::TO_DELETE));

        if (tables.size() > 0) {
            meta::CollectionSchema table_schema;
            table_schema.id_ = std::get<0>(tables[0]);
            table_schema.collection_id_ = collection_id;
            table_schema.state_ = std::get<1>(tables[0]);
            table_schema.dimension_ = std::get<2>(tables[0]);
            table_schema.created_on_ = std::get<3>(tables[0]);
            table_schema.flag_ = std::get<4>(tables[0]);
            table_schema.index_file_size_ = std::get<5>(tables[0]);
            table_schema.owner_table_ = std::get<6>(tables[0]);
            table_schema.partition_tag_ = std::get<7>(tables[0]);
            table_schema.version_ = std::get<8>(tables[0]);
            table_schema.engine_type_ = index.engine_type_;
            table_schema.index_params_ = index.extra_params_.dump();
            table_schema.metric_type_ = index.metric_type_;

            ConnectorPtr->update(table_schema);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }

        // set all backup file to raw
        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::RAW,
                                     c(&SegmentSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::file_type_) == (int)SegmentSchema::BACKUP));

        ENGINE_LOG_DEBUG << "Successfully update collection index, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection index: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFilesToIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateTableFilesToIndex.throw_exception", throw std::exception());

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(set(c(&SegmentSchema::file_type_) = (int)SegmentSchema::TO_INDEX),
                                 where(c(&SegmentSchema::collection_id_) == collection_id and
                                       c(&SegmentSchema::row_count_) >= meta::BUILD_INDEX_THRESHOLD and
                                       c(&SegmentSchema::file_type_) == (int)SegmentSchema::RAW));

        ENGINE_LOG_DEBUG << "Update files to to_index, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files to to_index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DescribeTableIndex(const std::string& collection_id, TableIndex& index) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DescribeTableIndex.throw_exception", throw std::exception());

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
SqliteMetaImpl::DropTableIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DropTableIndex.throw_exception", throw std::exception());

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

        ENGINE_LOG_DEBUG << "Successfully drop collection index, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreatePartition(const std::string& collection_id, const std::string& partition_name, const std::string& tag,
                                uint64_t lsn) {
    server::MetricCollector metric;

    CollectionSchema table_schema;
    table_schema.collection_id_ = collection_id;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    // not allow create partition under partition
    if (!table_schema.owner_table_.empty()) {
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
        NextTableId(table_schema.collection_id_);
    } else {
        table_schema.collection_id_ = partition_name;
    }

    table_schema.id_ = -1;
    table_schema.flag_ = 0;
    table_schema.created_on_ = utils::GetMicroSecTimeStamp();
    table_schema.owner_table_ = collection_id;
    table_schema.partition_tag_ = valid_tag;
    table_schema.flush_lsn_ = lsn;

    status = CreateTable(table_schema);
    if (status.code() == DB_ALREADY_EXIST) {
        return Status(DB_ALREADY_EXIST, "Partition already exists");
    }

    return status;
}

Status
SqliteMetaImpl::DropPartition(const std::string& partition_name) {
    return DropTable(partition_name);
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
            where(c(&CollectionSchema::owner_table_) == collection_id and
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
            partition_schema.owner_table_ = collection_id;
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
            where(c(&CollectionSchema::owner_table_) == collection_id and c(&CollectionSchema::partition_tag_) == valid_tag and
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

        auto select_columns =
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_);

        auto match_tableid = c(&SegmentSchema::collection_id_) == collection_id;

        std::vector<int> file_types = {(int)SegmentSchema::RAW, (int)SegmentSchema::TO_INDEX,
                                       (int)SegmentSchema::INDEX};
        auto match_type = in(&SegmentSchema::file_type_, file_types);

        CollectionSchema table_schema;
        table_schema.collection_id_ = collection_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        // perform query
        decltype(ConnectorPtr->select(select_columns)) selected;
        auto filter = where(match_tableid and match_type);
        selected = ConnectorPtr->select(select_columns, filter);

        Status ret;
        for (auto& file : selected) {
            SegmentSchema table_file;
            table_file.id_ = std::get<0>(file);
            table_file.collection_id_ = std::get<1>(file);
            table_file.segment_id_ = std::get<2>(file);
            table_file.file_id_ = std::get<3>(file);
            table_file.file_type_ = std::get<4>(file);
            table_file.file_size_ = std::get<5>(file);
            table_file.row_count_ = std::get<6>(file);
            table_file.date_ = std::get<7>(file);
            table_file.engine_type_ = std::get<8>(file);
            table_file.dimension_ = table_schema.dimension_;
            table_file.index_file_size_ = table_schema.index_file_size_;
            table_file.index_params_ = table_schema.index_params_;
            table_file.metric_type_ = table_schema.metric_type_;

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                ret = status;
            }

            files.emplace_back(table_file);
        }
        if (files.empty()) {
            ENGINE_LOG_ERROR << "No file to search for collection: " << collection_id;
        }

        if (selected.size() > 0) {
            ENGINE_LOG_DEBUG << "Collect " << selected.size() << " to-search files";
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
        CollectionSchema table_schema;
        table_schema.collection_id_ = collection_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        // get files to merge
        auto selected = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::created_on_),
            where(c(&SegmentSchema::file_type_) == (int)SegmentSchema::RAW and
                  c(&SegmentSchema::collection_id_) == collection_id),
            order_by(&SegmentSchema::file_size_).desc());

        Status result;
        int64_t to_merge_files = 0;
        for (auto& file : selected) {
            SegmentSchema table_file;
            table_file.file_size_ = std::get<5>(file);
            if (table_file.file_size_ >= table_schema.index_file_size_) {
                continue;  // skip large file
            }

            table_file.id_ = std::get<0>(file);
            table_file.collection_id_ = std::get<1>(file);
            table_file.segment_id_ = std::get<2>(file);
            table_file.file_id_ = std::get<3>(file);
            table_file.file_type_ = std::get<4>(file);
            table_file.row_count_ = std::get<6>(file);
            table_file.date_ = std::get<7>(file);
            table_file.created_on_ = std::get<8>(file);
            table_file.dimension_ = table_schema.dimension_;
            table_file.index_file_size_ = table_schema.index_file_size_;
            table_file.index_params_ = table_schema.index_params_;
            table_file.metric_type_ = table_schema.metric_type_;

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                result = status;
            }

            files.emplace_back(table_file);
            ++to_merge_files;
        }

        if (to_merge_files > 0) {
            ENGINE_LOG_TRACE << "Collect " << to_merge_files << " to-merge files";
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

        auto selected = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::collection_id_, &SegmentSchema::segment_id_,
                    &SegmentSchema::file_id_, &SegmentSchema::file_type_, &SegmentSchema::file_size_,
                    &SegmentSchema::row_count_, &SegmentSchema::date_, &SegmentSchema::engine_type_,
                    &SegmentSchema::created_on_),
            where(c(&SegmentSchema::file_type_) == (int)SegmentSchema::TO_INDEX));

        std::map<std::string, CollectionSchema> groups;
        SegmentSchema table_file;

        Status ret;
        for (auto& file : selected) {
            table_file.id_ = std::get<0>(file);
            table_file.collection_id_ = std::get<1>(file);
            table_file.segment_id_ = std::get<2>(file);
            table_file.file_id_ = std::get<3>(file);
            table_file.file_type_ = std::get<4>(file);
            table_file.file_size_ = std::get<5>(file);
            table_file.row_count_ = std::get<6>(file);
            table_file.date_ = std::get<7>(file);
            table_file.engine_type_ = std::get<8>(file);
            table_file.created_on_ = std::get<9>(file);

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                ret = status;
            }
            auto groupItr = groups.find(table_file.collection_id_);
            if (groupItr == groups.end()) {
                CollectionSchema table_schema;
                table_schema.collection_id_ = table_file.collection_id_;
                auto status = DescribeTable(table_schema);
                fiu_do_on("SqliteMetaImpl_FilesToIndex_TableNotFound",
                          status = Status(DB_NOT_FOUND, "collection not found"));
                if (!status.ok()) {
                    return status;
                }
                groups[table_file.collection_id_] = table_schema;
            }
            table_file.dimension_ = groups[table_file.collection_id_].dimension_;
            table_file.index_file_size_ = groups[table_file.collection_id_].index_file_size_;
            table_file.index_params_ = groups[table_file.collection_id_].index_params_;
            table_file.metric_type_ = groups[table_file.collection_id_].metric_type_;
            files.push_back(table_file);
        }

        if (selected.size() > 0) {
            ENGINE_LOG_DEBUG << "Collect " << selected.size() << " to-index files";
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

    CollectionSchema table_schema;
    table_schema.collection_id_ = collection_id;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        fiu_do_on("SqliteMetaImpl.FilesByType.throw_exception", throw std::exception());

        files.clear();
        auto selected = ConnectorPtr->select(
            columns(&SegmentSchema::id_, &SegmentSchema::segment_id_, &SegmentSchema::file_id_,
                    &SegmentSchema::file_type_, &SegmentSchema::file_size_, &SegmentSchema::row_count_,
                    &SegmentSchema::date_, &SegmentSchema::engine_type_, &SegmentSchema::created_on_),
            where(in(&SegmentSchema::file_type_, file_types) and c(&SegmentSchema::collection_id_) == collection_id));

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

                file_schema.dimension_ = table_schema.dimension_;
                file_schema.index_file_size_ = table_schema.index_file_size_;
                file_schema.index_params_ = table_schema.index_params_;
                file_schema.metric_type_ = table_schema.metric_type_;

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

                auto status = utils::GetTableFilePath(options_, file_schema);
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
            ENGINE_LOG_DEBUG << msg;
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
        selected = ConnectorPtr->select(select_columns, filter);

        std::map<std::string, meta::CollectionSchema> tables;
        Status ret;
        for (auto& file : selected) {
            SegmentSchema table_file;
            table_file.id_ = std::get<0>(file);
            table_file.collection_id_ = std::get<1>(file);
            table_file.segment_id_ = std::get<2>(file);
            table_file.file_id_ = std::get<3>(file);
            table_file.file_type_ = std::get<4>(file);
            table_file.file_size_ = std::get<5>(file);
            table_file.row_count_ = std::get<6>(file);
            table_file.date_ = std::get<7>(file);
            table_file.engine_type_ = std::get<8>(file);

            if (tables.find(table_file.collection_id_) == tables.end()) {
                CollectionSchema table_schema;
                table_schema.collection_id_ = table_file.collection_id_;
                auto status = DescribeTable(table_schema);
                if (!status.ok()) {
                    return status;
                }
                tables.insert(std::make_pair(table_file.collection_id_, table_schema));
            }

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                ret = status;
            }

            files.emplace_back(table_file);
        }

        for (auto& table_file : files) {
            CollectionSchema& table_schema = tables[table_file.collection_id_];
            table_file.dimension_ = table_schema.dimension_;
            table_file.index_file_size_ = table_schema.index_file_size_;
            table_file.index_params_ = table_schema.index_params_;
            table_file.metric_type_ = table_schema.metric_type_;
        }

        if (files.empty()) {
            ENGINE_LOG_ERROR << "No file to search in file id list";
        } else {
            ENGINE_LOG_DEBUG << "Collect " << selected.size() << " files by id";
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

            ENGINE_LOG_DEBUG << "Archive old files";
        }
        if (criteria == engine::ARCHIVE_CONF_DISK) {
            uint64_t sum = 0;
            Size(sum);

            int64_t to_delete = (int64_t)sum - limit * G;
            DiscardFiles(to_delete);

            ENGINE_LOG_DEBUG << "Archive files to free disk";
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
                ENGINE_LOG_DEBUG << "Remove collection file type as NEW";
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
            ENGINE_LOG_DEBUG << "Clean " << files.size() << " files";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpFilesWithTTL(uint64_t seconds /*, CleanUpFilter* filter*/) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> table_ids;
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
            SegmentSchema table_file;
            for (auto& file : files) {
                table_file.id_ = std::get<0>(file);
                table_file.collection_id_ = std::get<1>(file);
                table_file.segment_id_ = std::get<2>(file);
                table_file.engine_type_ = std::get<3>(file);
                table_file.file_id_ = std::get<4>(file);
                table_file.file_type_ = std::get<5>(file);
                table_file.date_ = std::get<6>(file);

                // check if the file can be deleted
                if (OngoingFileChecker::GetInstance().IsIgnored(table_file)) {
                    ENGINE_LOG_DEBUG << "File:" << table_file.file_id_
                                     << " currently is in use, not able to delete now";
                    continue;  // ignore this file, don't delete it
                }

                // erase from cache, must do this before file deleted,
                // because GetTableFilePath won't able to generate file path after the file is deleted
                // TODO(zhiru): clean up
                utils::GetTableFilePath(options_, table_file);
                server::CommonUtil::EraseFromCache(table_file.location_);

                if (table_file.file_type_ == (int)SegmentSchema::TO_DELETE) {
                    // delete file from meta
                    ConnectorPtr->remove<SegmentSchema>(table_file.id_);

                    // delete file from disk storage
                    utils::DeleteTableFilePath(options_, table_file);

                    ENGINE_LOG_DEBUG << "Remove file id:" << table_file.file_id_ << " location:"
                                     << table_file.location_;
                    table_ids.insert(table_file.collection_id_);
                    segment_ids.insert(std::make_pair(table_file.segment_id_, table_file));

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
            ENGINE_LOG_DEBUG << "Clean " << clean_files << " files expired in " << seconds << " seconds";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection files", e.what());
    }

    // remove to_delete tables
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveTable_ThrowException", throw std::exception());
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&CollectionSchema::id_, &CollectionSchema::collection_id_),
                                           where(c(&CollectionSchema::state_) == (int)CollectionSchema::TO_DELETE));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& collection : tables) {
                utils::DeleteTablePath(options_, std::get<1>(collection), false);  // only delete empty folder
                ConnectorPtr->remove<CollectionSchema>(std::get<0>(collection));
            }

            return true;
        });
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveTable_Failcommited", commited = false);

        if (!commited) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }

        if (tables.size() > 0) {
            ENGINE_LOG_DEBUG << "Remove " << tables.size() << " tables from meta";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection files", e.what());
    }

    // remove deleted collection folder
    // don't remove collection folder until all its files has been deleted
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveTableFolder_ThrowException", throw std::exception());
        server::MetricCollector metric;

        int64_t remove_tables = 0;
        for (auto& collection_id : table_ids) {
            auto selected = ConnectorPtr->select(columns(&SegmentSchema::file_id_),
                                                 where(c(&SegmentSchema::collection_id_) == collection_id));
            if (selected.size() == 0) {
                utils::DeleteTablePath(options_, collection_id);
                ++remove_tables;
            }
        }

        if (remove_tables) {
            ENGINE_LOG_DEBUG << "Remove " << remove_tables << " tables folder";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    // remove deleted segment folder
    // don't remove segment folder until all its tablefiles has been deleted
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
                ENGINE_LOG_DEBUG << "Remove segment directory: " << segment_dir;
                ++remove_segments;
            }
        }

        if (remove_segments > 0) {
            ENGINE_LOG_DEBUG << "Remove " << remove_segments << " segments folder";
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
        auto selected = ConnectorPtr->select(
            columns(&SegmentSchema::row_count_),
            where(in(&SegmentSchema::file_type_, file_types) and c(&SegmentSchema::collection_id_) == collection_id));

        CollectionSchema table_schema;
        table_schema.collection_id_ = collection_id;
        auto status = DescribeTable(table_schema);

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
    ENGINE_LOG_DEBUG << "Drop all sqlite meta";

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

    ENGINE_LOG_DEBUG << "About to discard size=" << to_discard_size;

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
            SegmentSchema table_file;

            for (auto& file : selected) {
                if (to_discard_size <= 0)
                    break;
                table_file.id_ = std::get<0>(file);
                table_file.file_size_ = std::get<1>(file);
                ids.push_back(table_file.id_);
                ENGINE_LOG_DEBUG << "Discard table_file.id=" << table_file.file_id_
                                 << " table_file.size=" << table_file.file_size_;
                to_discard_size -= table_file.file_size_;
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

        ENGINE_LOG_DEBUG << "Update global lsn = " << lsn;
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

}  // namespace meta
}  // namespace engine
}  // namespace milvus
