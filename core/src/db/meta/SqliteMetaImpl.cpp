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

#include "db/meta/SqliteMetaImpl.h"
#include "MetaConsts.h"
#include "db/IDGenerator.h"
#include "db/Utils.h"
#include "metrics/Metrics.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

#include <sqlite_orm.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>

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
    return make_storage(path,
                        make_table(META_TABLES,
                                   make_column("id", &TableSchema::id_, primary_key()),
                                   make_column("table_id", &TableSchema::table_id_, unique()),
                                   make_column("state", &TableSchema::state_),
                                   make_column("dimension", &TableSchema::dimension_),
                                   make_column("created_on", &TableSchema::created_on_),
                                   make_column("flag", &TableSchema::flag_, default_value(0)),
                                   make_column("index_file_size", &TableSchema::index_file_size_),
                                   make_column("engine_type", &TableSchema::engine_type_),
                                   make_column("nlist", &TableSchema::nlist_),
                                   make_column("metric_type", &TableSchema::metric_type_),
                                   make_column("owner_table", &TableSchema::owner_table_, default_value("")),
                                   make_column("partition_tag", &TableSchema::partition_tag_, default_value("")),
                                   make_column("version", &TableSchema::version_, default_value(CURRENT_VERSION))),
                        make_table(META_TABLEFILES,
                                   make_column("id", &TableFileSchema::id_, primary_key()),
                                   make_column("table_id", &TableFileSchema::table_id_),
                                   make_column("engine_type", &TableFileSchema::engine_type_),
                                   make_column("file_id", &TableFileSchema::file_id_),
                                   make_column("file_type", &TableFileSchema::file_type_),
                                   make_column("file_size", &TableFileSchema::file_size_, default_value(0)),
                                   make_column("row_count", &TableFileSchema::row_count_, default_value(0)),
                                   make_column("updated_time", &TableFileSchema::updated_time_),
                                   make_column("created_on", &TableFileSchema::created_on_),
                                   make_column("date", &TableFileSchema::date_)));
}

using ConnectorT = decltype(StoragePrototype(""));
static std::unique_ptr<ConnectorT> ConnectorPtr;

SqliteMetaImpl::SqliteMetaImpl(const DBMetaOptions& options) : options_(options) {
    Initialize();
}

SqliteMetaImpl::~SqliteMetaImpl() {
}

Status
SqliteMetaImpl::NextTableId(std::string& table_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    table_id = ss.str();
    return Status::OK();
}

Status
SqliteMetaImpl::NextFileId(std::string& file_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    file_id = ss.str();
    return Status::OK();
}

void
SqliteMetaImpl::ValidateMetaSchema() {
    if (ConnectorPtr == nullptr) {
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
SqliteMetaImpl::CreateTable(TableSchema& table_schema) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        if (table_schema.table_id_ == "") {
            NextTableId(table_schema.table_id_);
        } else {
            auto table = ConnectorPtr->select(columns(&TableSchema::state_),
                                              where(c(&TableSchema::table_id_) == table_schema.table_id_));
            if (table.size() == 1) {
                if (TableSchema::TO_DELETE == std::get<0>(table[0])) {
                    return Status(DB_ERROR, "Table already exists and it is in delete state, please wait a second");
                } else {
                    // Change from no error to already exist.
                    return Status(DB_ALREADY_EXIST, "Table already exists");
                }
            }
        }

        table_schema.id_ = -1;
        table_schema.created_on_ = utils::GetMicroSecTimeStamp();

        try {
            auto id = ConnectorPtr->insert(table_schema);
            table_schema.id_ = id;
        } catch (std::exception& e) {
            return HandleException("Encounter exception when create table", e.what());
        }

        ENGINE_LOG_DEBUG << "Successfully create table: " << table_schema.table_id_;

        return utils::CreateTablePath(options_, table_schema.table_id_);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create table", e.what());
    }
}

Status
SqliteMetaImpl::DescribeTable(TableSchema& table_schema) {
    try {
        server::MetricCollector metric;

        auto groups = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::state_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::created_on_,
                                                   &TableSchema::flag_,
                                                   &TableSchema::index_file_size_,
                                                   &TableSchema::engine_type_,
                                                   &TableSchema::nlist_,
                                                   &TableSchema::metric_type_,
                                                   &TableSchema::owner_table_,
                                                   &TableSchema::partition_tag_,
                                                   &TableSchema::version_),
                                           where(c(&TableSchema::table_id_) == table_schema.table_id_
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        if (groups.size() == 1) {
            table_schema.id_ = std::get<0>(groups[0]);
            table_schema.state_ = std::get<1>(groups[0]);
            table_schema.dimension_ = std::get<2>(groups[0]);
            table_schema.created_on_ = std::get<3>(groups[0]);
            table_schema.flag_ = std::get<4>(groups[0]);
            table_schema.index_file_size_ = std::get<5>(groups[0]);
            table_schema.engine_type_ = std::get<6>(groups[0]);
            table_schema.nlist_ = std::get<7>(groups[0]);
            table_schema.metric_type_ = std::get<8>(groups[0]);
            table_schema.owner_table_ = std::get<9>(groups[0]);
            table_schema.partition_tag_ = std::get<10>(groups[0]);
            table_schema.version_ = std::get<11>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_schema.table_id_ + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe table", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::HasTable(const std::string& table_id, bool& has_or_not) {
    has_or_not = false;

    try {
        server::MetricCollector metric;
        auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
        if (tables.size() == 1) {
            has_or_not = true;
        } else {
            has_or_not = false;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup table", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::AllTables(std::vector<TableSchema>& table_schema_array) {
    try {
        server::MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableSchema::id_,
                                                     &TableSchema::table_id_,
                                                     &TableSchema::dimension_,
                                                     &TableSchema::created_on_,
                                                     &TableSchema::flag_,
                                                     &TableSchema::index_file_size_,
                                                     &TableSchema::engine_type_,
                                                     &TableSchema::nlist_,
                                                     &TableSchema::metric_type_,
                                                     &TableSchema::owner_table_,
                                                     &TableSchema::partition_tag_,
                                                     &TableSchema::version_),
                                             where(c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
        for (auto& table : selected) {
            TableSchema schema;
            schema.id_ = std::get<0>(table);
            schema.table_id_ = std::get<1>(table);
            schema.dimension_ = std::get<2>(table);
            schema.created_on_ = std::get<3>(table);
            schema.flag_ = std::get<4>(table);
            schema.index_file_size_ = std::get<5>(table);
            schema.engine_type_ = std::get<6>(table);
            schema.nlist_ = std::get<7>(table);
            schema.metric_type_ = std::get<8>(table);
            schema.owner_table_ = std::get<9>(table);
            schema.partition_tag_ = std::get<10>(table);
            schema.version_ = std::get<11>(table);

            table_schema_array.emplace_back(schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup all tables", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropTable(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete table
        ConnectorPtr->update_all(
            set(
                c(&TableSchema::state_) = (int)TableSchema::TO_DELETE),
            where(
                c(&TableSchema::table_id_) == table_id and
                c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        ENGINE_LOG_DEBUG << "Successfully delete table, table id = " << table_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete table", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DeleteTableFiles(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete table files
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int)TableFileSchema::TO_DELETE,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                c(&TableFileSchema::file_type_) != (int)TableFileSchema::TO_DELETE));

        ENGINE_LOG_DEBUG << "Successfully delete table files, table id = " << table_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete table files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateTableFile(TableFileSchema& file_schema) {
    if (file_schema.date_ == EmptyDate) {
        file_schema.date_ = utils::GetDate();
    }
    TableSchema table_schema;
    table_schema.table_id_ = file_schema.table_id_;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        server::MetricCollector metric;

        NextFileId(file_schema.file_id_);
        file_schema.dimension_ = table_schema.dimension_;
        file_schema.file_size_ = 0;
        file_schema.row_count_ = 0;
        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
        file_schema.updated_time_ = file_schema.created_on_;
        file_schema.index_file_size_ = table_schema.index_file_size_;
        file_schema.engine_type_ = table_schema.engine_type_;
        file_schema.nlist_ = table_schema.nlist_;
        file_schema.metric_type_ = table_schema.metric_type_;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto id = ConnectorPtr->insert(file_schema);
        file_schema.id_ = id;

        ENGINE_LOG_DEBUG << "Successfully create table file, file id = " << file_schema.file_id_;
        return utils::CreateTableFilePath(options_, file_schema);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create table file", e.what());
    }

    return Status::OK();
}

// TODO(myh): Delete single vecotor by id
Status
SqliteMetaImpl::DropDataByDate(const std::string& table_id,
                               const DatesT& dates) {
    if (dates.empty()) {
        return Status::OK();
    }

    TableSchema table_schema;
    table_schema.table_id_ = table_id;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        // sqlite_orm has a bug, 'in' statement cannot handle too many elements
        // so we split one query into multi-queries, this is a work-around!!
        std::vector<DatesT> split_dates;
        split_dates.push_back(DatesT());
        const size_t batch_size = 30;
        for (DateT date : dates) {
            DatesT& last_batch = *split_dates.rbegin();
            last_batch.push_back(date);
            if (last_batch.size() > batch_size) {
                split_dates.push_back(DatesT());
            }
        }

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        for (auto& batch_dates : split_dates) {
            if (batch_dates.empty()) {
                continue;
            }

            ConnectorPtr->update_all(
                set(c(&TableFileSchema::file_type_) = (int)TableFileSchema::TO_DELETE,
                    c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                where(c(&TableFileSchema::table_id_) == table_id and in(&TableFileSchema::date_, batch_dates)));
        }

        ENGINE_LOG_DEBUG << "Successfully drop data by date, table id = " << table_schema.table_id_;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when drop partition", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetTableFiles(const std::string& table_id,
                              const std::vector<size_t>& ids,
                              TableFilesSchema& table_files) {
    try {
        table_files.clear();
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                  &TableFileSchema::file_id_,
                                                  &TableFileSchema::file_type_,
                                                  &TableFileSchema::file_size_,
                                                  &TableFileSchema::row_count_,
                                                  &TableFileSchema::date_,
                                                  &TableFileSchema::engine_type_,
                                                  &TableFileSchema::created_on_),
                                          where(c(&TableFileSchema::table_id_) == table_id and
                                                in(&TableFileSchema::id_, ids) and
                                                c(&TableFileSchema::file_type_) != (int)TableFileSchema::TO_DELETE));
        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        Status result;
        for (auto& file : files) {
            TableFileSchema file_schema;
            file_schema.table_id_ = table_id;
            file_schema.id_ = std::get<0>(file);
            file_schema.file_id_ = std::get<1>(file);
            file_schema.file_type_ = std::get<2>(file);
            file_schema.file_size_ = std::get<3>(file);
            file_schema.row_count_ = std::get<4>(file);
            file_schema.date_ = std::get<5>(file);
            file_schema.engine_type_ = std::get<6>(file);
            file_schema.created_on_ = std::get<7>(file);
            file_schema.dimension_ = table_schema.dimension_;
            file_schema.index_file_size_ = table_schema.index_file_size_;
            file_schema.nlist_ = table_schema.nlist_;
            file_schema.metric_type_ = table_schema.metric_type_;

            utils::GetTableFilePath(options_, file_schema);

            table_files.emplace_back(file_schema);
        }

        ENGINE_LOG_DEBUG << "Get table files by id";
        return result;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup table files", e.what());
    }
}

Status
SqliteMetaImpl::UpdateTableFlag(const std::string& table_id, int64_t flag) {
    try {
        server::MetricCollector metric;

        //set all backup file to raw
        ConnectorPtr->update_all(
            set(
                c(&TableSchema::flag_) = flag),
            where(
                c(&TableSchema::table_id_) == table_id));
        ENGINE_LOG_DEBUG << "Successfully update table flag, table id = " << table_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update table flag: table_id = " + table_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFile(TableFileSchema& file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::state_),
                                           where(c(&TableSchema::table_id_) == file_schema.table_id_));

        //if the table has been deleted, just mark the table file as TO_DELETE
        //clean thread will delete the file later
        if (tables.size() < 1 || std::get<0>(tables[0]) == (int)TableSchema::TO_DELETE) {
            file_schema.file_type_ = TableFileSchema::TO_DELETE;
        }

        ConnectorPtr->update(file_schema);

        ENGINE_LOG_DEBUG << "Update single table file, file id = " << file_schema.file_id_;
    } catch (std::exception& e) {
        std::string msg = "Exception update table file: table_id = " + file_schema.table_id_
                          + " file_id = " + file_schema.file_id_;
        return HandleException(msg, e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFiles(TableFilesSchema& files) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::map<std::string, bool> has_tables;
        for (auto& file : files) {
            if (has_tables.find(file.table_id_) != has_tables.end()) {
                continue;
            }
            auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
                                               where(c(&TableSchema::table_id_) == file.table_id_
                                                     and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
            if (tables.size() >= 1) {
                has_tables[file.table_id_] = true;
            } else {
                has_tables[file.table_id_] = false;
            }
        }

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& file : files) {
                if (!has_tables[file.table_id_]) {
                    file.file_type_ = TableFileSchema::TO_DELETE;
                }

                file.updated_time_ = utils::GetMicroSecTimeStamp();
                ConnectorPtr->update(file);
            }
            return true;
        });

        if (!commited) {
            return HandleException("UpdateTableFiles error: sqlite transaction failed");
        }

        ENGINE_LOG_DEBUG << "Update " << files.size() << " table files";
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update table files", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableIndex(const std::string& table_id, const TableIndex& index) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::state_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::created_on_,
                                                   &TableSchema::flag_,
                                                   &TableSchema::index_file_size_,
                                                   &TableSchema::owner_table_,
                                                   &TableSchema::partition_tag_,
                                                   &TableSchema::version_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        if (tables.size() > 0) {
            meta::TableSchema table_schema;
            table_schema.id_ = std::get<0>(tables[0]);
            table_schema.table_id_ = table_id;
            table_schema.state_ = std::get<1>(tables[0]);
            table_schema.dimension_ = std::get<2>(tables[0]);
            table_schema.created_on_ = std::get<3>(tables[0]);
            table_schema.flag_ = std::get<4>(tables[0]);
            table_schema.index_file_size_ = std::get<5>(tables[0]);
            table_schema.owner_table_ = std::get<6>(tables[0]);
            table_schema.partition_tag_ = std::get<7>(tables[0]);
            table_schema.version_ = std::get<8>(tables[0]);
            table_schema.engine_type_ = index.engine_type_;
            table_schema.nlist_ = index.nlist_;
            table_schema.metric_type_ = index.metric_type_;

            ConnectorPtr->update(table_schema);
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_id + " not found");
        }

        //set all backup file to raw
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int)TableFileSchema::RAW,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                c(&TableFileSchema::file_type_) == (int)TableFileSchema::BACKUP));

        ENGINE_LOG_DEBUG << "Successfully update table index, table id = " << table_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update table index: table_id = " + table_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFilesToIndex(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int)TableFileSchema::TO_INDEX),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                c(&TableFileSchema::row_count_) >= meta::BUILD_INDEX_THRESHOLD and
                c(&TableFileSchema::file_type_) == (int)TableFileSchema::RAW));

        ENGINE_LOG_DEBUG << "Update files to to_index, table id = " << table_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update table files to to_index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DescribeTableIndex(const std::string& table_id, TableIndex& index) {
    try {
        server::MetricCollector metric;

        auto groups = ConnectorPtr->select(columns(&TableSchema::engine_type_,
                                                   &TableSchema::nlist_,
                                                   &TableSchema::metric_type_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        if (groups.size() == 1) {
            index.engine_type_ = std::get<0>(groups[0]);
            index.nlist_ = std::get<1>(groups[0]);
            index.metric_type_ = std::get<2>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_id + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropTableIndex(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete index files
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int)TableFileSchema::TO_DELETE,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                c(&TableFileSchema::file_type_) == (int)TableFileSchema::INDEX));

        //set all backup file to raw
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int)TableFileSchema::RAW,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                c(&TableFileSchema::file_type_) == (int)TableFileSchema::BACKUP));

        //set table index type to raw
        ConnectorPtr->update_all(
            set(
                c(&TableSchema::engine_type_) = DEFAULT_ENGINE_TYPE,
                c(&TableSchema::nlist_) = DEFAULT_NLIST),
            where(
                c(&TableSchema::table_id_) == table_id));

        ENGINE_LOG_DEBUG << "Successfully drop table index, table id = " << table_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete table index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreatePartition(const std::string& table_id,
                                const std::string& partition_name,
                                const std::string& tag) {
    server::MetricCollector metric;

    TableSchema table_schema;
    table_schema.table_id_ = table_id;
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
    GetPartitionName(table_id, valid_tag, exist_partition);
    if (!exist_partition.empty()) {
        return Status(DB_ERROR, "Duplicate partition is not allowed");
    }

    if (partition_name == "") {
        // generate unique partition name
        NextTableId(table_schema.table_id_);
    } else {
        table_schema.table_id_ = partition_name;
    }

    table_schema.id_ = -1;
    table_schema.flag_ = 0;
    table_schema.created_on_ = utils::GetMicroSecTimeStamp();
    table_schema.owner_table_ = table_id;
    table_schema.partition_tag_ = valid_tag;

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
SqliteMetaImpl::ShowPartitions(const std::string& table_id, std::vector<meta::TableSchema>& partition_schema_array) {
    try {
        server::MetricCollector metric;

        auto partitions = ConnectorPtr->select(columns(&TableSchema::table_id_),
                                               where(c(&TableSchema::owner_table_) == table_id
                                                     and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
        for (size_t i = 0; i < partitions.size(); i++) {
            std::string partition_name = std::get<0>(partitions[i]);
            meta::TableSchema partition_schema;
            partition_schema.table_id_ = partition_name;
            DescribeTable(partition_schema);
            partition_schema_array.emplace_back(partition_schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when show partitions", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetPartitionName(const std::string& table_id, const std::string& tag, std::string& partition_name) {
    try {
        server::MetricCollector metric;

        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        auto name = ConnectorPtr->select(columns(&TableSchema::table_id_),
                                         where(c(&TableSchema::owner_table_) == table_id
                                               and c(&TableSchema::partition_tag_) == valid_tag
                                               and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
        if (name.size() > 0) {
            partition_name = std::get<0>(name[0]);
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_id + "'s partition " + valid_tag + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when get partition name", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::FilesToSearch(const std::string& table_id,
                              const std::vector<size_t>& ids,
                              const DatesT& dates,
                              DatePartionedTableFilesSchema& files) {
    files.clear();
    server::MetricCollector metric;

    try {
        auto select_columns =
            columns(&TableFileSchema::id_, &TableFileSchema::table_id_, &TableFileSchema::file_id_,
                    &TableFileSchema::file_type_, &TableFileSchema::file_size_, &TableFileSchema::row_count_,
                    &TableFileSchema::date_, &TableFileSchema::engine_type_);

        auto match_tableid = c(&TableFileSchema::table_id_) == table_id;

        std::vector<int> file_types = {(int)TableFileSchema::RAW, (int)TableFileSchema::TO_INDEX,
                                       (int)TableFileSchema::INDEX};
        auto match_type = in(&TableFileSchema::file_type_, file_types);

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        // sqlite_orm has a bug, 'in' statement cannot handle too many elements
        // so we split one query into multi-queries, this is a work-around!!
        std::vector<DatesT> split_dates;
        split_dates.push_back(DatesT());
        const size_t batch_size = 30;
        for (DateT date : dates) {
            DatesT& last_batch = *split_dates.rbegin();
            last_batch.push_back(date);
            if (last_batch.size() > batch_size) {
                split_dates.push_back(DatesT());
            }
        }

        // perform query
        decltype(ConnectorPtr->select(select_columns)) selected;
        if (dates.empty() && ids.empty()) {
            auto filter = where(match_tableid and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        } else if (dates.empty() && !ids.empty()) {
            auto match_fileid = in(&TableFileSchema::id_, ids);
            auto filter = where(match_tableid and match_fileid and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        } else if (!dates.empty() && ids.empty()) {
            for (auto& batch_dates : split_dates) {
                if (batch_dates.empty()) {
                    continue;
                }
                auto match_date = in(&TableFileSchema::date_, batch_dates);
                auto filter = where(match_tableid and match_date and match_type);
                auto batch_selected = ConnectorPtr->select(select_columns, filter);
                for (auto& file : batch_selected) {
                    selected.push_back(file);
                }
            }

        } else if (!dates.empty() && !ids.empty()) {
            for (auto& batch_dates : split_dates) {
                if (batch_dates.empty()) {
                    continue;
                }
                auto match_fileid = in(&TableFileSchema::id_, ids);
                auto match_date = in(&TableFileSchema::date_, batch_dates);
                auto filter = where(match_tableid and match_fileid and match_date and match_type);
                auto batch_selected = ConnectorPtr->select(select_columns, filter);
                for (auto& file : batch_selected) {
                    selected.push_back(file);
                }
            }
        }

        Status ret;
        TableFileSchema table_file;
        for (auto& file : selected) {
            table_file.id_ = std::get<0>(file);
            table_file.table_id_ = std::get<1>(file);
            table_file.file_id_ = std::get<2>(file);
            table_file.file_type_ = std::get<3>(file);
            table_file.file_size_ = std::get<4>(file);
            table_file.row_count_ = std::get<5>(file);
            table_file.date_ = std::get<6>(file);
            table_file.engine_type_ = std::get<7>(file);
            table_file.dimension_ = table_schema.dimension_;
            table_file.index_file_size_ = table_schema.index_file_size_;
            table_file.nlist_ = table_schema.nlist_;
            table_file.metric_type_ = table_schema.metric_type_;

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                ret = status;
            }

            auto dateItr = files.find(table_file.date_);
            if (dateItr == files.end()) {
                files[table_file.date_] = TableFilesSchema();
            }
            files[table_file.date_].push_back(table_file);
        }
        if (files.empty()) {
            ENGINE_LOG_ERROR << "No file to search for table: " << table_id;
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
SqliteMetaImpl::FilesToMerge(const std::string& table_id, DatePartionedTableFilesSchema& files) {
    files.clear();

    try {
        server::MetricCollector metric;

        // check table existence
        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        // get files to merge
        auto selected = ConnectorPtr->select(
            columns(&TableFileSchema::id_, &TableFileSchema::table_id_, &TableFileSchema::file_id_,
                    &TableFileSchema::file_type_, &TableFileSchema::file_size_, &TableFileSchema::row_count_,
                    &TableFileSchema::date_, &TableFileSchema::created_on_),
            where(c(&TableFileSchema::file_type_) == (int)TableFileSchema::RAW and
                  c(&TableFileSchema::table_id_) == table_id),
            order_by(&TableFileSchema::file_size_).desc());

        Status result;
        int64_t to_merge_files = 0;
        for (auto& file : selected) {
            TableFileSchema table_file;
            table_file.file_size_ = std::get<4>(file);
            if (table_file.file_size_ >= table_schema.index_file_size_) {
                continue;  // skip large file
            }

            table_file.id_ = std::get<0>(file);
            table_file.table_id_ = std::get<1>(file);
            table_file.file_id_ = std::get<2>(file);
            table_file.file_type_ = std::get<3>(file);
            table_file.row_count_ = std::get<5>(file);
            table_file.date_ = std::get<6>(file);
            table_file.created_on_ = std::get<7>(file);
            table_file.dimension_ = table_schema.dimension_;
            table_file.index_file_size_ = table_schema.index_file_size_;
            table_file.nlist_ = table_schema.nlist_;
            table_file.metric_type_ = table_schema.metric_type_;

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                result = status;
            }

            auto dateItr = files.find(table_file.date_);
            if (dateItr == files.end()) {
                files[table_file.date_] = TableFilesSchema();
            }

            files[table_file.date_].push_back(table_file);
            to_merge_files++;
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
SqliteMetaImpl::FilesToIndex(TableFilesSchema& files) {
    files.clear();

    try {
        server::MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                     &TableFileSchema::table_id_,
                                                     &TableFileSchema::file_id_,
                                                     &TableFileSchema::file_type_,
                                                     &TableFileSchema::file_size_,
                                                     &TableFileSchema::row_count_,
                                                     &TableFileSchema::date_,
                                                     &TableFileSchema::engine_type_,
                                                     &TableFileSchema::created_on_),
                                             where(c(&TableFileSchema::file_type_)
                                                   == (int)TableFileSchema::TO_INDEX));

        std::map<std::string, TableSchema> groups;
        TableFileSchema table_file;

        Status ret;
        for (auto& file : selected) {
            table_file.id_ = std::get<0>(file);
            table_file.table_id_ = std::get<1>(file);
            table_file.file_id_ = std::get<2>(file);
            table_file.file_type_ = std::get<3>(file);
            table_file.file_size_ = std::get<4>(file);
            table_file.row_count_ = std::get<5>(file);
            table_file.date_ = std::get<6>(file);
            table_file.engine_type_ = std::get<7>(file);
            table_file.created_on_ = std::get<8>(file);

            auto status = utils::GetTableFilePath(options_, table_file);
            if (!status.ok()) {
                ret = status;
            }
            auto groupItr = groups.find(table_file.table_id_);
            if (groupItr == groups.end()) {
                TableSchema table_schema;
                table_schema.table_id_ = table_file.table_id_;
                auto status = DescribeTable(table_schema);
                if (!status.ok()) {
                    return status;
                }
                groups[table_file.table_id_] = table_schema;
            }
            table_file.dimension_ = groups[table_file.table_id_].dimension_;
            table_file.index_file_size_ = groups[table_file.table_id_].index_file_size_;
            table_file.nlist_ = groups[table_file.table_id_].nlist_;
            table_file.metric_type_ = groups[table_file.table_id_].metric_type_;
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
SqliteMetaImpl::FilesByType(const std::string& table_id,
                            const std::vector<int>& file_types,
                            TableFilesSchema& table_files) {
    if (file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    try {
        table_files.clear();
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                     &TableFileSchema::file_id_,
                                                     &TableFileSchema::file_type_,
                                                     &TableFileSchema::file_size_,
                                                     &TableFileSchema::row_count_,
                                                     &TableFileSchema::date_,
                                                     &TableFileSchema::engine_type_,
                                                     &TableFileSchema::created_on_),
                                             where(in(&TableFileSchema::file_type_, file_types)
                                                   and c(&TableFileSchema::table_id_) == table_id));

        if (selected.size() >= 1) {
            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
            int to_index_count = 0, index_count = 0, backup_count = 0;
            for (auto& file : selected) {
                TableFileSchema file_schema;
                file_schema.table_id_ = table_id;
                file_schema.id_ = std::get<0>(file);
                file_schema.file_id_ = std::get<1>(file);
                file_schema.file_type_ = std::get<2>(file);
                file_schema.file_size_ = std::get<3>(file);
                file_schema.row_count_ = std::get<4>(file);
                file_schema.date_ = std::get<5>(file);
                file_schema.engine_type_ = std::get<6>(file);
                file_schema.created_on_ = std::get<7>(file);

                switch (file_schema.file_type_) {
                    case (int)TableFileSchema::RAW:raw_count++;
                        break;
                    case (int)TableFileSchema::NEW:new_count++;
                        break;
                    case (int)TableFileSchema::NEW_MERGE:new_merge_count++;
                        break;
                    case (int)TableFileSchema::NEW_INDEX:new_index_count++;
                        break;
                    case (int)TableFileSchema::TO_INDEX:to_index_count++;
                        break;
                    case (int)TableFileSchema::INDEX:index_count++;
                        break;
                    case (int)TableFileSchema::BACKUP:backup_count++;
                        break;
                    default:break;
                }

                table_files.emplace_back(file_schema);
            }

            std::string msg = "Get table files by type.";
            for (int file_type : file_types) {
                switch (file_type) {
                    case (int)TableFileSchema::RAW:
                        msg = msg + " raw files:" + std::to_string(raw_count);
                        break;
                    case (int)TableFileSchema::NEW:
                        msg = msg + " new files:" + std::to_string(new_count);
                        break;
                    case (int)TableFileSchema::NEW_MERGE:
                        msg = msg + " new_merge files:" + std::to_string(new_merge_count);
                        break;
                    case (int)TableFileSchema::NEW_INDEX:
                        msg = msg + " new_index files:" + std::to_string(new_index_count);
                        break;
                    case (int)TableFileSchema::TO_INDEX:
                        msg = msg + " to_index files:" + std::to_string(to_index_count);
                        break;
                    case (int)TableFileSchema::INDEX:
                        msg = msg + " index files:" + std::to_string(index_count);
                        break;
                    case (int)TableFileSchema::BACKUP:
                        msg = msg + " backup files:" + std::to_string(backup_count);
                        break;
                    default:break;
                }
            }
            ENGINE_LOG_DEBUG << msg;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when check non index files", e.what());
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
                // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                ConnectorPtr->update_all(
                    set(
                        c(&TableFileSchema::file_type_) = (int)TableFileSchema::TO_DELETE),
                    where(
                        c(&TableFileSchema::created_on_) < (int64_t)(now - usecs) and
                        c(&TableFileSchema::file_type_) != (int)TableFileSchema::TO_DELETE));
            } catch (std::exception& e) {
                return HandleException("Encounter exception when update table files", e.what());
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
        auto selected = ConnectorPtr->select(columns(sum(&TableFileSchema::file_size_)),
                                             where(c(&TableFileSchema::file_type_) != (int)TableFileSchema::TO_DELETE));
        for (auto& total_size : selected) {
            if (!std::get<0>(total_size)) {
                continue;
            }
            result += (uint64_t)(*std::get<0>(total_size));
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when calculte db size", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpShadowFiles() {
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::vector<int> file_types = {
            (int)TableFileSchema::NEW,
            (int)TableFileSchema::NEW_INDEX,
            (int)TableFileSchema::NEW_MERGE
        };
        auto files =
            ConnectorPtr->select(columns(&TableFileSchema::id_), where(in(&TableFileSchema::file_type_, file_types)));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& file : files) {
                ENGINE_LOG_DEBUG << "Remove table file type as NEW";
                ConnectorPtr->remove<TableFileSchema>(std::get<0>(file));
            }
            return true;
        });

        if (!commited) {
            return HandleException("CleanUp error: sqlite transaction failed");
        }

        if (files.size() > 0) {
            ENGINE_LOG_DEBUG << "Clean " << files.size() << " files";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean table file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpFilesWithTTL(uint64_t seconds, CleanUpFilter* filter) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> table_ids;

    // remove to_delete files
    try {
        server::MetricCollector metric;

        std::vector<int> file_types = {
            (int)TableFileSchema::TO_DELETE,
            (int)TableFileSchema::BACKUP,
        };

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        // collect files to be deleted
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                  &TableFileSchema::table_id_,
                                                  &TableFileSchema::file_id_,
                                                  &TableFileSchema::file_type_,
                                                  &TableFileSchema::date_),
                                          where(
                                              in(&TableFileSchema::file_type_, file_types)
                                              and
                                              c(&TableFileSchema::updated_time_)
                                              < now - seconds * US_PS));

        int64_t clean_files = 0;
        auto commited = ConnectorPtr->transaction([&]() mutable {
            TableFileSchema table_file;
            for (auto& file : files) {
                table_file.id_ = std::get<0>(file);
                table_file.table_id_ = std::get<1>(file);
                table_file.file_id_ = std::get<2>(file);
                table_file.file_type_ = std::get<3>(file);
                table_file.date_ = std::get<4>(file);

                // check if the file can be deleted
                if (filter && filter->IsIgnored(table_file)) {
                    ENGINE_LOG_DEBUG << "File:" << table_file.file_id_
                                     << " currently is in use, not able to delete now";
                    continue; // ignore this file, don't delete it
                }

                // erase from cache, must do this before file deleted,
                // because GetTableFilePath won't able to generate file path after the file is deleted
                utils::GetTableFilePath(options_, table_file);
                server::CommonUtil::EraseFromCache(table_file.location_);

                if (table_file.file_type_ == (int)TableFileSchema::TO_DELETE) {
                    // delete file from meta
                    ConnectorPtr->remove<TableFileSchema>(table_file.id_);

                    // delete file from disk storage
                    utils::DeleteTableFilePath(options_, table_file);

                    ENGINE_LOG_DEBUG << "Remove file id:" << table_file.file_id_ << " location:" << table_file.location_;
                    table_ids.insert(table_file.table_id_);

                    clean_files++;
                }
            }
            return true;
        });

        if (!commited) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }

        if (clean_files > 0) {
            ENGINE_LOG_DEBUG << "Clean " << clean_files << " files expired in " << seconds << " seconds";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean table files", e.what());
    }

    // remove to_delete tables
    try {
        server::MetricCollector metric;

        // multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::id_, &TableSchema::table_id_),
                                           where(c(&TableSchema::state_) == (int)TableSchema::TO_DELETE));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto& table : tables) {
                utils::DeleteTablePath(options_, std::get<1>(table), false);  // only delete empty folder
                ConnectorPtr->remove<TableSchema>(std::get<0>(table));
            }

            return true;
        });

        if (!commited) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }

        if (tables.size() > 0) {
            ENGINE_LOG_DEBUG << "Remove " << tables.size() << " tables from meta";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean table files", e.what());
    }

    // remove deleted table folder
    // don't remove table folder until all its files has been deleted
    try {
        server::MetricCollector metric;

        int64_t remove_tables = 0;
        for (auto& table_id : table_ids) {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::file_id_),
                                                 where(c(&TableFileSchema::table_id_) == table_id));
            if (selected.size() == 0) {
                utils::DeleteTablePath(options_, table_id);
                remove_tables++;
            }
        }

        if (remove_tables) {
            ENGINE_LOG_DEBUG << "Remove " << remove_tables << " tables folder";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete table folder", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Count(const std::string& table_id, uint64_t& result) {
    try {
        server::MetricCollector metric;

        std::vector<int> file_types = {(int)TableFileSchema::RAW, (int)TableFileSchema::TO_INDEX,
                                       (int)TableFileSchema::INDEX};
        auto selected = ConnectorPtr->select(
            columns(&TableFileSchema::row_count_),
            where(in(&TableFileSchema::file_type_, file_types) and c(&TableFileSchema::table_id_) == table_id));

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);

        if (!status.ok()) {
            return status;
        }

        result = 0;
        for (auto& file : selected) {
            result += std::get<0>(file);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when calculate table file size", e.what());
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
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto commited = ConnectorPtr->transaction([&]() mutable {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                         &TableFileSchema::file_size_),
                                                 where(c(&TableFileSchema::file_type_)
                                                       != (int)TableFileSchema::TO_DELETE),
                                                 order_by(&TableFileSchema::id_),
                                                 limit(10));

            std::vector<int> ids;
            TableFileSchema table_file;

            for (auto& file : selected) {
                if (to_discard_size <= 0) break;
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

            ConnectorPtr->update_all(
                set(
                    c(&TableFileSchema::file_type_) = (int)TableFileSchema::TO_DELETE,
                    c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                where(
                    in(&TableFileSchema::id_, ids)));

            return true;
        });

        if (!commited) {
            return HandleException("DiscardFiles error: sqlite transaction failed");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when discard table file", e.what());
    }

    return DiscardFiles(to_discard_size);
}

} // namespace meta
} // namespace engine
} // namespace milvus

