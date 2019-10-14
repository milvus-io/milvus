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
#include "db/IDGenerator.h"
#include "db/Utils.h"
#include "utils/Log.h"
#include "utils/Exception.h"
#include "MetaConsts.h"
#include "metrics/Metrics.h"

#include <unistd.h>
#include <sstream>
#include <iostream>
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <memory>
#include <map>
#include <set>
#include <sqlite_orm.h>


namespace milvus {
namespace engine {
namespace meta {

using namespace sqlite_orm;

namespace {

Status
HandleException(const std::string &desc, const char *what = nullptr) {
    if (what == nullptr) {
        ENGINE_LOG_ERROR << desc;
        return Status(DB_META_TRANSACTION_FAILED, desc);
    } else {
        std::string msg = desc + ":" + what;
        ENGINE_LOG_ERROR << msg;
        return Status(DB_META_TRANSACTION_FAILED, msg);
    }
}

} // namespace

inline auto
StoragePrototype(const std::string &path) {
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
                                   make_column("metric_type", &TableSchema::metric_type_)),
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

SqliteMetaImpl::SqliteMetaImpl(const DBMetaOptions &options)
    : options_(options) {
    Initialize();
}

SqliteMetaImpl::~SqliteMetaImpl() {
}

Status
SqliteMetaImpl::NextTableId(std::string &table_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    table_id = ss.str();
    return Status::OK();
}

Status
SqliteMetaImpl::NextFileId(std::string &file_id) {
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

    //old meta could be recreated since schema changed, throw exception if meta schema is not compatible
    auto ret = ConnectorPtr->sync_schema_simulate();
    if (ret.find(META_TABLES) != ret.end()
        && sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_TABLES]) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }
    if (ret.find(META_TABLEFILES) != ret.end()
        && sqlite_orm::sync_schema_result::dropped_and_recreated == ret[META_TABLEFILES]) {
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
            return Status(DB_INVALID_PATH, msg);
        }
    }

    ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(options_.path_ + "/meta.sqlite"));

    ValidateMetaSchema();

    ConnectorPtr->sync_schema();
    ConnectorPtr->open_forever(); // thread safe option
    ConnectorPtr->pragma.journal_mode(journal_mode::WAL); // WAL => write ahead log

    CleanUp();

    return Status::OK();
}

// TODO(myh): Delete single vecotor by id
Status
SqliteMetaImpl::DropPartitionsByDates(const std::string &table_id,
                                      const DatesT &dates) {
    if (dates.size() == 0) {
        return Status::OK();
    }

    TableSchema table_schema;
    table_schema.table_id_ = table_id;
    auto status = DescribeTable(table_schema);
    if (!status.ok()) {
        return status;
    }

    try {
        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    in(&TableFileSchema::date_, dates)));

        ENGINE_LOG_DEBUG << "Successfully drop partitions, table id = " << table_schema.table_id_;
    } catch (std::exception &e) {
        return HandleException("Encounter exception when drop partition", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateTable(TableSchema &table_schema) {
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
        } catch (std::exception &e) {
            return HandleException("Encounter exception when create table", e.what());
        }

        ENGINE_LOG_DEBUG << "Successfully create table: " << table_schema.table_id_;

        return utils::CreateTablePath(options_, table_schema.table_id_);
    } catch (std::exception &e) {
        return HandleException("Encounter exception when create table", e.what());
    }
}

Status
SqliteMetaImpl::DeleteTable(const std::string &table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete table
        ConnectorPtr->update_all(
            set(
                c(&TableSchema::state_) = (int) TableSchema::TO_DELETE),
            where(
                c(&TableSchema::table_id_) == table_id and
                    c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));

        ENGINE_LOG_DEBUG << "Successfully delete table, table id = " << table_id;
    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DeleteTableFiles(const std::string &table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete table files
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE));

        ENGINE_LOG_DEBUG << "Successfully delete table files, table id = " << table_id;
    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DescribeTable(TableSchema &table_schema) {
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
                                                   &TableSchema::metric_type_),
                                           where(c(&TableSchema::table_id_) == table_schema.table_id_
                                                     and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));

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
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_schema.table_id_ + " not found");
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when describe table", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::FilesByType(const std::string &table_id,
                            const std::vector<int> &file_types,
                            std::vector<std::string> &file_ids) {
    if (file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    try {
        file_ids.clear();
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::file_id_,
                                                     &TableFileSchema::file_type_),
                                             where(in(&TableFileSchema::file_type_, file_types)
                                                       and c(&TableFileSchema::table_id_) == table_id));

        if (selected.size() >= 1) {
            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
            int to_index_count = 0, index_count = 0, backup_count = 0;
            for (auto &file : selected) {
                file_ids.push_back(std::get<0>(file));
                switch (std::get<1>(file)) {
                    case (int) TableFileSchema::RAW:raw_count++;
                        break;
                    case (int) TableFileSchema::NEW:new_count++;
                        break;
                    case (int) TableFileSchema::NEW_MERGE:new_merge_count++;
                        break;
                    case (int) TableFileSchema::NEW_INDEX:new_index_count++;
                        break;
                    case (int) TableFileSchema::TO_INDEX:to_index_count++;
                        break;
                    case (int) TableFileSchema::INDEX:index_count++;
                        break;
                    case (int) TableFileSchema::BACKUP:backup_count++;
                        break;
                    default:break;
                }
            }

            ENGINE_LOG_DEBUG << "Table " << table_id << " currently has raw files:" << raw_count
                             << " new files:" << new_count << " new_merge files:" << new_merge_count
                             << " new_index files:" << new_index_count << " to_index files:" << to_index_count
                             << " index files:" << index_count << " backup files:" << backup_count;
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when check non index files", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableIndex(const std::string &table_id, const TableIndex &index) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::state_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::created_on_,
                                                   &TableSchema::flag_,
                                                   &TableSchema::index_file_size_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                     and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));

        if (tables.size() > 0) {
            meta::TableSchema table_schema;
            table_schema.id_ = std::get<0>(tables[0]);
            table_schema.table_id_ = table_id;
            table_schema.state_ = std::get<1>(tables[0]);
            table_schema.dimension_ = std::get<2>(tables[0]);
            table_schema.created_on_ = std::get<3>(tables[0]);
            table_schema.flag_ = std::get<4>(tables[0]);
            table_schema.index_file_size_ = std::get<5>(tables[0]);
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
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::RAW,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    c(&TableFileSchema::file_type_) == (int) TableFileSchema::BACKUP));

        ENGINE_LOG_DEBUG << "Successfully update table index, table id = " << table_id;
    } catch (std::exception &e) {
        std::string msg = "Encounter exception when update table index: table_id = " + table_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFlag(const std::string &table_id, int64_t flag) {
    try {
        server::MetricCollector metric;

        //set all backup file to raw
        ConnectorPtr->update_all(
            set(
                c(&TableSchema::flag_) = flag),
            where(
                c(&TableSchema::table_id_) == table_id));
        ENGINE_LOG_DEBUG << "Successfully update table flag, table id = " << table_id;
    } catch (std::exception &e) {
        std::string msg = "Encounter exception when update table flag: table_id = " + table_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DescribeTableIndex(const std::string &table_id, TableIndex &index) {
    try {
        server::MetricCollector metric;

        auto groups = ConnectorPtr->select(columns(&TableSchema::engine_type_,
                                                   &TableSchema::nlist_,
                                                   &TableSchema::metric_type_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                     and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));

        if (groups.size() == 1) {
            index.engine_type_ = std::get<0>(groups[0]);
            index.nlist_ = std::get<1>(groups[0]);
            index.metric_type_ = std::get<2>(groups[0]);
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_id + " not found");
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when describe index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropTableIndex(const std::string &table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete index files
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    c(&TableFileSchema::file_type_) == (int) TableFileSchema::INDEX));

        //set all backup file to raw
        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::RAW,
                c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    c(&TableFileSchema::file_type_) == (int) TableFileSchema::BACKUP));

        //set table index type to raw
        ConnectorPtr->update_all(
            set(
                c(&TableSchema::engine_type_) = DEFAULT_ENGINE_TYPE,
                c(&TableSchema::nlist_) = DEFAULT_NLIST,
                c(&TableSchema::metric_type_) = DEFAULT_METRIC_TYPE),
            where(
                c(&TableSchema::table_id_) == table_id));

        ENGINE_LOG_DEBUG << "Successfully drop table index, table id = " << table_id;
    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {
    has_or_not = false;

    try {
        server::MetricCollector metric;
        auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                     and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));
        if (tables.size() == 1) {
            has_or_not = true;
        } else {
            has_or_not = false;
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup table", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::AllTables(std::vector<TableSchema> &table_schema_array) {
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
                                                     &TableSchema::metric_type_),
                                             where(c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));
        for (auto &table : selected) {
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

            table_schema_array.emplace_back(schema);
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup all tables", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateTableFile(TableFileSchema &file_schema) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when create table file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::FilesToIndex(TableFilesSchema &files) {
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
                                                       == (int) TableFileSchema::TO_INDEX));

        std::map<std::string, TableSchema> groups;
        TableFileSchema table_file;

        Status ret;
        for (auto &file : selected) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate raw files", e.what());
    }
}

Status
SqliteMetaImpl::FilesToSearch(const std::string &table_id,
                              const std::vector<size_t> &ids,
                              const DatesT &partition,
                              DatePartionedTableFilesSchema &files) {
    files.clear();
    server::MetricCollector metric;

    try {
        auto select_columns = columns(&TableFileSchema::id_,
                                      &TableFileSchema::table_id_,
                                      &TableFileSchema::file_id_,
                                      &TableFileSchema::file_type_,
                                      &TableFileSchema::file_size_,
                                      &TableFileSchema::row_count_,
                                      &TableFileSchema::date_,
                                      &TableFileSchema::engine_type_);

        auto match_tableid = c(&TableFileSchema::table_id_) == table_id;

        std::vector<int> file_types = {
            (int) TableFileSchema::RAW,
            (int) TableFileSchema::TO_INDEX,
            (int) TableFileSchema::INDEX
        };
        auto match_type = in(&TableFileSchema::file_type_, file_types);

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) { return status; }

        decltype(ConnectorPtr->select(select_columns)) selected;
        if (partition.empty() && ids.empty()) {
            auto filter = where(match_tableid and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        } else if (partition.empty() && !ids.empty()) {
            auto match_fileid = in(&TableFileSchema::id_, ids);
            auto filter = where(match_tableid and match_fileid and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        } else if (!partition.empty() && ids.empty()) {
            auto match_date = in(&TableFileSchema::date_, partition);
            auto filter = where(match_tableid and match_date and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        } else if (!partition.empty() && !ids.empty()) {
            auto match_fileid = in(&TableFileSchema::id_, ids);
            auto match_date = in(&TableFileSchema::date_, partition);
            auto filter = where(match_tableid and match_fileid and match_date and match_type);
            selected = ConnectorPtr->select(select_columns, filter);
        }

        Status ret;
        TableFileSchema table_file;
        for (auto &file : selected) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate index files", e.what());
    }
}

Status
SqliteMetaImpl::FilesToMerge(const std::string &table_id,
                             DatePartionedTableFilesSchema &files) {
    files.clear();

    try {
        server::MetricCollector metric;

        //check table existence
        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        //get files to merge
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                     &TableFileSchema::table_id_,
                                                     &TableFileSchema::file_id_,
                                                     &TableFileSchema::file_type_,
                                                     &TableFileSchema::file_size_,
                                                     &TableFileSchema::row_count_,
                                                     &TableFileSchema::date_,
                                                     &TableFileSchema::created_on_),
                                             where(c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW and
                                                 c(&TableFileSchema::table_id_) == table_id),
                                             order_by(&TableFileSchema::file_size_).desc());

        Status result;
        for (auto &file : selected) {
            TableFileSchema table_file;
            table_file.file_size_ = std::get<4>(file);
            if (table_file.file_size_ >= table_schema.index_file_size_) {
                continue;//skip large file
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
        }

        if (selected.size() > 0) {
            ENGINE_LOG_DEBUG << "Collect " << selected.size() << " to-merge files";
        }
        return result;
    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate merge files", e.what());
    }
}

Status
SqliteMetaImpl::GetTableFiles(const std::string &table_id,
                              const std::vector<size_t> &ids,
                              TableFilesSchema &table_files) {
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
                                              c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE));

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        Status result;
        for (auto &file : files) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup table files", e.what());
    }
}

// TODO(myh): Support swap to cloud storage
Status
SqliteMetaImpl::Archive() {
    auto &criterias = options_.archive_conf_.GetCriterias();
    if (criterias.size() == 0) {
        return Status::OK();
    }

    for (auto kv : criterias) {
        auto &criteria = kv.first;
        auto &limit = kv.second;
        if (criteria == engine::ARCHIVE_CONF_DAYS) {
            int64_t usecs = limit * D_SEC * US_PS;
            int64_t now = utils::GetMicroSecTimeStamp();
            try {
                //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                ConnectorPtr->update_all(
                    set(
                        c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE),
                    where(
                        c(&TableFileSchema::created_on_) < (int64_t) (now - usecs) and
                            c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE));
            } catch (std::exception &e) {
                return HandleException("Encounter exception when update table files", e.what());
            }

            ENGINE_LOG_DEBUG << "Archive old files";
        }
        if (criteria == engine::ARCHIVE_CONF_DISK) {
            uint64_t sum = 0;
            Size(sum);

            int64_t to_delete = (int64_t) sum - limit * G;
            DiscardFiles(to_delete);

            ENGINE_LOG_DEBUG << "Archive files to free disk";
        }
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Size(uint64_t &result) {
    result = 0;
    try {
        auto selected = ConnectorPtr->select(columns(sum(&TableFileSchema::file_size_)),
                                             where(
                                                 c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE));
        for (auto &total_size : selected) {
            if (!std::get<0>(total_size)) {
                continue;
            }
            result += (uint64_t) (*std::get<0>(total_size));
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when calculte db size", e.what());
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
                                                           != (int) TableFileSchema::TO_DELETE),
                                                 order_by(&TableFileSchema::id_),
                                                 limit(10));

            std::vector<int> ids;
            TableFileSchema table_file;

            for (auto &file : selected) {
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
                    c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE,
                    c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()),
                where(
                    in(&TableFileSchema::id_, ids)));

            return true;
        });

        if (!commited) {
            return HandleException("DiscardFiles error: sqlite transaction failed");
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when discard table file", e.what());
    }

    return DiscardFiles(to_discard_size);
}

Status
SqliteMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::state_),
                                           where(c(&TableSchema::table_id_) == file_schema.table_id_));

        //if the table has been deleted, just mark the table file as TO_DELETE
        //clean thread will delete the file later
        if (tables.size() < 1 || std::get<0>(tables[0]) == (int) TableSchema::TO_DELETE) {
            file_schema.file_type_ = TableFileSchema::TO_DELETE;
        }

        ConnectorPtr->update(file_schema);

        ENGINE_LOG_DEBUG << "Update single table file, file id = " << file_schema.file_id_;
    } catch (std::exception &e) {
        std::string msg = "Exception update table file: table_id = " + file_schema.table_id_
            + " file_id = " + file_schema.file_id_;
        return HandleException(msg, e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFilesToIndex(const std::string &table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_INDEX),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW));

        ENGINE_LOG_DEBUG << "Update files to to_index, table id = " << table_id;
    } catch (std::exception &e) {
        return HandleException("Encounter exception when update table files to to_index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateTableFiles(TableFilesSchema &files) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::map<std::string, bool> has_tables;
        for (auto &file : files) {
            if (has_tables.find(file.table_id_) != has_tables.end()) {
                continue;
            }
            auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
                                               where(c(&TableSchema::table_id_) == file.table_id_
                                                         and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));
            if (tables.size() >= 1) {
                has_tables[file.table_id_] = true;
            } else {
                has_tables[file.table_id_] = false;
            }
        }

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &file : files) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when update table files", e.what());
    }
    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> table_ids;

    //remove to_delete files
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto files = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                  &TableFileSchema::table_id_,
                                                  &TableFileSchema::file_id_,
                                                  &TableFileSchema::date_),
                                          where(
                                              c(&TableFileSchema::file_type_) ==
                                                  (int) TableFileSchema::TO_DELETE
                                                  and
                                                      c(&TableFileSchema::updated_time_)
                                                          < now - seconds * US_PS));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            TableFileSchema table_file;
            for (auto &file : files) {
                table_file.id_ = std::get<0>(file);
                table_file.table_id_ = std::get<1>(file);
                table_file.file_id_ = std::get<2>(file);
                table_file.date_ = std::get<3>(file);

                utils::DeleteTableFilePath(options_, table_file);
                ENGINE_LOG_DEBUG << "Removing file id:" << table_file.file_id_ << " location:" << table_file.location_;
                ConnectorPtr->remove<TableFileSchema>(table_file.id_);

                table_ids.insert(table_file.table_id_);
            }
            return true;
        });

        if (!commited) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }

        if (files.size() > 0) {
            ENGINE_LOG_DEBUG << "Clean " << files.size() << " files deleted in " << seconds << " seconds";
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table files", e.what());
    }

    //remove to_delete tables
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::table_id_),
                                           where(c(&TableSchema::state_) == (int) TableSchema::TO_DELETE));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &table : tables) {
                utils::DeleteTablePath(options_, std::get<1>(table), false);//only delete empty folder
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table files", e.what());
    }

    //remove deleted table folder
    //don't remove table folder until all its files has been deleted
    try {
        server::MetricCollector metric;

        for (auto &table_id : table_ids) {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::file_id_),
                                                 where(c(&TableFileSchema::table_id_) == table_id));
            if (selected.size() == 0) {
                utils::DeleteTablePath(options_, table_id);
            }
        }

        if (table_ids.size() > 0) {
            ENGINE_LOG_DEBUG << "Remove " << table_ids.size() << " tables folder";
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table folder", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUp() {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::vector<int> file_types = {
            (int) TableFileSchema::NEW,
            (int) TableFileSchema::NEW_INDEX,
            (int) TableFileSchema::NEW_MERGE
        };
        auto files =
            ConnectorPtr->select(columns(&TableFileSchema::id_), where(in(&TableFileSchema::file_type_, file_types)));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &file : files) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Count(const std::string &table_id, uint64_t &result) {
    try {
        server::MetricCollector metric;

        std::vector<int> file_types = {
            (int) TableFileSchema::RAW,
            (int) TableFileSchema::TO_INDEX,
            (int) TableFileSchema::INDEX
        };
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::row_count_),
                                             where(in(&TableFileSchema::file_type_, file_types)
                                                       and c(&TableFileSchema::table_id_) == table_id));

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);

        if (!status.ok()) {
            return status;
        }

        result = 0;
        for (auto &file : selected) {
            result += std::get<0>(file);
        }
    } catch (std::exception &e) {
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
    } catch (std::exception &e) {
        return HandleException("Encounter exception when drop all meta", e.what());
    }

    return Status::OK();
}

} // namespace meta
} // namespace engine
} // namespace milvus

