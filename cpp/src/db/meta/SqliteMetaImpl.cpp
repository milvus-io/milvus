/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "SqliteMetaImpl.h"
#include "db/IDGenerator.h"
#include "db/Utils.h"
#include "db/Log.h"
#include "MetaConsts.h"
#include "db/Factories.h"
#include "metrics/Metrics.h"

#include <unistd.h>
#include <sstream>
#include <iostream>
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <sqlite_orm.h>


namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

using namespace sqlite_orm;

namespace {

Status HandleException(const std::string& desc, std::exception &e) {
    ENGINE_LOG_ERROR << desc << ": " << e.what();
    return Status::DBTransactionError(desc, e.what());
}

}

inline auto StoragePrototype(const std::string &path) {
    return make_storage(path,
                        make_table("Tables",
                                   make_column("id", &TableSchema::id_, primary_key()),
                                   make_column("table_id", &TableSchema::table_id_, unique()),
                                   make_column("state", &TableSchema::state_),
                                   make_column("dimension", &TableSchema::dimension_),
                                   make_column("created_on", &TableSchema::created_on_),
                                   make_column("engine_type", &TableSchema::engine_type_),
                                   make_column("nlist", &TableSchema::nlist_),
                                   make_column("index_file_size", &TableSchema::index_file_size_),
                                   make_column("metric_type", &TableSchema::metric_type_)),
                        make_table("TableFiles",
                                   make_column("id", &TableFileSchema::id_, primary_key()),
                                   make_column("table_id", &TableFileSchema::table_id_),
                                   make_column("engine_type", &TableFileSchema::engine_type_),
                                   make_column("file_id", &TableFileSchema::file_id_),
                                   make_column("file_type", &TableFileSchema::file_type_),
                                   make_column("file_size", &TableFileSchema::file_size_, default_value(0)),
                                   make_column("row_count", &TableFileSchema::row_count_, default_value(0)),
                                   make_column("updated_time", &TableFileSchema::updated_time_),
                                   make_column("created_on", &TableFileSchema::created_on_),
                                   make_column("date", &TableFileSchema::date_))
    );

}

using ConnectorT = decltype(StoragePrototype(""));
static std::unique_ptr<ConnectorT> ConnectorPtr;
using ConditionT = decltype(c(&TableFileSchema::id_) == 1UL);

Status SqliteMetaImpl::NextTableId(std::string &table_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    table_id = ss.str();
    return Status::OK();
}

Status SqliteMetaImpl::NextFileId(std::string &file_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    file_id = ss.str();
    return Status::OK();
}

SqliteMetaImpl::SqliteMetaImpl(const DBMetaOptions &options_)
    : options_(options_) {
    Initialize();
}

Status SqliteMetaImpl::Initialize() {
    if (!boost::filesystem::is_directory(options_.path)) {
        auto ret = boost::filesystem::create_directory(options_.path);
        if (!ret) {
            ENGINE_LOG_ERROR << "Failed to create db directory " << options_.path;
            return Status::InvalidDBPath("Failed to create db directory", options_.path);
        }
    }

    ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(options_.path + "/meta.sqlite"));

    ConnectorPtr->sync_schema();
    ConnectorPtr->open_forever(); // thread safe option
    ConnectorPtr->pragma.journal_mode(journal_mode::WAL); // WAL => write ahead log

    CleanUp();

    return Status::OK();
}

// PXU TODO: Temp solution. Will fix later
Status SqliteMetaImpl::DropPartitionsByDates(const std::string &table_id,
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
        auto yesterday = GetDateWithDelta(-1);

        for (auto &date : dates) {
            if (date >= yesterday) {
                return Status::Error("Could not delete partitions with 2 days");
            }
        }

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE
            ),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                    in(&TableFileSchema::date_, dates)
            ));
    } catch (std::exception &e) {
        return HandleException("Encounter exception when drop partition", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::CreateTable(TableSchema &table_schema) {

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
                if(TableSchema::TO_DELETE == std::get<0>(table[0])) {
                    return Status::Error("Table already exists and it is in delete state, please wait a second");
                } else {
                    // Change from no error to already exist.
                    return Status::AlreadyExist("Table already exists");
                }
            }
        }

        table_schema.id_ = -1;
        table_schema.created_on_ = utils::GetMicroSecTimeStamp();

        try {
            auto id = ConnectorPtr->insert(table_schema);
            table_schema.id_ = id;
        } catch (...) {
            ENGINE_LOG_ERROR << "sqlite transaction failed";
            return Status::DBTransactionError("Add Table Error");
        }

        return utils::CreateTablePath(options_, table_schema.table_id_);

    } catch (std::exception &e) {
        return HandleException("Encounter exception when create table", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::DeleteTable(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete table
        ConnectorPtr->update_all(
                set(
                        c(&TableSchema::state_) = (int) TableSchema::TO_DELETE
                ),
                where(
                        c(&TableSchema::table_id_) == table_id and
                        c(&TableSchema::state_) != (int) TableSchema::TO_DELETE
                ));

    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::DeleteTableFiles(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete table files
        ConnectorPtr->update_all(
                set(
                        c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE,
                        c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()
                ),
                where(
                        c(&TableFileSchema::table_id_) == table_id and
                        c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE
                ));

    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table files", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::DescribeTable(TableSchema &table_schema) {
    try {
        server::MetricCollector metric;

        auto groups = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::state_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::created_on_,
                                                   &TableSchema::engine_type_,
                                                   &TableSchema::nlist_,
                                                   &TableSchema::index_file_size_,
                                                   &TableSchema::metric_type_),
                                           where(c(&TableSchema::table_id_) == table_schema.table_id_
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        if (groups.size() == 1) {
            table_schema.id_ = std::get<0>(groups[0]);
            table_schema.state_ = std::get<1>(groups[0]);
            table_schema.dimension_ = std::get<2>(groups[0]);
            table_schema.created_on_ = std::get<3>(groups[0]);
            table_schema.engine_type_ = std::get<4>(groups[0]);
            table_schema.nlist_ = std::get<5>(groups[0]);
            table_schema.index_file_size_ = std::get<6>(groups[0]);
            table_schema.metric_type_ = std::get<7>(groups[0]);
        } else {
            return Status::NotFound("Table " + table_schema.table_id_ + " not found");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when describe table", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::HasNonIndexFiles(const std::string& table_id, bool& has) {
    has = false;
    try {
        std::vector<int> file_types = {
                (int) TableFileSchema::RAW,
                (int) TableFileSchema::NEW,
                (int) TableFileSchema::NEW_MERGE,
                (int) TableFileSchema::NEW_INDEX,
                (int) TableFileSchema::TO_INDEX,
        };
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                     &TableFileSchema::file_type_),
                                             where(in(&TableFileSchema::file_type_, file_types)
                                                   and c(&TableFileSchema::table_id_) == table_id
                                             ));

        if (selected.size() >= 1) {
            has = true;

            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0, to_index_count = 0;
            std::vector<std::string> file_ids;
            for (auto &file : selected) {
                switch (std::get<1>(file)) {
                    case (int) TableFileSchema::RAW:
                        raw_count++;
                        break;
                    case (int) TableFileSchema::NEW:
                        new_count++;
                        break;
                    case (int) TableFileSchema::NEW_MERGE:
                        new_merge_count++;
                        break;
                    case (int) TableFileSchema::NEW_INDEX:
                        new_index_count++;
                        break;
                    case (int) TableFileSchema::TO_INDEX:
                        to_index_count++;
                        break;
                    default:
                        break;
                }
            }

            ENGINE_LOG_DEBUG << "Table " << table_id << " currently has raw files:" << raw_count
                << " new files:" << new_count << " new_merge files:" << new_merge_count
                << " new_index files:" << new_index_count << " to_index files:" << to_index_count;
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when check non index files", e);
    }
    return Status::OK();
}

Status SqliteMetaImpl::UpdateTableIndexParam(const std::string &table_id, const TableIndex& index) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::state_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::created_on_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                 and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));

        if(tables.size() > 0) {
            meta::TableSchema table_schema;
            table_schema.id_ = std::get<0>(tables[0]);
            table_schema.table_id_ = table_id;
            table_schema.state_ = std::get<1>(tables[0]);
            table_schema.dimension_ = std::get<2>(tables[0]);
            table_schema.created_on_ = std::get<3>(tables[0]);
            table_schema.engine_type_ = index.engine_type_;
            table_schema.nlist_ = index.nlist_;
            table_schema.index_file_size_ = index.index_file_size_*ONE_MB;
            table_schema.metric_type_ = index.metric_type_;

            ConnectorPtr->update(table_schema);
        } else {
            return Status::NotFound("Table " + table_id + " not found");
        }

        //set all backup file to raw
        ConnectorPtr->update_all(
                set(
                        c(&TableFileSchema::file_type_) = (int) TableFileSchema::RAW,
                        c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()
                ),
                where(
                        c(&TableFileSchema::table_id_) == table_id and
                        c(&TableFileSchema::file_type_) == (int) TableFileSchema::BACKUP
                ));

    } catch (std::exception &e) {
        std::string msg = "Encounter exception when update table index: table_id = " + table_id;
        return HandleException(msg, e);
    }
    return Status::OK();
}

Status SqliteMetaImpl::DescribeTableIndex(const std::string &table_id, TableIndex& index) {
    try {
        server::MetricCollector metric;

        auto groups = ConnectorPtr->select(columns(&TableSchema::engine_type_,
                                                   &TableSchema::nlist_,
                                                   &TableSchema::index_file_size_,
                                                   &TableSchema::metric_type_),
                                           where(c(&TableSchema::table_id_) == table_id
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        if (groups.size() == 1) {
            index.engine_type_ = std::get<0>(groups[0]);
            index.nlist_ = std::get<1>(groups[0]);
            index.index_file_size_ = std::get<2>(groups[0])/ONE_MB;
            index.metric_type_ = std::get<3>(groups[0]);
        } else {
            return Status::NotFound("Table " + table_id + " not found");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when describe index", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::DropTableIndex(const std::string &table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        //soft delete index files
        ConnectorPtr->update_all(
                set(
                        c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE,
                        c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()
                ),
                where(
                        c(&TableFileSchema::table_id_) == table_id and
                        c(&TableFileSchema::file_type_) == (int) TableFileSchema::INDEX
                ));

        //set all backup file to raw
        ConnectorPtr->update_all(
                set(
                        c(&TableFileSchema::file_type_) = (int) TableFileSchema::RAW,
                        c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()
                ),
                where(
                        c(&TableFileSchema::table_id_) == table_id and
                        c(&TableFileSchema::file_type_) == (int) TableFileSchema::BACKUP
                ));

    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table index files", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {
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

    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup table", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::AllTables(std::vector<TableSchema>& table_schema_array) {
    try {
        server::MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableSchema::id_,
                                                     &TableSchema::table_id_,
                                                     &TableSchema::dimension_,
                                                     &TableSchema::created_on_,
                                                     &TableSchema::engine_type_,
                                                     &TableSchema::nlist_,
                                                     &TableSchema::index_file_size_,
                                                     &TableSchema::metric_type_),
                                             where(c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
        for (auto &table : selected) {
            TableSchema schema;
            schema.id_ = std::get<0>(table);
            schema.table_id_ = std::get<1>(table);
            schema.created_on_ = std::get<2>(table);
            schema.dimension_ = std::get<3>(table);
            schema.engine_type_ = std::get<4>(table);
            schema.nlist_ = std::get<5>(table);
            schema.index_file_size_ = std::get<6>(table);
            schema.metric_type_ = std::get<7>(table);

            table_schema_array.emplace_back(schema);
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup all tables", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::CreateTableFile(TableFileSchema &file_schema) {
    if (file_schema.date_ == EmptyDate) {
        file_schema.date_ = Meta::GetDate();
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
        file_schema.engine_type_ = table_schema.engine_type_;
        file_schema.nlist_ = table_schema.nlist_;
        file_schema.metric_type_ = table_schema.metric_type_;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto id = ConnectorPtr->insert(file_schema);
        file_schema.id_ = id;

        return utils::CreateTableFilePath(options_, file_schema);

    } catch (std::exception& ex) {
        return HandleException("Encounter exception when create table file", ex);
    }

    return Status::OK();
}

Status SqliteMetaImpl::FilesToIndex(TableFilesSchema &files) {
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

            utils::GetTableFilePath(options_, table_file);
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
            table_file.metric_type_ = groups[table_file.table_id_].metric_type_;
            table_file.nlist_ = groups[table_file.table_id_].nlist_;
            table_file.dimension_ = groups[table_file.table_id_].dimension_;
            files.push_back(table_file);
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate raw files", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::FilesToSearch(const std::string &table_id,
                                 const DatesT &partition,
                                 DatePartionedTableFilesSchema &files) {
    files.clear();

    try {
        server::MetricCollector metric;

        if (partition.empty()) {
            std::vector<int> file_type = {(int) TableFileSchema::RAW, (int) TableFileSchema::TO_INDEX, (int) TableFileSchema::INDEX};
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                         &TableFileSchema::table_id_,
                                                         &TableFileSchema::file_id_,
                                                         &TableFileSchema::file_type_,
                                                         &TableFileSchema::file_size_,
                                                         &TableFileSchema::row_count_,
                                                         &TableFileSchema::date_,
                                                         &TableFileSchema::engine_type_),
                                                 where(c(&TableFileSchema::table_id_) == table_id and
                                                       in(&TableFileSchema::file_type_, file_type)));

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);
            if (!status.ok()) {
                return status;
            }

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
                table_file.metric_type_ = table_schema.metric_type_;
                table_file.nlist_ = table_schema.nlist_;
                table_file.dimension_ = table_schema.dimension_;
                utils::GetTableFilePath(options_, table_file);
                auto dateItr = files.find(table_file.date_);
                if (dateItr == files.end()) {
                    files[table_file.date_] = TableFilesSchema();
                }
                files[table_file.date_].push_back(table_file);
            }
        }
        else {
            std::vector<int> file_type = {(int) TableFileSchema::RAW, (int) TableFileSchema::TO_INDEX, (int) TableFileSchema::INDEX};
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                         &TableFileSchema::table_id_,
                                                         &TableFileSchema::file_id_,
                                                         &TableFileSchema::file_type_,
                                                         &TableFileSchema::file_size_,
                                                         &TableFileSchema::row_count_,
                                                         &TableFileSchema::date_,
                                                         &TableFileSchema::engine_type_),
                                                 where(c(&TableFileSchema::table_id_) == table_id and
                                                       in(&TableFileSchema::date_, partition) and
                                                       in(&TableFileSchema::file_type_, file_type)));

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);
            if (!status.ok()) {
                return status;
            }

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
                table_file.metric_type_ = table_schema.metric_type_;
                table_file.nlist_ = table_schema.nlist_;
                table_file.dimension_ = table_schema.dimension_;
                utils::GetTableFilePath(options_, table_file);
                auto dateItr = files.find(table_file.date_);
                if (dateItr == files.end()) {
                    files[table_file.date_] = TableFilesSchema();
                }
                files[table_file.date_].push_back(table_file);
            }

        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate index files", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::FilesToSearch(const std::string &table_id,
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

        std::vector<int> file_type = {(int) TableFileSchema::RAW, (int) TableFileSchema::TO_INDEX, (int) TableFileSchema::INDEX};
        auto match_type = in(&TableFileSchema::file_type_, file_type);

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) { return status; }

        decltype(ConnectorPtr->select(select_columns)) result;
        if (partition.empty() && ids.empty()) {
            auto filter = where(match_tableid and match_type);
            result = ConnectorPtr->select(select_columns, filter);
        }
        else if (partition.empty() && !ids.empty()) {
            auto match_fileid = in(&TableFileSchema::id_, ids);
            auto filter = where(match_tableid and match_fileid and match_type);
            result = ConnectorPtr->select(select_columns, filter);
        }
        else if (!partition.empty() && ids.empty()) {
            auto match_date = in(&TableFileSchema::date_, partition);
            auto filter = where(match_tableid and match_date and match_type);
            result = ConnectorPtr->select(select_columns, filter);
        }
        else if (!partition.empty() && !ids.empty()) {
            auto match_fileid = in(&TableFileSchema::id_, ids);
            auto match_date = in(&TableFileSchema::date_, partition);
            auto filter = where(match_tableid and match_fileid and match_date and match_type);
            result = ConnectorPtr->select(select_columns, filter);
        }

        TableFileSchema table_file;
        for (auto &file : result) {
            table_file.id_ = std::get<0>(file);
            table_file.table_id_ = std::get<1>(file);
            table_file.file_id_ = std::get<2>(file);
            table_file.file_type_ = std::get<3>(file);
            table_file.file_size_ = std::get<4>(file);
            table_file.row_count_ = std::get<5>(file);
            table_file.date_ = std::get<6>(file);
            table_file.engine_type_ = std::get<7>(file);
            table_file.dimension_ = table_schema.dimension_;
            table_file.metric_type_ = table_schema.metric_type_;
            table_file.nlist_ = table_schema.nlist_;
            utils::GetTableFilePath(options_, table_file);
            auto dateItr = files.find(table_file.date_);
            if (dateItr == files.end()) {
                files[table_file.date_] = TableFilesSchema();
            }
            files[table_file.date_].push_back(table_file);
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate index files", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::FilesToMerge(const std::string &table_id,
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

        for (auto &file : selected) {
            TableFileSchema table_file;
            table_file.file_size_ = std::get<4>(file);
            if(table_file.file_size_ >= table_schema.index_file_size_) {
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
            table_file.metric_type_ = table_schema.metric_type_;
            table_file.nlist_ = table_schema.nlist_;
            utils::GetTableFilePath(options_, table_file);
            auto dateItr = files.find(table_file.date_);
            if (dateItr == files.end()) {
                files[table_file.date_] = TableFilesSchema();
            }
            files[table_file.date_].push_back(table_file);
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate merge files", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::GetTableFiles(const std::string& table_id,
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
                                                  in(&TableFileSchema::id_, ids)
                                          ));

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

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
            file_schema.metric_type_ = table_schema.metric_type_;
            file_schema.nlist_ = table_schema.nlist_;
            file_schema.created_on_ = std::get<7>(file);
            file_schema.dimension_ = table_schema.dimension_;

            utils::GetTableFilePath(options_, file_schema);

            table_files.emplace_back(file_schema);
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup table files", e);
    }

    return Status::OK();
}

// PXU TODO: Support Swap
Status SqliteMetaImpl::Archive() {
    auto &criterias = options_.archive_conf.GetCriterias();
    if (criterias.size() == 0) {
        return Status::OK();
    }

    for (auto kv : criterias) {
        auto &criteria = kv.first;
        auto &limit = kv.second;
        if (criteria == engine::ARCHIVE_CONF_DAYS) {
            long usecs = limit * D_SEC * US_PS;
            long now = utils::GetMicroSecTimeStamp();
            try {
                //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                ConnectorPtr->update_all(
                    set(
                        c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE
                    ),
                    where(
                        c(&TableFileSchema::created_on_) < (long) (now - usecs) and
                            c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE
                    ));
            } catch (std::exception &e) {
                return HandleException("Encounter exception when update table files", e);
            }
        }
        if (criteria == engine::ARCHIVE_CONF_DISK) {
            uint64_t sum = 0;
            Size(sum);

            int64_t to_delete = (int64_t)sum - limit * G;
            DiscardFiles(to_delete);
        }
    }

    return Status::OK();
}

Status SqliteMetaImpl::Size(uint64_t &result) {
    result = 0;
    try {
        auto selected = ConnectorPtr->select(columns(sum(&TableFileSchema::file_size_)),
                                          where(
                                                  c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE
                                          ));
        for (auto &total_size : selected) {
            if (!std::get<0>(total_size)) {
                continue;
            }
            result += (uint64_t) (*std::get<0>(total_size));
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when calculte db size", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::DiscardFiles(long to_discard_size) {
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
                            c(&TableFileSchema::updated_time_) = utils::GetMicroSecTimeStamp()
                    ),
                    where(
                            in(&TableFileSchema::id_, ids)
                    ));

            return true;
        });

        if (!commited) {
            ENGINE_LOG_ERROR << "sqlite transaction failed";
            return Status::DBTransactionError("Update table file error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when discard table file", e);
    }

    return DiscardFiles(to_discard_size);
}

Status SqliteMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        auto tables = ConnectorPtr->select(columns(&TableSchema::state_),
                                           where(c(&TableSchema::table_id_) == file_schema.table_id_));

        //if the table has been deleted, just mark the table file as TO_DELETE
        //clean thread will delete the file later
        if(tables.size() < 1 || std::get<0>(tables[0]) == (int)TableSchema::TO_DELETE) {
            file_schema.file_type_ = TableFileSchema::TO_DELETE;
        }

        ConnectorPtr->update(file_schema);

    } catch (std::exception &e) {
        std::string msg = "Exception update table file: table_id = " + file_schema.table_id_
            + " file_id = " + file_schema.file_id_;
        return HandleException(msg, e);
    }
    return Status::OK();
}

Status SqliteMetaImpl::UpdateTableFilesToIndex(const std::string& table_id) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        ConnectorPtr->update_all(
            set(
                c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_INDEX
            ),
            where(
                c(&TableFileSchema::table_id_) == table_id and
                c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW
            ));
    } catch (std::exception &e) {
        return HandleException("Encounter exception when update table files to to_index", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::UpdateTableFiles(TableFilesSchema &files) {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::map<std::string, bool> has_tables;
        for (auto &file : files) {
            if(has_tables.find(file.table_id_) != has_tables.end()) {
                continue;
            }
            auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
                                               where(c(&TableSchema::table_id_) == file.table_id_
                                                     and c(&TableSchema::state_) != (int) TableSchema::TO_DELETE));
            if(tables.size() >= 1) {
                has_tables[file.table_id_] = true;
            } else {
                has_tables[file.table_id_] = false;
            }
        }

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &file : files) {
                if(!has_tables[file.table_id_]) {
                    file.file_type_ = TableFileSchema::TO_DELETE;
                }

                file.updated_time_ = utils::GetMicroSecTimeStamp();
                ConnectorPtr->update(file);
            }
            return true;
        });

        if (!commited) {
            ENGINE_LOG_ERROR << "sqlite transaction failed";
            return Status::DBTransactionError("Update table files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when update table files", e);
    }
    return Status::OK();
}

Status SqliteMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
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
            ENGINE_LOG_ERROR << "sqlite transaction failed";
            return Status::DBTransactionError("Clean files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table files", e);
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
            ENGINE_LOG_ERROR << "sqlite transaction failed";
            return Status::DBTransactionError("Clean files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table files", e);
    }

    //remove deleted table folder
    //don't remove table folder until all its files has been deleted
    try {
        server::MetricCollector metric;

        for(auto& table_id : table_ids) {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::file_id_),
                                                 where(c(&TableFileSchema::table_id_) == table_id));
            if(selected.size() == 0) {
                utils::DeleteTablePath(options_, table_id);
            }
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table folder", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::CleanUp() {
    try {
        server::MetricCollector metric;

        //multi-threads call sqlite update may get exception('bad logic', etc), so we add a lock here
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        std::vector<int> file_type = {(int) TableFileSchema::NEW, (int) TableFileSchema::NEW_INDEX, (int) TableFileSchema::NEW_MERGE};
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id_), where(in(&TableFileSchema::file_type_, file_type)));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &file : files) {
                ENGINE_LOG_DEBUG << "Remove table file type as NEW";
                ConnectorPtr->remove<TableFileSchema>(std::get<0>(file));
            }
            return true;
        });

        if (!commited) {
            ENGINE_LOG_ERROR << "sqlite transaction failed";
            return Status::DBTransactionError("Clean files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table file", e);
    }

    return Status::OK();
}

Status SqliteMetaImpl::Count(const std::string &table_id, uint64_t &result) {

    try {
        server::MetricCollector metric;

        std::vector<int> file_type = {(int) TableFileSchema::RAW, (int) TableFileSchema::TO_INDEX, (int) TableFileSchema::INDEX};
        auto selected = ConnectorPtr->select(columns(&TableFileSchema::row_count_),
                                             where(in(&TableFileSchema::file_type_, file_type)
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
        return HandleException("Encounter exception when calculate table file size", e);
    }
    return Status::OK();
}

Status SqliteMetaImpl::DropAll() {
    if (boost::filesystem::is_directory(options_.path)) {
        boost::filesystem::remove_all(options_.path);
    }
    return Status::OK();
}

SqliteMetaImpl::~SqliteMetaImpl() {
    CleanUp();
}

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
