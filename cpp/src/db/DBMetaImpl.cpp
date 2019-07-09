/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "DBMetaImpl.h"
#include "IDGenerator.h"
#include "Utils.h"
#include "Log.h"
#include "MetaConsts.h"
#include "Factories.h"
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

class MetricCollector {
public:
    MetricCollector() {
        server::Metrics::GetInstance().MetaAccessTotalIncrement();
        start_time_ = METRICS_NOW_TIME;
    }

    ~MetricCollector() {
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time_, end_time);
        server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
    }

private:
    using TIME_POINT = std::chrono::system_clock::time_point;
    TIME_POINT start_time_;
};

}

inline auto StoragePrototype(const std::string &path) {
    return make_storage(path,
                        make_table("Tables",
                                   make_column("id", &TableSchema::id_, primary_key()),
                                   make_column("table_id", &TableSchema::table_id_, unique()),
                                   make_column("state", &TableSchema::state_),
                                   make_column("dimension", &TableSchema::dimension_),
                                   make_column("created_on", &TableSchema::created_on_),
                                   make_column("files_cnt", &TableSchema::files_cnt_, default_value(0)),
                                   make_column("engine_type", &TableSchema::engine_type_),
                                   make_column("store_raw_data", &TableSchema::store_raw_data_)),
                        make_table("TableFiles",
                                   make_column("id", &TableFileSchema::id_, primary_key()),
                                   make_column("table_id", &TableFileSchema::table_id_),
                                   make_column("engine_type", &TableFileSchema::engine_type_),
                                   make_column("file_id", &TableFileSchema::file_id_),
                                   make_column("file_type", &TableFileSchema::file_type_),
                                   make_column("size", &TableFileSchema::size_, default_value(0)),
                                   make_column("updated_time", &TableFileSchema::updated_time_),
                                   make_column("created_on", &TableFileSchema::created_on_),
                                   make_column("date", &TableFileSchema::date_))
    );

}

using ConnectorT = decltype(StoragePrototype(""));
static std::unique_ptr<ConnectorT> ConnectorPtr;
using ConditionT = decltype(c(&TableFileSchema::id_) == 1UL);

std::string DBMetaImpl::GetTablePath(const std::string &table_id) {
    return options_.path + "/tables/" + table_id;
}

std::string DBMetaImpl::GetTableDatePartitionPath(const std::string &table_id, DateT &date) {
    std::stringstream ss;
    ss << GetTablePath(table_id) << "/" << date;
    return ss.str();
}

void DBMetaImpl::GetTableFilePath(TableFileSchema &group_file) {
    if (group_file.date_ == EmptyDate) {
        group_file.date_ = Meta::GetDate();
    }
    std::stringstream ss;
    ss << GetTableDatePartitionPath(group_file.table_id_, group_file.date_)
       << "/" << group_file.file_id_;
    group_file.location_ = ss.str();
}

Status DBMetaImpl::NextTableId(std::string &table_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    table_id = ss.str();
    return Status::OK();
}

Status DBMetaImpl::NextFileId(std::string &file_id) {
    std::stringstream ss;
    SimpleIDGenerator g;
    ss << g.GetNextIDNumber();
    file_id = ss.str();
    return Status::OK();
}

DBMetaImpl::DBMetaImpl(const DBMetaOptions &options_)
    : options_(options_) {
    Initialize();
}

Status DBMetaImpl::Initialize() {
    if (!boost::filesystem::is_directory(options_.path)) {
        auto ret = boost::filesystem::create_directory(options_.path);
        if (!ret) {
            ENGINE_LOG_ERROR << "Failed to create db directory " << options_.path;
            return Status::DBTransactionError("Failed to create db directory", options_.path);
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
Status DBMetaImpl::DropPartitionsByDates(const std::string &table_id,
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

Status DBMetaImpl::CreateTable(TableSchema &table_schema) {

    try {
        MetricCollector metric;

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

        table_schema.files_cnt_ = 0;
        table_schema.id_ = -1;
        table_schema.created_on_ = utils::GetMicroSecTimeStamp();

        try {
            auto id = ConnectorPtr->insert(table_schema);
            table_schema.id_ = id;
        } catch (...) {
            return Status::DBTransactionError("Add Table Error");
        }

        auto table_path = GetTablePath(table_schema.table_id_);
        table_schema.location_ = table_path;
        if (!boost::filesystem::is_directory(table_path)) {
            auto ret = boost::filesystem::create_directories(table_path);
            if (!ret) {
                ENGINE_LOG_ERROR << "Create directory " << table_path << " Error";
                return Status::Error("Failed to create table path");
            }
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when create table", e);
    }

    return Status::OK();
}

Status DBMetaImpl::DeleteTable(const std::string& table_id) {
    try {
        MetricCollector metric;

        //soft delete table
        auto tables = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::files_cnt_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::engine_type_,
                                                   &TableSchema::store_raw_data_,
                                                   &TableSchema::created_on_),
                                           where(c(&TableSchema::table_id_) == table_id));
        for (auto &table : tables) {
            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            table_schema.state_ = (int)TableSchema::TO_DELETE;
            table_schema.id_ = std::get<0>(table);
            table_schema.files_cnt_ = std::get<1>(table);
            table_schema.dimension_ = std::get<2>(table);
            table_schema.engine_type_ = std::get<3>(table);
            table_schema.store_raw_data_ = std::get<4>(table);
            table_schema.created_on_ = std::get<5>(table);

            ConnectorPtr->update<TableSchema>(table_schema);
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when delete table", e);
    }

    return Status::OK();
}

Status DBMetaImpl::DeleteTableFiles(const std::string& table_id) {
    try {
        MetricCollector metric;

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

Status DBMetaImpl::DescribeTable(TableSchema &table_schema) {
    try {
        MetricCollector metric;

        auto groups = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::table_id_,
                                                   &TableSchema::files_cnt_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::engine_type_,
                                                   &TableSchema::store_raw_data_),
                                           where(c(&TableSchema::table_id_) == table_schema.table_id_
                                                 and c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));

        if (groups.size() == 1) {
            table_schema.id_ = std::get<0>(groups[0]);
            table_schema.files_cnt_ = std::get<2>(groups[0]);
            table_schema.dimension_ = std::get<3>(groups[0]);
            table_schema.engine_type_ = std::get<4>(groups[0]);
            table_schema.store_raw_data_ = std::get<5>(groups[0]);
        } else {
            return Status::NotFound("Table " + table_schema.table_id_ + " not found");
        }

        auto table_path = GetTablePath(table_schema.table_id_);
        table_schema.location_ = table_path;

    } catch (std::exception &e) {
        return HandleException("Encounter exception when describe table", e);
    }

    return Status::OK();
}

Status DBMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {
    has_or_not = false;

    try {
        MetricCollector metric;
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

Status DBMetaImpl::AllTables(std::vector<TableSchema>& table_schema_array) {
    try {
        MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::table_id_,
                                                   &TableSchema::files_cnt_,
                                                   &TableSchema::dimension_,
                                                   &TableSchema::engine_type_,
                                                   &TableSchema::store_raw_data_),
                                             where(c(&TableSchema::state_) != (int)TableSchema::TO_DELETE));
        for (auto &table : selected) {
            TableSchema schema;
            schema.id_ = std::get<0>(table);
            schema.table_id_ = std::get<1>(table);
            schema.files_cnt_ = std::get<2>(table);
            schema.dimension_ = std::get<3>(table);
            schema.engine_type_ = std::get<4>(table);
            schema.store_raw_data_ = std::get<5>(table);

            table_schema_array.emplace_back(schema);
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup all tables", e);
    }

    return Status::OK();
}

Status DBMetaImpl::CreateTableFile(TableFileSchema &file_schema) {
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
        MetricCollector metric;

        NextFileId(file_schema.file_id_);
        file_schema.file_type_ = TableFileSchema::NEW;
        file_schema.dimension_ = table_schema.dimension_;
        file_schema.size_ = 0;
        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
        file_schema.updated_time_ = file_schema.created_on_;
        file_schema.engine_type_ = table_schema.engine_type_;
        GetTableFilePath(file_schema);

        auto id = ConnectorPtr->insert(file_schema);
        file_schema.id_ = id;

        auto partition_path = GetTableDatePartitionPath(file_schema.table_id_, file_schema.date_);

        if (!boost::filesystem::is_directory(partition_path)) {
            auto ret = boost::filesystem::create_directory(partition_path);
            if (!ret) {
                ENGINE_LOG_ERROR << "Create directory " << partition_path << " Error";
                return Status::DBTransactionError("Failed to create partition directory");
            }
        }

    } catch (std::exception& ex) {
        return HandleException("Encounter exception when create table file", ex);
    }

    return Status::OK();
}

Status DBMetaImpl::FilesToIndex(TableFilesSchema &files) {
    files.clear();

    try {
        MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                     &TableFileSchema::table_id_,
                                                     &TableFileSchema::file_id_,
                                                     &TableFileSchema::file_type_,
                                                     &TableFileSchema::size_,
                                                     &TableFileSchema::date_,
                                                     &TableFileSchema::engine_type_),
                                             where(c(&TableFileSchema::file_type_)
                                                       == (int) TableFileSchema::TO_INDEX));

        std::map<std::string, TableSchema> groups;
        TableFileSchema table_file;

        for (auto &file : selected) {
            table_file.id_ = std::get<0>(file);
            table_file.table_id_ = std::get<1>(file);
            table_file.file_id_ = std::get<2>(file);
            table_file.file_type_ = std::get<3>(file);
            table_file.size_ = std::get<4>(file);
            table_file.date_ = std::get<5>(file);
            table_file.engine_type_ = std::get<6>(file);

            GetTableFilePath(table_file);
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
            files.push_back(table_file);
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when iterate raw files", e);
    }

    return Status::OK();
}

Status DBMetaImpl::FilesToSearch(const std::string &table_id,
                                 const DatesT &partition,
                                 DatePartionedTableFilesSchema &files) {
    files.clear();

    try {
        MetricCollector metric;

        if (partition.empty()) {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                         &TableFileSchema::table_id_,
                                                         &TableFileSchema::file_id_,
                                                         &TableFileSchema::file_type_,
                                                         &TableFileSchema::size_,
                                                         &TableFileSchema::date_,
                                                         &TableFileSchema::engine_type_),
                                                 where(c(&TableFileSchema::table_id_) == table_id and
                                                     (c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW or
                                                         c(&TableFileSchema::file_type_)
                                                             == (int) TableFileSchema::TO_INDEX or
                                                         c(&TableFileSchema::file_type_)
                                                             == (int) TableFileSchema::INDEX)));

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
                table_file.size_ = std::get<4>(file);
                table_file.date_ = std::get<5>(file);
                table_file.engine_type_ = std::get<6>(file);
                table_file.dimension_ = table_schema.dimension_;
                GetTableFilePath(table_file);
                auto dateItr = files.find(table_file.date_);
                if (dateItr == files.end()) {
                    files[table_file.date_] = TableFilesSchema();
                }
                files[table_file.date_].push_back(table_file);
            }
        }
        else {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                         &TableFileSchema::table_id_,
                                                         &TableFileSchema::file_id_,
                                                         &TableFileSchema::file_type_,
                                                         &TableFileSchema::size_,
                                                         &TableFileSchema::date_,
                                                         &TableFileSchema::engine_type_),
                                                 where(c(&TableFileSchema::table_id_) == table_id and
                                                     in(&TableFileSchema::date_, partition) and
                                                     (c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW or
                                                         c(&TableFileSchema::file_type_)
                                                             == (int) TableFileSchema::TO_INDEX or
                                                         c(&TableFileSchema::file_type_)
                                                             == (int) TableFileSchema::INDEX)));

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
                table_file.size_ = std::get<4>(file);
                table_file.date_ = std::get<5>(file);
                table_file.engine_type_ = std::get<6>(file);
                table_file.dimension_ = table_schema.dimension_;
                GetTableFilePath(table_file);
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

Status DBMetaImpl::FilesToMerge(const std::string &table_id,
                                DatePartionedTableFilesSchema &files) {
    files.clear();

    try {
        MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                     &TableFileSchema::table_id_,
                                                     &TableFileSchema::file_id_,
                                                     &TableFileSchema::file_type_,
                                                     &TableFileSchema::size_,
                                                     &TableFileSchema::date_),
                                             where(c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW and
                                                 c(&TableFileSchema::table_id_) == table_id),
                                             order_by(&TableFileSchema::size_).desc());

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
            table_file.size_ = std::get<4>(file);
            table_file.date_ = std::get<5>(file);
            table_file.dimension_ = table_schema.dimension_;
            GetTableFilePath(table_file);
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

Status DBMetaImpl::GetTableFiles(const std::string& table_id,
                                 const std::vector<size_t>& ids,
                                 TableFilesSchema& table_files) {
    try {
        table_files.clear();
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                  &TableFileSchema::file_id_,
                                                  &TableFileSchema::file_type_,
                                                  &TableFileSchema::size_,
                                                  &TableFileSchema::date_,
                                                  &TableFileSchema::engine_type_),
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
            file_schema.size_ = std::get<3>(file);
            file_schema.date_ = std::get<4>(file);
            file_schema.engine_type_ = std::get<5>(file);
            file_schema.dimension_ = table_schema.dimension_;
            GetTableFilePath(file_schema);

            table_files.emplace_back(file_schema);
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when lookup table files", e);
    }

    return Status::OK();
}

// PXU TODO: Support Swap
Status DBMetaImpl::Archive() {
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

Status DBMetaImpl::Size(uint64_t &result) {
    result = 0;
    try {
        auto selected = ConnectorPtr->select(columns(sum(&TableFileSchema::size_)),
                                             where(
                                                 c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE
                                             ));

        for (auto &sub_query : selected) {
            if (!std::get<0>(sub_query)) {
                continue;
            }
            result += (uint64_t) (*std::get<0>(sub_query));
        }
    } catch (std::exception &e) {
        return HandleException("Encounter exception when calculte db size", e);
    }

    return Status::OK();
}

Status DBMetaImpl::DiscardFiles(long to_discard_size) {
    if (to_discard_size <= 0) {
        return Status::OK();
    }

    ENGINE_LOG_DEBUG << "About to discard size=" << to_discard_size;

    try {
        MetricCollector metric;

        auto commited = ConnectorPtr->transaction([&]() mutable {
            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
                                                         &TableFileSchema::size_),
                                                 where(c(&TableFileSchema::file_type_)
                                                       != (int) TableFileSchema::TO_DELETE),
                                                 order_by(&TableFileSchema::id_),
                                                 limit(10));

            std::vector<int> ids;
            TableFileSchema table_file;

            for (auto &file : selected) {
                if (to_discard_size <= 0) break;
                table_file.id_ = std::get<0>(file);
                table_file.size_ = std::get<1>(file);
                ids.push_back(table_file.id_);
                ENGINE_LOG_DEBUG << "Discard table_file.id=" << table_file.file_id_
                                 << " table_file.size=" << table_file.size_;
                to_discard_size -= table_file.size_;
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
            return Status::DBTransactionError("Update table file error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when discard table file", e);
    }

    return DiscardFiles(to_discard_size);
}

Status DBMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        MetricCollector metric;

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

Status DBMetaImpl::UpdateTableFilesToIndex(const std::string& table_id) {
    try {
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

Status DBMetaImpl::UpdateTableFiles(TableFilesSchema &files) {
    try {
        MetricCollector metric;

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
            return Status::DBTransactionError("Update table files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when update table files", e);
    }
    return Status::OK();
}

Status DBMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
    auto now = utils::GetMicroSecTimeStamp();
    try {
        MetricCollector metric;

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
                GetTableFilePath(table_file);

                ENGINE_LOG_DEBUG << "Removing deleted id =" << table_file.id_ << " location = " << table_file.location_ << std::endl;
                boost::filesystem::remove(table_file.location_);
                ConnectorPtr->remove<TableFileSchema>(table_file.id_);

            }
            return true;
        });

        if (!commited) {
            return Status::DBTransactionError("Clean files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table files", e);
    }

    try {
        MetricCollector metric;

        auto tables = ConnectorPtr->select(columns(&TableSchema::id_,
                                                   &TableSchema::table_id_),
                                           where(c(&TableSchema::state_) == (int) TableSchema::TO_DELETE));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &table : tables) {
                auto table_path = GetTablePath(std::get<1>(table));

                ENGINE_LOG_DEBUG << "Remove table folder: " << table_path;
                boost::filesystem::remove_all(table_path);
                ConnectorPtr->remove<TableSchema>(std::get<0>(table));
            }

            return true;
        });

        if (!commited) {
            return Status::DBTransactionError("Clean files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table files", e);
    }

    return Status::OK();
}

Status DBMetaImpl::CleanUp() {
    try {
        auto files = ConnectorPtr->select(columns(&TableFileSchema::id_),
                                             where(c(&TableFileSchema::file_type_) == (int) TableFileSchema::NEW));

        auto commited = ConnectorPtr->transaction([&]() mutable {
            for (auto &file : files) {
                ENGINE_LOG_DEBUG << "Remove table file type as NEW";
                ConnectorPtr->remove<TableFileSchema>(std::get<0>(file));
            }
            return true;
        });

        if (!commited) {
            return Status::DBTransactionError("Clean files error");
        }

    } catch (std::exception &e) {
        return HandleException("Encounter exception when clean table file", e);
    }

    return Status::OK();
}

Status DBMetaImpl::Count(const std::string &table_id, uint64_t &result) {

    try {
        MetricCollector metric;

        auto selected = ConnectorPtr->select(columns(&TableFileSchema::size_),
                                             where((c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW
                                                    or
                                                    c(&TableFileSchema::file_type_) == (int) TableFileSchema::TO_INDEX
                                                    or c(&TableFileSchema::file_type_) == (int) TableFileSchema::INDEX)
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

        result /= table_schema.dimension_;
        result /= sizeof(float);

    } catch (std::exception &e) {
        return HandleException("Encounter exception when calculate table file size", e);
    }
    return Status::OK();
}

Status DBMetaImpl::DropAll() {
    if (boost::filesystem::is_directory(options_.path)) {
        boost::filesystem::remove_all(options_.path);
    }
    return Status::OK();
}

DBMetaImpl::~DBMetaImpl() {
    CleanUp();
}

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
