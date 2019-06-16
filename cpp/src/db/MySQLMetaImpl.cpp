/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "MySQLMetaImpl.h"
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
#include <regex>
#include <string>

#include "mysql++/mysql++.h"

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

    using namespace mysqlpp;

//    static std::unique_ptr<Connection> connectionPtr(new Connection());
    static Connection* connectionPtr = new Connection();

    namespace {

        void HandleException(std::exception &e) {
            ENGINE_LOG_DEBUG << "Engine meta exception: " << e.what();
            throw e;
        }

    }

    std::string MySQLMetaImpl::GetTablePath(const std::string &table_id) {
        return options_.path + "/tables/" + table_id;
    }

    std::string MySQLMetaImpl::GetTableDatePartitionPath(const std::string &table_id, DateT &date) {
        std::stringstream ss;
        ss << GetTablePath(table_id) << "/" << date;
        return ss.str();
    }

    void MySQLMetaImpl::GetTableFilePath(TableFileSchema &group_file) {
        if (group_file.date_ == EmptyDate) {
            group_file.date_ = Meta::GetDate();
        }
        std::stringstream ss;
        ss << GetTableDatePartitionPath(group_file.table_id_, group_file.date_)
           << "/" << group_file.file_id_;
        group_file.location_ = ss.str();
    }

    Status MySQLMetaImpl::NextTableId(std::string &table_id) {
        std::stringstream ss;
        SimpleIDGenerator g;
        ss << g.GetNextIDNumber();
        table_id = ss.str();
        return Status::OK();
    }

    Status MySQLMetaImpl::NextFileId(std::string &file_id) {
        std::stringstream ss;
        SimpleIDGenerator g;
        ss << g.GetNextIDNumber();
        file_id = ss.str();
        return Status::OK();
    }

    MySQLMetaImpl::MySQLMetaImpl(const DBMetaOptions &options_)
            : options_(options_) {
        //Initialize();
    }

    Status MySQLMetaImpl::Initialize() {
//        if (!boost::filesystem::is_directory(options_.path)) {
//            auto ret = boost::filesystem::create_directory(options_.path);
//            if (!ret) {
//                ENGINE_LOG_ERROR << "Create directory " << options_.path << " Error";
//            }
//            assert(ret);
//        }

//        ConnectorPtr = std::make_unique<ConnectorT>(StoragePrototype(options_.path + "/meta.sqlite"));
//
//        ConnectorPtr->sync_schema();
//        ConnectorPtr->open_forever(); // thread safe option
//        ConnectorPtr->pragma.journal_mode(journal_mode::WAL); // WAL => write ahead log

        std::string uri = options_.backend_uri;

        std::string dialectRegex = "(.*)";
        std::string usernameRegex = "(.*)";
        std::string passwordRegex = "(.*)";
        std::string hostRegex = "(.*)";
        std::string portRegex = "(.*)";
        std::string dbNameRegex = "(.*)";
        std::string uriRegexStr = dialectRegex + "\\:\\/\\/" +
                                  usernameRegex + "\\:" +
                                  passwordRegex + "\\@" +
                                  hostRegex + "\\:" +
                                  portRegex + "\\/" +
                                  dbNameRegex;
        std::regex uriRegex(uriRegexStr);
        std::smatch pieces_match;

        if (std::regex_match(uri, pieces_match, uriRegex)) {
            std::string dialect = pieces_match[1].str();
            std::transform(dialect.begin(), dialect.end(), dialect.begin(), ::tolower);
            if (dialect.find("mysql") == std::string::npos) {
                return Status::Error("URI's dialect is not MySQL");
            }
            const char* username = pieces_match[2].str().c_str();
            const char* password = pieces_match[3].str().c_str();
            const char* serverAddress = pieces_match[4].str().c_str();
            unsigned int port = 0;
            if (!pieces_match[5].str().empty()) {
                port = std::stoi(pieces_match[5].str());
            }
            const char* dbName = pieces_match[6].str().c_str();
            //std::cout << dbName << " " << serverAddress << " " << username << " " << password << " " << port << std::endl;
            connectionPtr->set_option(new MultiStatementsOption(true));

            try {
                if (!connectionPtr->connect(dbName, serverAddress, username, password, port)) {
                    return Status::Error("DB connection failed: ", connectionPtr->error());
                }

                CleanUp();

                Query InitializeQuery = connectionPtr->query();

                InitializeQuery << "DROP TABLE IF EXISTS meta, metaFile;";
                InitializeQuery << "CREATE TABLE meta (" <<
                                    "id BIGINT AUTO INCREMENT PRIMARY KEY, " <<
                                    "table_id VARCHAR(255) UNIQUE, " <<
                                    "dimension SMALLINT, " <<
                                    "created_on BIGINT, " <<
                                    "files_cnt BIGINT DEFAULT 0, " <<
                                    "engine_type INT DEFAULT 1, " <<
                                    "store_raw_data BOOL DEFAULT false);";
                InitializeQuery << "CREATE TABLE metaFile (" <<
                                   "id BIGINT AUTO INCREMENT PRIMARY KEY, " <<
                                   "table_id VARCHAR(255), " <<
                                   "engine_type INT DEFAULT 1, " <<
                                   "file_id VARCHAR(255), " <<
                                   "file_type INT DEFAULT 0, " <<
                                   "size BIGINT DEFAULT 0, " <<
                                   "updated_time BIGINT, " <<
                                   "created_on BIGINT, " <<
                                   "date INT DEFAULT -1);";

                if (InitializeQuery.exec()) {
                    return Status::OK();
                } else {
                    return Status::DBTransactionError("Initialization Error: ", InitializeQuery.error());
                }
            } catch (const ConnectionFailed& er) {
                return Status::DBTransactionError("Failed to connect to MySQL server: ", er.what());
            } catch (const BadQuery& er) {
                // Handle any query errors
                return Status::DBTransactionError("QUERY ERROR DURING INITIALIZATION: ", er.what());
            } catch (const Exception& er) {
                // Catch-all for any other MySQL++ exceptions
                return Status::DBTransactionError("GENERAL ERROR DURING INITIALIZATION: ", er.what());
            }
        }
        else {
            return Status::Error("Wrong URI format");
        }
    }

// PXU TODO: Temp solution. Will fix later
    Status MySQLMetaImpl::DropPartitionsByDates(const std::string &table_id,
                                             const DatesT &dates) {
//        if (dates.size() == 0) {
//            return Status::OK();
//        }
//
//        TableSchema table_schema;
//        table_schema.table_id_ = table_id;
//        auto status = DescribeTable(table_schema);
//        if (!status.ok()) {
//            return status;
//        }
//
//        auto yesterday = GetDateWithDelta(-1);
//
//        for (auto &date : dates) {
//            if (date >= yesterday) {
//                return Status::Error("Could not delete partitions with 2 days");
//            }
//        }
//
//        try {
//            ConnectorPtr->update_all(
//                    set(
//                            c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE
//                    ),
//                    where(
//                            c(&TableFileSchema::table_id_) == table_id and
//                            in(&TableFileSchema::date_, dates)
//                    ));
//        } catch (std::exception &e) {
//            HandleException(e);
//        }
        return Status::OK();
    }

    Status MySQLMetaImpl::CreateTable(TableSchema &table_schema) {
//        server::Metrics::GetInstance().MetaAccessTotalIncrement();
//        if (table_schema.table_id_.empty()) {
//            NextTableId(table_schema.table_id_);
//        }
//        table_schema.files_cnt_ = 0;
//        table_schema.id_ = -1;
//        table_schema.created_on_ = utils::GetMicroSecTimeStamp();
//        auto start_time = METRICS_NOW_TIME;
//        {
//            try {
//                Query addTableQuery = connectionPtr->query();
//
//            } catch (...) {
//                return Status::DBTransactionError("Add Table Error");
//            }
//        }
//        auto end_time = METRICS_NOW_TIME;
//        auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//        server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//
//        auto table_path = GetTablePath(table_schema.table_id_);
//        table_schema.location_ = table_path;
//        if (!boost::filesystem::is_directory(table_path)) {
//            auto ret = boost::filesystem::create_directories(table_path);
//            if (!ret) {
//                ENGINE_LOG_ERROR << "Create directory " << table_path << " Error";
//            }
//            assert(ret);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DeleteTable(const std::string& table_id) {
//        try {
//            //drop the table from meta
//            auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
//                                               where(c(&TableSchema::table_id_) == table_id));
//            for (auto &table : tables) {
//                ConnectorPtr->remove<TableSchema>(std::get<0>(table));
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DescribeTable(TableSchema &table_schema) {
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            auto groups = ConnectorPtr->select(columns(&TableSchema::id_,
//                                                       &TableSchema::table_id_,
//                                                       &TableSchema::files_cnt_,
//                                                       &TableSchema::dimension_,
//                                                       &TableSchema::engine_type_,
//                                                       &TableSchema::store_raw_data_),
//                                               where(c(&TableSchema::table_id_) == table_schema.table_id_));
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//            assert(groups.size() <= 1);
//            if (groups.size() == 1) {
//                table_schema.id_ = std::get<0>(groups[0]);
//                table_schema.files_cnt_ = std::get<2>(groups[0]);
//                table_schema.dimension_ = std::get<3>(groups[0]);
//                table_schema.engine_type_ = std::get<4>(groups[0]);
//                table_schema.store_raw_data_ = std::get<5>(groups[0]);
//            } else {
//                return Status::NotFound("Table " + table_schema.table_id_ + " not found");
//            }
//
//            auto table_path = GetTablePath(table_schema.table_id_);
//            table_schema.location_ = table_path;
//
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//
//            auto tables = ConnectorPtr->select(columns(&TableSchema::id_),
//                                               where(c(&TableSchema::table_id_) == table_id));
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//            assert(tables.size() <= 1);
//            if (tables.size() == 1) {
//                has_or_not = true;
//            } else {
//                has_or_not = false;
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }
        return Status::OK();
    }

    Status MySQLMetaImpl::AllTables(std::vector<TableSchema>& table_schema_array) {
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            auto selected = ConnectorPtr->select(columns(&TableSchema::id_,
//                                                         &TableSchema::table_id_,
//                                                         &TableSchema::files_cnt_,
//                                                         &TableSchema::dimension_,
//                                                         &TableSchema::engine_type_,
//                                                         &TableSchema::store_raw_data_));
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//            for (auto &table : selected) {
//                TableSchema schema;
//                schema.id_ = std::get<0>(table);
//                schema.table_id_ = std::get<1>(table);
//                schema.files_cnt_ = std::get<2>(table);
//                schema.dimension_ = std::get<3>(table);
//                schema.engine_type_ = std::get<4>(table);
//                schema.store_raw_data_ = std::get<5>(table);
//
//                table_schema_array.emplace_back(schema);
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::CreateTableFile(TableFileSchema &file_schema) {
//        if (file_schema.date_ == EmptyDate) {
//            file_schema.date_ = Meta::GetDate();
//        }
//        TableSchema table_schema;
//        table_schema.table_id_ = file_schema.table_id_;
//        auto status = DescribeTable(table_schema);
//        if (!status.ok()) {
//            return status;
//        }
//
//        NextFileId(file_schema.file_id_);
//        file_schema.file_type_ = TableFileSchema::NEW;
//        file_schema.dimension_ = table_schema.dimension_;
//        file_schema.size_ = 0;
//        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
//        file_schema.updated_time_ = file_schema.created_on_;
//        file_schema.engine_type_ = table_schema.engine_type_;
//        GetTableFilePath(file_schema);
//
//        {
//            try {
//                server::Metrics::GetInstance().MetaAccessTotalIncrement();
//                auto start_time = METRICS_NOW_TIME;
//                auto id = ConnectorPtr->insert(file_schema);
//                file_schema.id_ = id;
//                auto end_time = METRICS_NOW_TIME;
//                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//            } catch (...) {
//                return Status::DBTransactionError("Add file Error");
//            }
//        }
//
//        auto partition_path = GetTableDatePartitionPath(file_schema.table_id_, file_schema.date_);
//
//        if (!boost::filesystem::is_directory(partition_path)) {
//            auto ret = boost::filesystem::create_directory(partition_path);
//            if (!ret) {
//                ENGINE_LOG_ERROR << "Create directory " << partition_path << " Error";
//            }
//            assert(ret);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToIndex(TableFilesSchema &files) {
//        files.clear();
//
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                         &TableFileSchema::table_id_,
//                                                         &TableFileSchema::file_id_,
//                                                         &TableFileSchema::file_type_,
//                                                         &TableFileSchema::size_,
//                                                         &TableFileSchema::date_,
//                                                         &TableFileSchema::engine_type_),
//                                                 where(c(&TableFileSchema::file_type_)
//                                                       == (int) TableFileSchema::TO_INDEX));
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//
//            std::map<std::string, TableSchema> groups;
//            TableFileSchema table_file;
//
//            for (auto &file : selected) {
//                table_file.id_ = std::get<0>(file);
//                table_file.table_id_ = std::get<1>(file);
//                table_file.file_id_ = std::get<2>(file);
//                table_file.file_type_ = std::get<3>(file);
//                table_file.size_ = std::get<4>(file);
//                table_file.date_ = std::get<5>(file);
//                table_file.engine_type_ = std::get<6>(file);
//
//                GetTableFilePath(table_file);
//                auto groupItr = groups.find(table_file.table_id_);
//                if (groupItr == groups.end()) {
//                    TableSchema table_schema;
//                    table_schema.table_id_ = table_file.table_id_;
//                    auto status = DescribeTable(table_schema);
//                    if (!status.ok()) {
//                        return status;
//                    }
//                    groups[table_file.table_id_] = table_schema;
//                }
//                table_file.dimension_ = groups[table_file.table_id_].dimension_;
//                files.push_back(table_file);
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToSearch(const std::string &table_id,
                                     const DatesT &partition,
                                     DatePartionedTableFilesSchema &files) {
//        files.clear();
//
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            if (partition.empty()) {
//                auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                             &TableFileSchema::table_id_,
//                                                             &TableFileSchema::file_id_,
//                                                             &TableFileSchema::file_type_,
//                                                             &TableFileSchema::size_,
//                                                             &TableFileSchema::date_,
//                                                             &TableFileSchema::engine_type_),
//                                                     where(c(&TableFileSchema::table_id_) == table_id and
//                                                           (c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW or
//                                                            c(&TableFileSchema::file_type_)
//                                                            == (int) TableFileSchema::TO_INDEX or
//                                                            c(&TableFileSchema::file_type_)
//                                                            == (int) TableFileSchema::INDEX)));
//                auto end_time = METRICS_NOW_TIME;
//                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//                TableSchema table_schema;
//                table_schema.table_id_ = table_id;
//                auto status = DescribeTable(table_schema);
//                if (!status.ok()) {
//                    return status;
//                }
//
//                TableFileSchema table_file;
//
//                for (auto &file : selected) {
//                    table_file.id_ = std::get<0>(file);
//                    table_file.table_id_ = std::get<1>(file);
//                    table_file.file_id_ = std::get<2>(file);
//                    table_file.file_type_ = std::get<3>(file);
//                    table_file.size_ = std::get<4>(file);
//                    table_file.date_ = std::get<5>(file);
//                    table_file.engine_type_ = std::get<6>(file);
//                    table_file.dimension_ = table_schema.dimension_;
//                    GetTableFilePath(table_file);
//                    auto dateItr = files.find(table_file.date_);
//                    if (dateItr == files.end()) {
//                        files[table_file.date_] = TableFilesSchema();
//                    }
//                    files[table_file.date_].push_back(table_file);
//                }
//            }
//            else {
//                auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                             &TableFileSchema::table_id_,
//                                                             &TableFileSchema::file_id_,
//                                                             &TableFileSchema::file_type_,
//                                                             &TableFileSchema::size_,
//                                                             &TableFileSchema::date_),
//                                                     where(c(&TableFileSchema::table_id_) == table_id and
//                                                           in(&TableFileSchema::date_, partition) and
//                                                           (c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW or
//                                                            c(&TableFileSchema::file_type_)
//                                                            == (int) TableFileSchema::TO_INDEX or
//                                                            c(&TableFileSchema::file_type_)
//                                                            == (int) TableFileSchema::INDEX)));
//                auto end_time = METRICS_NOW_TIME;
//                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//                TableSchema table_schema;
//                table_schema.table_id_ = table_id;
//                auto status = DescribeTable(table_schema);
//                if (!status.ok()) {
//                    return status;
//                }
//
//                TableFileSchema table_file;
//
//                for (auto &file : selected) {
//                    table_file.id_ = std::get<0>(file);
//                    table_file.table_id_ = std::get<1>(file);
//                    table_file.file_id_ = std::get<2>(file);
//                    table_file.file_type_ = std::get<3>(file);
//                    table_file.size_ = std::get<4>(file);
//                    table_file.date_ = std::get<5>(file);
//                    table_file.dimension_ = table_schema.dimension_;
//                    GetTableFilePath(table_file);
//                    auto dateItr = files.find(table_file.date_);
//                    if (dateItr == files.end()) {
//                        files[table_file.date_] = TableFilesSchema();
//                    }
//                    files[table_file.date_].push_back(table_file);
//                }
//
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToMerge(const std::string &table_id,
                                    DatePartionedTableFilesSchema &files) {
//        files.clear();
//
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                         &TableFileSchema::table_id_,
//                                                         &TableFileSchema::file_id_,
//                                                         &TableFileSchema::file_type_,
//                                                         &TableFileSchema::size_,
//                                                         &TableFileSchema::date_),
//                                                 where(c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW and
//                                                       c(&TableFileSchema::table_id_) == table_id));
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//            TableSchema table_schema;
//            table_schema.table_id_ = table_id;
//            auto status = DescribeTable(table_schema);
//
//            if (!status.ok()) {
//                return status;
//            }
//
//            TableFileSchema table_file;
//            for (auto &file : selected) {
//                table_file.id_ = std::get<0>(file);
//                table_file.table_id_ = std::get<1>(file);
//                table_file.file_id_ = std::get<2>(file);
//                table_file.file_type_ = std::get<3>(file);
//                table_file.size_ = std::get<4>(file);
//                table_file.date_ = std::get<5>(file);
//                table_file.dimension_ = table_schema.dimension_;
//                GetTableFilePath(table_file);
//                auto dateItr = files.find(table_file.date_);
//                if (dateItr == files.end()) {
//                    files[table_file.date_] = TableFilesSchema();
//                }
//                files[table_file.date_].push_back(table_file);
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToDelete(const std::string& table_id,
                                     const DatesT& partition,
                                     DatePartionedTableFilesSchema& files) {
//        auto now = utils::GetMicroSecTimeStamp();
//        try {
//            if(partition.empty()) {
//                //step 1: get table files by dates
//                auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                             &TableFileSchema::table_id_,
//                                                             &TableFileSchema::file_id_,
//                                                             &TableFileSchema::size_,
//                                                             &TableFileSchema::date_),
//                                                     where(c(&TableFileSchema::file_type_) !=
//                                                           (int) TableFileSchema::TO_DELETE
//                                                           and c(&TableFileSchema::table_id_) == table_id));
//
//                //step 2: erase table files from meta
//                for (auto &file : selected) {
//                    TableFileSchema table_file;
//                    table_file.id_ = std::get<0>(file);
//                    table_file.table_id_ = std::get<1>(file);
//                    table_file.file_id_ = std::get<2>(file);
//                    table_file.size_ = std::get<3>(file);
//                    table_file.date_ = std::get<4>(file);
//                    GetTableFilePath(table_file);
//                    auto dateItr = files.find(table_file.date_);
//                    if (dateItr == files.end()) {
//                        files[table_file.date_] = TableFilesSchema();
//                    }
//                    files[table_file.date_].push_back(table_file);
//
//                    ConnectorPtr->remove<TableFileSchema>(std::get<0>(file));
//                }
//
//            } else {
//                //step 1: get all table files
//                auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                             &TableFileSchema::table_id_,
//                                                             &TableFileSchema::file_id_,
//                                                             &TableFileSchema::size_,
//                                                             &TableFileSchema::date_),
//                                                     where(c(&TableFileSchema::file_type_) !=
//                                                           (int) TableFileSchema::TO_DELETE
//                                                           and in(&TableFileSchema::date_, partition)
//                                                           and c(&TableFileSchema::table_id_) == table_id));
//
//                //step 2: erase table files from meta
//                for (auto &file : selected) {
//                    TableFileSchema table_file;
//                    table_file.id_ = std::get<0>(file);
//                    table_file.table_id_ = std::get<1>(file);
//                    table_file.file_id_ = std::get<2>(file);
//                    table_file.size_ = std::get<3>(file);
//                    table_file.date_ = std::get<4>(file);
//                    GetTableFilePath(table_file);
//                    auto dateItr = files.find(table_file.date_);
//                    if (dateItr == files.end()) {
//                        files[table_file.date_] = TableFilesSchema();
//                    }
//                    files[table_file.date_].push_back(table_file);
//
//                    ConnectorPtr->remove<TableFileSchema>(std::get<0>(file));
//                }
//            }
//
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::GetTableFile(TableFileSchema &file_schema) {

//        try {
//            auto files = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                      &TableFileSchema::table_id_,
//                                                      &TableFileSchema::file_id_,
//                                                      &TableFileSchema::file_type_,
//                                                      &TableFileSchema::size_,
//                                                      &TableFileSchema::date_),
//                                              where(c(&TableFileSchema::file_id_) == file_schema.file_id_ and
//                                                    c(&TableFileSchema::table_id_) == file_schema.table_id_
//                                              ));
//            assert(files.size() <= 1);
//            if (files.size() == 1) {
//                file_schema.id_ = std::get<0>(files[0]);
//                file_schema.table_id_ = std::get<1>(files[0]);
//                file_schema.file_id_ = std::get<2>(files[0]);
//                file_schema.file_type_ = std::get<3>(files[0]);
//                file_schema.size_ = std::get<4>(files[0]);
//                file_schema.date_ = std::get<5>(files[0]);
//            } else {
//                return Status::NotFound("Table:" + file_schema.table_id_ +
//                                        " File:" + file_schema.file_id_ + " not found");
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

// PXU TODO: Support Swap
    Status MySQLMetaImpl::Archive() {
//        auto &criterias = options_.archive_conf.GetCriterias();
//        if (criterias.size() == 0) {
//            return Status::OK();
//        }
//
//        for (auto kv : criterias) {
//            auto &criteria = kv.first;
//            auto &limit = kv.second;
//            if (criteria == "days") {
//                long usecs = limit * D_SEC * US_PS;
//                long now = utils::GetMicroSecTimeStamp();
//                try {
//                    ConnectorPtr->update_all(
//                            set(
//                                    c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE
//                            ),
//                            where(
//                                    c(&TableFileSchema::created_on_) < (long) (now - usecs) and
//                                    c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE
//                            ));
//                } catch (std::exception &e) {
//                    HandleException(e);
//                }
//            }
//            if (criteria == "disk") {
//                uint64_t sum = 0;
//                Size(sum);
//
//                auto to_delete = (sum - limit * G);
//                DiscardFiles(to_delete);
//            }
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::Size(uint64_t &result) {
//        result = 0;
//        try {
//            auto selected = ConnectorPtr->select(columns(sum(&TableFileSchema::size_)),
//                                                 where(
//                                                         c(&TableFileSchema::file_type_) != (int) TableFileSchema::TO_DELETE
//                                                 ));
//
//            for (auto &sub_query : selected) {
//                if (!std::get<0>(sub_query)) {
//                    continue;
//                }
//                result += (uint64_t) (*std::get<0>(sub_query));
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DiscardFiles(long to_discard_size) {
//        LOG(DEBUG) << "About to discard size=" << to_discard_size;
//        if (to_discard_size <= 0) {
//            return Status::OK();
//        }
//        try {
//            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                         &TableFileSchema::size_),
//                                                 where(c(&TableFileSchema::file_type_)
//                                                       != (int) TableFileSchema::TO_DELETE),
//                                                 order_by(&TableFileSchema::id_),
//                                                 limit(10));
//
//            std::vector<int> ids;
//            TableFileSchema table_file;
//
//            for (auto &file : selected) {
//                if (to_discard_size <= 0) break;
//                table_file.id_ = std::get<0>(file);
//                table_file.size_ = std::get<1>(file);
//                ids.push_back(table_file.id_);
//                ENGINE_LOG_DEBUG << "Discard table_file.id=" << table_file.file_id_
//                                 << " table_file.size=" << table_file.size_;
//                to_discard_size -= table_file.size_;
//            }
//
//            if (ids.size() == 0) {
//                return Status::OK();
//            }
//
//            ConnectorPtr->update_all(
//                    set(
//                            c(&TableFileSchema::file_type_) = (int) TableFileSchema::TO_DELETE
//                    ),
//                    where(
//                            in(&TableFileSchema::id_, ids)
//                    ));
//
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return DiscardFiles(to_discard_size);
    }

    Status MySQLMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {
//        file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            ConnectorPtr->update(file_schema);
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//        } catch (std::exception &e) {
//            ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
//            HandleException(e);
//        }
        return Status::OK();
    }

    Status MySQLMetaImpl::UpdateTableFiles(TableFilesSchema &files) {
//        try {
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            auto commited = ConnectorPtr->transaction([&]() mutable {
//                for (auto &file : files) {
//                    file.updated_time_ = utils::GetMicroSecTimeStamp();
//                    ConnectorPtr->update(file);
//                }
//                auto end_time = METRICS_NOW_TIME;
//                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//                return true;
//            });
//            if (!commited) {
//                return Status::DBTransactionError("Update files Error");
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }
        return Status::OK();
    }

    Status MySQLMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
//        auto now = utils::GetMicroSecTimeStamp();
//        try {
//            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                         &TableFileSchema::table_id_,
//                                                         &TableFileSchema::file_id_,
//                                                         &TableFileSchema::file_type_,
//                                                         &TableFileSchema::size_,
//                                                         &TableFileSchema::date_),
//                                                 where(
//                                                         c(&TableFileSchema::file_type_) == (int) TableFileSchema::TO_DELETE
//                                                         and
//                                                         c(&TableFileSchema::updated_time_)
//                                                         > now - seconds * US_PS));
//
//            TableFilesSchema updated;
//            TableFileSchema table_file;
//
//            for (auto &file : selected) {
//                table_file.id_ = std::get<0>(file);
//                table_file.table_id_ = std::get<1>(file);
//                table_file.file_id_ = std::get<2>(file);
//                table_file.file_type_ = std::get<3>(file);
//                table_file.size_ = std::get<4>(file);
//                table_file.date_ = std::get<5>(file);
//                GetTableFilePath(table_file);
//                if (table_file.file_type_ == TableFileSchema::TO_DELETE) {
//                    boost::filesystem::remove(table_file.location_);
//                }
//                ConnectorPtr->remove<TableFileSchema>(table_file.id_);
//                /* LOG(DEBUG) << "Removing deleted id=" << table_file.id << " location=" << table_file.location << std::endl; */
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::CleanUp() {
//        try {
//            auto selected = ConnectorPtr->select(columns(&TableFileSchema::id_,
//                                                         &TableFileSchema::table_id_,
//                                                         &TableFileSchema::file_id_,
//                                                         &TableFileSchema::file_type_,
//                                                         &TableFileSchema::size_,
//                                                         &TableFileSchema::date_),
//                                                 where(
//                                                         c(&TableFileSchema::file_type_) == (int) TableFileSchema::TO_DELETE
//                                                         or
//                                                         c(&TableFileSchema::file_type_)
//                                                         == (int) TableFileSchema::NEW));
//
//            TableFilesSchema updated;
//            TableFileSchema table_file;
//
//            for (auto &file : selected) {
//                table_file.id_ = std::get<0>(file);
//                table_file.table_id_ = std::get<1>(file);
//                table_file.file_id_ = std::get<2>(file);
//                table_file.file_type_ = std::get<3>(file);
//                table_file.size_ = std::get<4>(file);
//                table_file.date_ = std::get<5>(file);
//                GetTableFilePath(table_file);
//                if (table_file.file_type_ == TableFileSchema::TO_DELETE) {
//                    boost::filesystem::remove(table_file.location_);
//                }
//                ConnectorPtr->remove<TableFileSchema>(table_file.id_);
//                /* LOG(DEBUG) << "Removing id=" << table_file.id << " location=" << table_file.location << std::endl; */
//            }
//        } catch (std::exception &e) {
//            HandleException(e);
//        }

        return Status::OK();
    }

    Status MySQLMetaImpl::Count(const std::string &table_id, uint64_t &result) {

//        try {
//
//            server::Metrics::GetInstance().MetaAccessTotalIncrement();
//            auto start_time = METRICS_NOW_TIME;
//            auto selected = ConnectorPtr->select(columns(&TableFileSchema::size_,
//                                                         &TableFileSchema::date_),
//                                                 where((c(&TableFileSchema::file_type_) == (int) TableFileSchema::RAW or
//                                                        c(&TableFileSchema::file_type_) == (int) TableFileSchema::TO_INDEX
//                                                        or
//                                                        c(&TableFileSchema::file_type_) == (int) TableFileSchema::INDEX)
//                                                       and
//                                                       c(&TableFileSchema::table_id_) == table_id));
//            auto end_time = METRICS_NOW_TIME;
//            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
//            TableSchema table_schema;
//            table_schema.table_id_ = table_id;
//            auto status = DescribeTable(table_schema);
//
//            if (!status.ok()) {
//                return status;
//            }
//
//            result = 0;
//            for (auto &file : selected) {
//                result += std::get<0>(file);
//            }
//
//            result /= table_schema.dimension_;
//            result /= sizeof(float);
//
//        } catch (std::exception &e) {
//            HandleException(e);
//        }
        return Status::OK();
    }

    Status MySQLMetaImpl::DropAll() {
//        if (boost::filesystem::is_directory(options_.path)) {
//            boost::filesystem::remove_all(options_.path);
//        }
        return Status::OK();
    }

    MySQLMetaImpl::~MySQLMetaImpl() {
        CleanUp();
    }

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
