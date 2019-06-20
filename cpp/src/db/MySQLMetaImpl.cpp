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

    static std::unique_ptr<Connection> connectionPtr(new Connection());

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
        Initialize();
    }

    Status MySQLMetaImpl::Initialize() {

        std::string path = options_.path;
        if (!boost::filesystem::is_directory(path)) {
            auto ret = boost::filesystem::create_directory(path);
            if (!ret) {
                ENGINE_LOG_ERROR << "Create directory " << path << " Error";
            }
            assert(ret);
        }

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
//            connectionPtr->set_option(new MultiStatementsOption(true));

            try {
                if (!connectionPtr->connect(dbName, serverAddress, username, password, port)) {
                    return Status::Error("DB connection failed: ", connectionPtr->error());
                }

                CleanUp();
                Query InitializeQuery = connectionPtr->query();

//                InitializeQuery << "DROP TABLE IF EXISTS meta, metaFile;";
                InitializeQuery << "CREATE TABLE IF NOT EXISTS meta (" <<
                                    "id BIGINT PRIMARY KEY AUTO_INCREMENT, " <<
                                    "table_id VARCHAR(255) UNIQUE NOT NULL, " <<
                                    "dimension SMALLINT NOT NULL, " <<
                                    "created_on BIGINT NOT NULL, " <<
                                    "files_cnt BIGINT DEFAULT 0 NOT NULL, " <<
                                    "engine_type INT DEFAULT 1 NOT NULL, " <<
                                    "store_raw_data BOOL DEFAULT false NOT NULL);";
                if (!InitializeQuery.exec()) {
                    return Status::DBTransactionError("Initialization Error", InitializeQuery.error());
                }

                InitializeQuery << "CREATE TABLE IF NOT EXISTS metaFile (" <<
                                   "id BIGINT PRIMARY KEY AUTO_INCREMENT, " <<
                                   "table_id VARCHAR(255) NOT NULL, " <<
                                   "engine_type INT DEFAULT 1 NOT NULL, " <<
                                   "file_id VARCHAR(255) NOT NULL, " <<
                                   "file_type INT DEFAULT 0 NOT NULL, " <<
                                   "size BIGINT DEFAULT 0 NOT NULL, " <<
                                   "updated_time BIGINT NOT NULL, " <<
                                   "created_on BIGINT NOT NULL, " <<
                                   "date INT DEFAULT -1 NOT NULL);";
                if (!InitializeQuery.exec()) {
                    return Status::DBTransactionError("Initialization Error", InitializeQuery.error());
                }

                return Status::OK();

//                if (InitializeQuery.exec()) {
//                    std::cout << "XXXXXXXXXXXXXXXXXXXXXXXXX" << std::endl;
//                    while (InitializeQuery.more_results()) {
//                        InitializeQuery.store_next();
//                    }
//                    return Status::OK();
//                } else {
//                    return Status::DBTransactionError("Initialization Error", InitializeQuery.error());
//                }
            } catch (const ConnectionFailed& er) {
                return Status::DBTransactionError("Failed to connect to database server", er.what());
            } catch (const BadQuery& er) {
                // Handle any query errors
                return Status::DBTransactionError("QUERY ERROR DURING INITIALIZATION", er.what());
            } catch (const Exception& er) {
                // Catch-all for any other MySQL++ exceptions
                return Status::DBTransactionError("GENERAL ERROR DURING INITIALIZATION", er.what());
            }
        }
        else {
            return Status::Error("Wrong URI format");
        }
    }

// PXU TODO: Temp solution. Will fix later
    Status MySQLMetaImpl::DropPartitionsByDates(const std::string &table_id,
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

        auto yesterday = GetDateWithDelta(-1);

        for (auto &date : dates) {
            if (date >= yesterday) {
                return Status::Error("Could not delete partitions within 2 days");
            }
        }

        try {

            Query dropPartitionsByDatesQuery = connectionPtr->query();

            std::stringstream dateListSS;
            for (auto &date : dates) {
                 dateListSS << std::to_string(date) << ", ";
            }
            std::string dateListStr = dateListSS.str();
            dateListStr = dateListStr.substr(0, dateListStr.size() - 2); //remove the last ", "

            dropPartitionsByDatesQuery << "UPDATE metaFile " <<
                                          "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                          "WHERE table_id = " << quote << table_id << " AND " <<
                                          "date in (" << dateListStr << ");";

            if (!dropPartitionsByDatesQuery.exec()) {
                return Status::DBTransactionError("QUERY ERROR WHEN DROPPING PARTITIONS BY DATES", dropPartitionsByDatesQuery.error());
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN DROPPING PARTITIONS BY DATES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN DROPPING PARTITIONS BY DATES", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::CreateTable(TableSchema &table_schema) {
        server::Metrics::GetInstance().MetaAccessTotalIncrement();
        if (table_schema.table_id_.empty()) {
            NextTableId(table_schema.table_id_);
        }
        table_schema.files_cnt_ = 0;
        table_schema.id_ = -1;
        table_schema.created_on_ = utils::GetMicroSecTimeStamp();
        auto start_time = METRICS_NOW_TIME;
        {
            try {
                Query createTableQuery = connectionPtr->query();
                std::string id = "NULL"; //auto-increment
                std::string table_id = table_schema.table_id_;
                std::string dimension = std::to_string(table_schema.dimension_);
                std::string created_on = std::to_string(table_schema.created_on_);
                std::string files_cnt = "0";
                std::string engine_type = std::to_string(table_schema.engine_type_);
                std::string store_raw_data = table_schema.store_raw_data_ ? "true" : "false";
                createTableQuery << "INSERT INTO meta VALUES" <<
                                    "(" << id << ", " << quote << table_id << ", " << dimension << ", " <<
                                    created_on << ", " << files_cnt << ", " << engine_type << ", " << store_raw_data
                                    << ");";
                if (SimpleResult res = createTableQuery.execute()) {
                    table_schema.id_ = res.insert_id(); //Might need to use SELECT LAST_INSERT_ID()?
//                    std::cout << table_schema.id_ << std::endl;
                }
                else {
                    return Status::DBTransactionError("Add Table Error", createTableQuery.error());
                }

            } catch (const BadQuery& er) {
                // Handle any query errors
                return Status::DBTransactionError("QUERY ERROR WHEN ADDING TABLE", er.what());
            } catch (const Exception& er) {
                // Catch-all for any other MySQL++ exceptions
                return Status::DBTransactionError("GENERAL ERROR WHEN ADDING TABLE", er.what());
            }
        }
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time, end_time);
        server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

        auto table_path = GetTablePath(table_schema.table_id_);
        table_schema.location_ = table_path;
        if (!boost::filesystem::is_directory(table_path)) {
            auto ret = boost::filesystem::create_directories(table_path);
            if (!ret) {
                ENGINE_LOG_ERROR << "Create directory " << table_path << " Error";
            }
            assert(ret);
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DeleteTable(const std::string& table_id) {
        try {
            //drop the table from meta
            Query deleteTableQuery = connectionPtr->query();
            deleteTableQuery << "DELETE FROM meta WHERE table_id = " << quote << table_id << ";";
            if (deleteTableQuery.exec()) {
                return Status::OK();
            }
            else {
                return Status::DBTransactionError("Delete Table Error", deleteTableQuery.error());
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN DELETING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN DELETING TABLE", er.what());
        }
    }

    Status MySQLMetaImpl::DescribeTable(TableSchema &table_schema) {
        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query describeTableQuery = connectionPtr->query();
            describeTableQuery << "SELECT id, table_id, dimension, files_cnt, engine_type, store_raw_data " <<
                                  "FROM meta " <<
                                  "WHERE table_id = " << quote << table_schema.table_id_ << ";";
            StoreQueryResult res = describeTableQuery.store();

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            assert(res && res.num_rows() <= 1);
            if (res.num_rows() == 1) {
                const Row& resRow = res[0];

//                std::string id;
//                resRow["id"].to_string(id);
//                table_schema.id_ = std::stoul(id);
                table_schema.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                table_schema.table_id_ = table_id;

//                std::string created_on;
//                resRow["created_on"].to_string(created_on);
//                table_schema.created_on_ = std::stol(created_on);
                table_schema.dimension_ = resRow["dimension"];

//                std::string files_cnt;
//                resRow["files_cnt"].to_string(files_cnt);
//                table_schema.files_cnt_ = std::stoul(files_cnt);
                table_schema.files_cnt_ = resRow["files_cnt"];

//                std::string engine_type;
//                resRow["engine_type"].to_string(engine_type);
//                table_schema.engine_type_ = std::stoi(engine_type);
                table_schema.engine_type_ = resRow["engine_type"];

                table_schema.store_raw_data_ = (resRow["store_raw_data"].compare("true") == 0);
            }
            else {
                return Status::NotFound("Table " + table_schema.table_id_ + " not found");
            }

            auto table_path = GetTablePath(table_schema.table_id_);
            table_schema.location_ = table_path;

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN DESCRIBING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN DESCRIBING TABLE", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {
        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query hasTableQuery = connectionPtr->query();
            //since table_id is a unique column we just need to check whether it exists or not
            hasTableQuery << "SELECT EXISTS (SELECT 1 FROM meta WHERE table_id = " << quote << table_id << ") "
                          << "AS " << quote << "check" << ";";
            StoreQueryResult res = hasTableQuery.store();

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            assert(res && res.num_rows() == 1);
            int check = res[0]["check"];
            has_or_not = (check == 1);

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN CHECKING IF TABLE EXISTS", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN CHECKING IF TABLE EXISTS", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::AllTables(std::vector<TableSchema>& table_schema_array) {
        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query allTablesQuery = connectionPtr->query();
            allTablesQuery << "SELECT id, table_id, dimension, files_cnt, engine_type, store_raw_data " <<
                              "FROM meta;";
            StoreQueryResult res = allTablesQuery.store();

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            for (auto& resRow : res) {
                TableSchema table_schema;

                table_schema.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                table_schema.table_id_ = table_id;

                table_schema.dimension_ = resRow["dimension"];

                table_schema.files_cnt_ = resRow["files_cnt"];

                table_schema.engine_type_ = resRow["engine_type"];

                table_schema.store_raw_data_ = (resRow["store_raw_data"].compare("true") == 0);

                table_schema_array.emplace_back(table_schema);
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN DESCRIBING ALL TABLES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN DESCRIBING ALL TABLES", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::CreateTableFile(TableFileSchema &file_schema) {
        if (file_schema.date_ == EmptyDate) {
            file_schema.date_ = Meta::GetDate();
        }
        TableSchema table_schema;
        table_schema.table_id_ = file_schema.table_id_;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        NextFileId(file_schema.file_id_);
        file_schema.file_type_ = TableFileSchema::NEW;
        file_schema.dimension_ = table_schema.dimension_;
        file_schema.size_ = 0;
        file_schema.created_on_ = utils::GetMicroSecTimeStamp();
        file_schema.updated_time_ = file_schema.created_on_;
        file_schema.engine_type_ = table_schema.engine_type_;
        GetTableFilePath(file_schema);

        {
            try {
                server::Metrics::GetInstance().MetaAccessTotalIncrement();
                auto start_time = METRICS_NOW_TIME;

                Query createTableFileQuery = connectionPtr->query();
                std::string id = "NULL"; //auto-increment
                std::string table_id = file_schema.table_id_;
                std::string engine_type = std::to_string(file_schema.engine_type_);
                std::string file_id = file_schema.file_id_;
                std::string file_type = std::to_string(file_schema.file_type_);
                std::string size = std::to_string(file_schema.size_);
                std::string updated_time = std::to_string(file_schema.updated_time_);
                std::string created_on = std::to_string(file_schema.created_on_);
                std::string date = std::to_string(file_schema.date_);

                createTableFileQuery << "INSERT INTO metaFile VALUES" <<
                                 "(" << id << ", " << quote << table_id << ", " << engine_type << ", " <<
                                 quote << file_id << ", " << file_type << ", " << size << ", " <<
                                 updated_time << ", " << created_on << ", " << date << ");";

                if (SimpleResult res = createTableFileQuery.execute()) {
                    file_schema.id_ = res.insert_id(); //Might need to use SELECT LAST_INSERT_ID()?
                }
                else {
                    return Status::DBTransactionError("Add file Error", createTableFileQuery.error());
                }

                auto end_time = METRICS_NOW_TIME;
                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
            } catch (const BadQuery& er) {
                // Handle any query errors
                return Status::DBTransactionError("QUERY ERROR WHEN ADDING TABLE FILE", er.what());
            } catch (const Exception& er) {
                // Catch-all for any other MySQL++ exceptions
                return Status::DBTransactionError("GENERAL ERROR WHEN ADDING TABLE FILE", er.what());
            }
        }

        auto partition_path = GetTableDatePartitionPath(file_schema.table_id_, file_schema.date_);

        if (!boost::filesystem::is_directory(partition_path)) {
            auto ret = boost::filesystem::create_directory(partition_path);
            if (!ret) {
                ENGINE_LOG_ERROR << "Create directory " << partition_path << " Error";
            }
            assert(ret);
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToIndex(TableFilesSchema &files) {
        files.clear();

        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query filesToIndexQuery = connectionPtr->query();
            filesToIndexQuery << "SELECT id, table_id, engine_type, file_id, file_type, size, date " <<
                                 "FROM metaFile " <<
                                 "WHERE file_type = " << std::to_string(TableFileSchema::TO_INDEX) << ";";
            StoreQueryResult res = filesToIndexQuery.store();

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            std::map<std::string, TableSchema> groups;
            TableFileSchema table_file;
            for (auto& resRow : res) {

                table_file.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                table_file.table_id_ = table_id;

                table_file.engine_type_ = resRow["engine_type"];

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                table_file.file_id_ = file_id;

                table_file.file_type_ = resRow["file_type"];

                table_file.size_ = resRow["size"];

                table_file.date_ = resRow["date"];

                auto groupItr = groups.find(table_file.table_id_);
                if (groupItr == groups.end()) {
                    TableSchema table_schema;
                    table_schema.table_id_ = table_file.table_id_;
                    auto status = DescribeTable(table_schema);
                    if (!status.ok()) {
                        return status;
                    }
                    groups[table_file.table_id_] = table_schema;
//                    std::cout << table_schema.dimension_ << std::endl;
                }
                table_file.dimension_ = groups[table_file.table_id_].dimension_;

                GetTableFilePath(table_file);

                files.push_back(table_file);
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN FINDING TABLE FILES TO INDEX", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN FINDING TABLE FILES TO INDEX", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToSearch(const std::string &table_id,
                                     const DatesT &partition,
                                     DatePartionedTableFilesSchema &files) {
        files.clear();

        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            StoreQueryResult res;

            if (partition.empty()) {

                Query filesToSearchQuery = connectionPtr->query();
                filesToSearchQuery << "SELECT id, table_id, engine_type, file_id, file_type, size, date " <<
                                      "FROM metaFile " <<
                                      "WHERE table_id = " << quote << table_id << " AND " <<
                                      "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                                      "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                                      "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";
                res = filesToSearchQuery.store();

                auto end_time = METRICS_NOW_TIME;
                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
            }
            else {

                Query filesToSearchQuery = connectionPtr->query();

                std::stringstream partitionListSS;
                for (auto &date : partition) {
                    partitionListSS << std::to_string(date) << ", ";
                }
                std::string partitionListStr = partitionListSS.str();
                partitionListStr = partitionListStr.substr(0, partitionListStr.size() - 2); //remove the last ", "

                filesToSearchQuery << "SELECT id, table_id, engine_type, file_id, file_type, size, date " <<
                                   "FROM metaFile " <<
                                   "WHERE table_id = " << quote << table_id << " AND " <<
                                   "date IN (" << partitionListStr << ") AND " <<
                                   "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                                   "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                                   "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";
                res = filesToSearchQuery.store();

                auto end_time = METRICS_NOW_TIME;
                auto total_time = METRICS_MICROSECONDS(start_time, end_time);
                server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
            }

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);
            if (!status.ok()) {
                return status;
            }

            TableFileSchema table_file;
            for (auto& resRow : res) {

                table_file.id_ = resRow["id"]; //implicit conversion

                std::string table_id_str;
                resRow["table_id"].to_string(table_id_str);
                table_file.table_id_ = table_id_str;

                table_file.engine_type_ = resRow["engine_type"];

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                table_file.file_id_ = file_id;

                table_file.file_type_ = resRow["file_type"];

                table_file.size_ = resRow["size"];

                table_file.date_ = resRow["date"];

                table_file.dimension_ = table_schema.dimension_;

                GetTableFilePath(table_file);

                auto dateItr = files.find(table_file.date_);
                if (dateItr == files.end()) {
                    files[table_file.date_] = TableFilesSchema();
                }

                files[table_file.date_].push_back(table_file);
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN FINDING TABLE FILES TO SEARCH", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN FINDING TABLE FILES TO SEARCH", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToMerge(const std::string &table_id,
                                    DatePartionedTableFilesSchema &files) {
        files.clear();

        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query filesToMergeQuery = connectionPtr->query();
            filesToMergeQuery << "SELECT id, table_id, file_id, file_type, size, date " <<
                                 "FROM metaFile " <<
                                 "WHERE table_id = " << quote << table_id << " AND " <<
                                 "file_type = " << std::to_string(TableFileSchema::RAW) << ";";
            StoreQueryResult res = filesToMergeQuery.store();

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);

            if (!status.ok()) {
                return status;
            }

            TableFileSchema table_file;
            for (auto& resRow : res) {

                table_file.id_ = resRow["id"]; //implicit conversion

                std::string table_id_str;
                resRow["table_id"].to_string(table_id_str);
                table_file.table_id_ = table_id_str;

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                table_file.file_id_ = file_id;

                table_file.file_type_ = resRow["file_type"];

                table_file.size_ = resRow["size"];

                table_file.date_ = resRow["date"];

                table_file.dimension_ = table_schema.dimension_;

                GetTableFilePath(table_file);

                auto dateItr = files.find(table_file.date_);
                if (dateItr == files.end()) {
                    files[table_file.date_] = TableFilesSchema();
                }

                files[table_file.date_].push_back(table_file);
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN FINDING TABLE FILES TO MERGE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN FINDING TABLE FILES TO MERGE", er.what());
        }

        return Status::OK();
    }

    //ZR: TODO: this function is pending to be removed, so not gonna implemented for now
    Status MySQLMetaImpl::FilesToDelete(const std::string& table_id,
                                     const DatesT& partition,
                                     DatePartionedTableFilesSchema& files) {

        return Status::OK();
    }

    Status MySQLMetaImpl::GetTableFile(TableFileSchema &file_schema) {

        try {

            Query getTableFileQuery = connectionPtr->query();
            getTableFileQuery << "SELECT id, table_id, file_id, file_type, size, date " <<
                                 "FROM metaFile " <<
                                 "WHERE file_id = " << quote << file_schema.file_id_ << " AND " <<
                                 "table_id = " << quote << file_schema.table_id_ << ";";
            StoreQueryResult res = getTableFileQuery.store();

            assert(res && res.num_rows() <= 1);
            if (res.num_rows() == 1) {

                const Row& resRow = res[0];

                file_schema.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                file_schema.table_id_ = table_id;

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                file_schema.file_id_ = file_id;

                file_schema.file_type_ = resRow["file_type"];

                file_schema.size_ = resRow["size"];

                file_schema.date_ = resRow["date"];
            }
            else {
                return Status::NotFound("Table:" + file_schema.table_id_ +
                                        " File:" + file_schema.file_id_ + " not found");
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING TABLE FILE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN RETRIEVING TABLE FILE", er.what());
        }

        return Status::OK();
    }

// PXU TODO: Support Swap
    Status MySQLMetaImpl::Archive() {
        auto &criterias = options_.archive_conf.GetCriterias();
        if (criterias.empty()) {
            return Status::OK();
        }

        for (auto& kv : criterias) {
            auto &criteria = kv.first;
            auto &limit = kv.second;
            if (criteria == "days") {
                size_t usecs = limit * D_SEC * US_PS;
                long now = utils::GetMicroSecTimeStamp();
                try {

                    Query archiveQuery = connectionPtr->query();
                    archiveQuery << "UPDATE metaFile " <<
                                    "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                    "WHERE created_on < " << std::to_string(now - usecs) << " AND " <<
                                    "file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";
                    if (!archiveQuery.exec()) {
                        return Status::DBTransactionError("QUERY ERROR DURING ARCHIVE", archiveQuery.error());
                    }

                } catch (const BadQuery& er) {
                    // Handle any query errors
                    return Status::DBTransactionError("QUERY ERROR WHEN DURING ARCHIVE", er.what());
                } catch (const Exception& er) {
                    // Catch-all for any other MySQL++ exceptions
                    return Status::DBTransactionError("GENERAL ERROR WHEN DURING ARCHIVE", er.what());
                }
            }
            if (criteria == "disk") {
                uint64_t sum = 0;
                Size(sum);

                auto to_delete = (sum - limit * G);
                DiscardFiles(to_delete);
            }
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::Size(uint64_t &result) {
        result = 0;
        try {

            Query getSizeQuery = connectionPtr->query();
            getSizeQuery << "SELECT SUM(size) AS sum " <<
                            "FROM metaFile " <<
                            "WHERE file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";
            StoreQueryResult res = getSizeQuery.store();

            assert(res && res.num_rows() == 1);
            result = res[0]["sum"];

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING SIZE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN RETRIEVING SIZE", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DiscardFiles(long to_discard_size) {
        LOG(DEBUG) << "About to discard size=" << to_discard_size;
        if (to_discard_size <= 0) {
//            std::cout << "in" << std::endl;
            return Status::OK();
        }
        try {

            Query discardFilesQuery = connectionPtr->query();
            discardFilesQuery << "SELECT id, size " <<
                                 "FROM metaFile " <<
                                 "WHERE file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                 "ORDER BY id ASC " <<
                                 "LIMIT 10;";
//            std::cout << discardFilesQuery.str() << std::endl;
            StoreQueryResult res = discardFilesQuery.store();

            assert(res);
            if (res.num_rows() == 0) {
                return Status::OK();
            }

            TableFileSchema table_file;
            std::stringstream idsToDiscardSS;
            for (auto& resRow : res) {
                if (to_discard_size <= 0) {
                    break;
                }
                table_file.id_ = resRow["id"];
                table_file.size_ = resRow["size"];
                idsToDiscardSS << "id = " << std::to_string(table_file.id_) << " OR ";
                ENGINE_LOG_DEBUG << "Discard table_file.id=" << table_file.file_id_
                                 << " table_file.size=" << table_file.size_;
                to_discard_size -= table_file.size_;
            }

            std::string idsToDiscardStr = idsToDiscardSS.str();
            idsToDiscardStr = idsToDiscardStr.substr(0, idsToDiscardStr.size() - 4); //remove the last " OR "

            discardFilesQuery << "UPDATE metaFile " <<
                                 "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                 "WHERE " << idsToDiscardStr << ";";

            if (discardFilesQuery.exec()) {
                return DiscardFiles(to_discard_size);
            }
            else {
                return Status::DBTransactionError("QUERY ERROR WHEN DISCARDING FILES", discardFilesQuery.error());
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN DISCARDING FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN DISCARDING FILES", er.what());
        }
    }

    //ZR: this function assumes all fields in file_schema have value
    Status MySQLMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {
        file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query updateTableFileQuery = connectionPtr->query();

            std::string id = std::to_string(file_schema.id_);
            std::string table_id = file_schema.table_id_;
            std::string engine_type = std::to_string(file_schema.engine_type_);
            std::string file_id = file_schema.file_id_;
            std::string file_type = std::to_string(file_schema.file_type_);
            std::string size = std::to_string(file_schema.size_);
            std::string updated_time = std::to_string(file_schema.updated_time_);
            std::string created_on = std::to_string(file_schema.created_on_);
            std::string date = std::to_string(file_schema.date_);

            updateTableFileQuery << "UPDATE metaFile " <<
                                    "SET table_id = " << quote << table_id << ", " <<
                                    "engine_type = " << engine_type << ", " <<
                                    "file_id = " << quote << file_id << ", " <<
                                    "file_type = " << file_type << ", " <<
                                    "size = " << size << ", " <<
                                    "updated_time = " << updated_time << ", " <<
                                    "created_on = " << created_on << ", " <<
                                    "date = " << date << " " <<
                                    "WHERE id = " << id << ";";

//            std::cout << updateTableFileQuery.str() << std::endl;

            if (!updateTableFileQuery.exec()) {
                ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
                return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILE", updateTableFileQuery.error());
            }

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
            return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
            return Status::DBTransactionError("GENERAL ERROR WHEN UPDATING TABLE FILE", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::UpdateTableFiles(TableFilesSchema &files) {
        try {
            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query updateTableFilesQuery = connectionPtr->query();

            for (auto& file_schema : files) {

                std::string id = std::to_string(file_schema.id_);
                std::string table_id = file_schema.table_id_;
                std::string engine_type = std::to_string(file_schema.engine_type_);
                std::string file_id = file_schema.file_id_;
                std::string file_type = std::to_string(file_schema.file_type_);
                std::string size = std::to_string(file_schema.size_);
                std::string updated_time = std::to_string(file_schema.updated_time_);
                std::string created_on = std::to_string(file_schema.created_on_);
                std::string date = std::to_string(file_schema.date_);

                updateTableFilesQuery << "UPDATE metaFile " <<
                                         "SET table_id = " << quote << table_id << ", " <<
                                         "engine_type = " << engine_type << ", " <<
                                         "file_id = " << quote << file_id << ", " <<
                                         "file_type = " << file_type << ", " <<
                                         "size = " << size << ", " <<
                                         "updated_time = " << updated_time << ", " <<
                                         "created_on = " << created_on << ", " <<
                                         "date = " << date << " " <<
                                         "WHERE id = " << id << ";";

            }

            if (!updateTableFilesQuery.exec()) {
                return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILES", updateTableFilesQuery.error());
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN UPDATING TABLE FILES", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
        auto now = utils::GetMicroSecTimeStamp();
        try {

            Query cleanUpFilesWithTTLQuery = connectionPtr->query();
            cleanUpFilesWithTTLQuery << "SELECT id, table_id, file_id, file_type, size, date " <<
                                        "FROM metaFile " <<
                                        "WHERE file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " AND " <<
                                        "updated_time > " << std::to_string(now - seconds * US_PS) << ";";
            StoreQueryResult res = cleanUpFilesWithTTLQuery.store();

            assert(res);

            TableFileSchema table_file;
            std::vector<std::string> idsToDelete;

            for (auto& resRow : res) {

                table_file.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                table_file.table_id_ = table_id;

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                table_file.file_id_ = file_id;

                table_file.file_type_ = resRow["file_type"];

                table_file.size_ = resRow["size"];

                table_file.date_ = resRow["date"];

                GetTableFilePath(table_file);

                if (table_file.file_type_ == TableFileSchema::TO_DELETE) {
                    boost::filesystem::remove(table_file.location_);
                }

                idsToDelete.emplace_back(std::to_string(table_file.id_));
            }

            std::stringstream idsToDeleteSS;
            for (auto& id : idsToDelete) {
                idsToDeleteSS << "id = " << id << " OR ";
            }
            std::string idsToDeleteStr = idsToDeleteSS.str();
            idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4); //remove the last " OR "
            cleanUpFilesWithTTLQuery << "DELETE FROM metaFile WHERE " <<
                                        idsToDeleteStr << ";";
            if (!cleanUpFilesWithTTLQuery.exec()) {
                return Status::DBTransactionError("CleanUpFilesWithTTL Error", cleanUpFilesWithTTLQuery.error());
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN CLEANING UP FILES WITH TTL", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN CLEANING UP FILES WITH TTL", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::CleanUp() {
        try {
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

            Query cleanUpQuery = connectionPtr->query();
            cleanUpQuery << "SELECT id, table_id, file_id, file_type, size, date " <<
                               "FROM metaFile " <<
                               "WHERE file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " OR " <<
                               "file_type = " << std::to_string(TableFileSchema::NEW) << ";";
            StoreQueryResult res = cleanUpQuery.store();

            assert(res);

            TableFileSchema table_file;
            std::vector<std::string> idsToDelete;

            for (auto& resRow : res) {

                table_file.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                table_file.table_id_ = table_id;

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                table_file.file_id_ = file_id;

                table_file.file_type_ = resRow["file_type"];

                table_file.size_ = resRow["size"];

                table_file.date_ = resRow["date"];

                GetTableFilePath(table_file);

                if (table_file.file_type_ == TableFileSchema::TO_DELETE) {
                    boost::filesystem::remove(table_file.location_);
                }

                idsToDelete.emplace_back(std::to_string(table_file.id_));
            }

            std::stringstream idsToDeleteSS;
            for (auto& id : idsToDelete) {
                idsToDeleteSS << "id = " << id << " OR ";
            }
            std::string idsToDeleteStr = idsToDeleteSS.str();
            idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4); //remove the last " OR "
            cleanUpQuery << "DELETE FROM metaFile WHERE " <<
                                     idsToDeleteStr << ";";
            if (!cleanUpQuery.exec()) {
                return Status::DBTransactionError("Clean up Error", cleanUpQuery.error());
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN CLEANING UP FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN CLEANING UP FILES", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::Count(const std::string &table_id, uint64_t &result) {

        try {

            server::Metrics::GetInstance().MetaAccessTotalIncrement();
            auto start_time = METRICS_NOW_TIME;

            Query countQuery = connectionPtr->query();
            countQuery << "SELECT size " <<
                          "FROM metaFile " <<
                          "WHERE table_id = " << quote << table_id << " AND " <<
                          "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                          "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                          "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";
            StoreQueryResult res = countQuery.store();

            auto end_time = METRICS_NOW_TIME;
            auto total_time = METRICS_MICROSECONDS(start_time, end_time);
            server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);

            if (!status.ok()) {
                return status;
            }

            result = 0;
            for (auto &resRow : res) {
                size_t size = resRow["size"];
                result += size;
            }

            assert(table_schema.dimension_ != 0);
            result /= table_schema.dimension_;
            result /= sizeof(float);

        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING COUNT", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN RETRIEVING COUNT", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::DropAll() {
        if (boost::filesystem::is_directory(options_.path)) {
            boost::filesystem::remove_all(options_.path);
        }
        try {
            Query dropTableQuery = connectionPtr->query();
            dropTableQuery << "DROP TABLE IF EXISTS meta, metaFile;";
            if (dropTableQuery.exec()) {
                return Status::OK();
            }
            else {
                return Status::DBTransactionError("DROP TABLE ERROR", dropTableQuery.error());
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            return Status::DBTransactionError("QUERY ERROR WHEN DROPPING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            return Status::DBTransactionError("GENERAL ERROR WHEN DROPPING TABLE", er.what());
        }
    }

    MySQLMetaImpl::~MySQLMetaImpl() {
        CleanUp();
    }

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
