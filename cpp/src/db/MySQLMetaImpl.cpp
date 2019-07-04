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
#include <mutex>
#include <thread>

#include "mysql++/mysql++.h"

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

    using namespace mysqlpp;

//    static std::unique_ptr<Connection> connectionPtr(new Connection());
//    std::recursive_mutex mysql_mutex;
//
//    std::unique_ptr<Connection>& MySQLMetaImpl::getConnectionPtr() {
////        static std::recursive_mutex connectionMutex_;
//        std::lock_guard<std::recursive_mutex> lock(connectionMutex_);
//        return connectionPtr;
//    }

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

    MySQLMetaImpl::MySQLMetaImpl(const DBMetaOptions &options_, const int& mode)
            : options_(options_),
              mode_(mode) {
        Initialize();
    }

    Status MySQLMetaImpl::Initialize() {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        if (!boost::filesystem::is_directory(options_.path)) {
            auto ret = boost::filesystem::create_directory(options_.path);
            if (!ret) {
                ENGINE_LOG_ERROR << "Failed to create db directory " << options_.path;
                return Status::DBTransactionError("Failed to create db directory", options_.path);
            }
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
//            connectionPtr->set_option(new mysqlpp::ReconnectOption(true));
            int threadHint = std::thread::hardware_concurrency();
            int maxPoolSize = threadHint == 0 ? 8 : threadHint;
            mysql_connection_pool_ = std::make_shared<MySQLConnectionPool>(dbName, username, password, serverAddress, port, maxPoolSize);
//            std::cout << "MySQL++ thread aware:" << std::to_string(connectionPtr->thread_aware()) << std::endl;
            ENGINE_LOG_DEBUG << "MySQL connection pool: maximum pool size = " << std::to_string(maxPoolSize);
            try {

                if (mode_ != Options::MODE::READ_ONLY) {
                    CleanUp();
                }

                {
                    ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                    if (connectionPtr == nullptr) {
                        return Status::Error("Failed to connect to database server");
                    }

//                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::Initialize: connections in use = " << mysql_connection_pool_->getConnectionsInUse();
//                if (!connectionPtr->connect(dbName, serverAddress, username, password, port)) {
//                    return Status::Error("DB connection failed: ", connectionPtr->error());
//                }
                    if (!connectionPtr->thread_aware()) {
                        ENGINE_LOG_ERROR << "MySQL++ wasn't built with thread awareness! Can't run without it.";
                        return Status::Error("MySQL++ wasn't built with thread awareness! Can't run without it.");
                    }
                    Query InitializeQuery = connectionPtr->query();

//                InitializeQuery << "SET max_allowed_packet=67108864;";
//                if (!InitializeQuery.exec()) {
//                    return Status::DBTransactionError("Initialization Error", InitializeQuery.error());
//                }

//                InitializeQuery << "DROP TABLE IF EXISTS Tables, TableFiles;";
                    InitializeQuery << "CREATE TABLE IF NOT EXISTS Tables (" <<
                                    "id BIGINT PRIMARY KEY AUTO_INCREMENT, " <<
                                    "table_id VARCHAR(255) UNIQUE NOT NULL, " <<
                                    "state INT NOT NULL, " <<
                                    "dimension SMALLINT NOT NULL, " <<
                                    "created_on BIGINT NOT NULL, " <<
                                    "files_cnt BIGINT DEFAULT 0 NOT NULL, " <<
                                    "engine_type INT DEFAULT 1 NOT NULL, " <<
                                    "store_raw_data BOOL DEFAULT false NOT NULL);";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::Initialize: " << InitializeQuery.str();

                    if (!InitializeQuery.exec()) {
                        return Status::DBTransactionError("Initialization Error", InitializeQuery.error());
                    }

                    InitializeQuery << "CREATE TABLE IF NOT EXISTS TableFiles (" <<
                                    "id BIGINT PRIMARY KEY AUTO_INCREMENT, " <<
                                    "table_id VARCHAR(255) NOT NULL, " <<
                                    "engine_type INT DEFAULT 1 NOT NULL, " <<
                                    "file_id VARCHAR(255) NOT NULL, " <<
                                    "file_type INT DEFAULT 0 NOT NULL, " <<
                                    "size BIGINT DEFAULT 0 NOT NULL, " <<
                                    "updated_time BIGINT NOT NULL, " <<
                                    "created_on BIGINT NOT NULL, " <<
                                    "date INT DEFAULT -1 NOT NULL);";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::Initialize: " << InitializeQuery.str();

                    if (!InitializeQuery.exec()) {
                        return Status::DBTransactionError("Initialization Error", InitializeQuery.error());
                    }
                } //Scoped Connection

//                //Consume all results to avoid "Commands out of sync" error
//                while (InitializeQuery.more_results()) {
//                    InitializeQuery.store_next();
//                }
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
            } catch (const BadQuery& er) {
                // Handle any query errors
                ENGINE_LOG_ERROR << "QUERY ERROR DURING INITIALIZATION" << ": " << er.what();
                return Status::DBTransactionError("QUERY ERROR DURING INITIALIZATION", er.what());
            } catch (const Exception& er) {
                // Catch-all for any other MySQL++ exceptions
                ENGINE_LOG_ERROR << "GENERAL ERROR DURING INITIALIZATION" << ": " << er.what();
                return Status::DBTransactionError("GENERAL ERROR DURING INITIALIZATION", er.what());
            } catch (std::exception &e) {
                return HandleException("Encounter exception during initialization", e);
            }
        }
        else {
            ENGINE_LOG_ERROR << "Wrong URI format. URI = " << uri;
            return Status::Error("Wrong URI format");
        }
    }

// PXU TODO: Temp solution. Will fix later
    Status MySQLMetaImpl::DropPartitionsByDates(const std::string &table_id,
                                             const DatesT &dates) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

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

            auto yesterday = GetDateWithDelta(-1);

            for (auto &date : dates) {
                if (date >= yesterday) {
                    return Status::Error("Could not delete partitions within 2 days");
                }
            }

            std::stringstream dateListSS;
            for (auto &date : dates) {
                dateListSS << std::to_string(date) << ", ";
            }
            std::string dateListStr = dateListSS.str();
            dateListStr = dateListStr.substr(0, dateListStr.size() - 2); //remove the last ", "

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::DropPartitionsByDates connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query dropPartitionsByDatesQuery = connectionPtr->query();

                dropPartitionsByDatesQuery << "UPDATE TableFiles " <<
                                            "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                            "WHERE table_id = " << quote << table_id << " AND " <<
                                            "date in (" << dateListStr << ");";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropPartitionsByDates: " << dropPartitionsByDatesQuery.str();

                if (!dropPartitionsByDatesQuery.exec()) {
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN DROPPING PARTITIONS BY DATES";
                    return Status::DBTransactionError("QUERY ERROR WHEN DROPPING PARTITIONS BY DATES",
                                                      dropPartitionsByDatesQuery.error());
                }
            } //Scoped Connection
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN DROPPING PARTITIONS BY DATES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DROPPING PARTITIONS BY DATES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DROPPING PARTITIONS BY DATES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DROPPING PARTITIONS BY DATES", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::CreateTable(TableSchema &table_schema) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

//        server::Metrics::GetInstance().MetaAccessTotalIncrement();
        try {

            MetricCollector metric;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::CreateTable connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query createTableQuery = connectionPtr->query();
//                ENGINE_LOG_DEBUG << "Create Table in";
                if (table_schema.table_id_.empty()) {
                    NextTableId(table_schema.table_id_);
                } else {
                    createTableQuery << "SELECT state FROM Tables " <<
                                        "WHERE table_id = " << quote << table_schema.table_id_ << ";";
//                    ENGINE_LOG_DEBUG << "Create Table : " << createTableQuery.str();

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::CreateTable: " << createTableQuery.str();

                    StoreQueryResult res = createTableQuery.store();

                    if (res.num_rows() == 1) {
                        int state = res[0]["state"];
                        if (TableSchema::TO_DELETE == state) {
                            return Status::Error("Table already exists and it is in delete state, please wait a second");
                        }
                        else {
                            return Status::OK();//table already exists, no error
                        }
                    }
                }
//                ENGINE_LOG_DEBUG << "Create Table start";

                table_schema.files_cnt_ = 0;
                table_schema.id_ = -1;
                table_schema.created_on_ = utils::GetMicroSecTimeStamp();

//            auto start_time = METRICS_NOW_TIME;

                std::string id = "NULL"; //auto-increment
                std::string table_id = table_schema.table_id_;
                std::string state = std::to_string(table_schema.state_);
                std::string dimension = std::to_string(table_schema.dimension_);
                std::string created_on = std::to_string(table_schema.created_on_);
                std::string files_cnt = "0";
                std::string engine_type = std::to_string(table_schema.engine_type_);
                std::string store_raw_data = table_schema.store_raw_data_ ? "true" : "false";

                createTableQuery << "INSERT INTO Tables VALUES" <<
                                 "(" << id << ", " << quote << table_id << ", " << state << ", " << dimension << ", " <<
                                 created_on << ", " << files_cnt << ", " << engine_type << ", " << store_raw_data << ");";
//                ENGINE_LOG_DEBUG << "Create Table : " << createTableQuery.str();

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CreateTable: " << createTableQuery.str();

                if (SimpleResult res = createTableQuery.execute()) {
                    table_schema.id_ = res.insert_id(); //Might need to use SELECT LAST_INSERT_ID()?
//                    std::cout << table_schema.id_ << std::endl;
                    //Consume all results to avoid "Commands out of sync" error
//                while (createTableQuery.more_results()) {
//                    createTableQuery.store_next();
//                }
                } else {
                    ENGINE_LOG_ERROR << "Add Table Error";
                    return Status::DBTransactionError("Add Table Error", createTableQuery.error());
                }
            } //Scoped Connection

//        auto end_time = METRICS_NOW_TIME;
//        auto total_time = METRICS_MICROSECONDS(start_time, end_time);
//        server::Metrics::GetInstance().MetaAccessDurationSecondsHistogramObserve(total_time);

            auto table_path = GetTablePath(table_schema.table_id_);
            table_schema.location_ = table_path;
            if (!boost::filesystem::is_directory(table_path)) {
                auto ret = boost::filesystem::create_directories(table_path);
                if (!ret) {
                    ENGINE_LOG_ERROR << "Create directory " << table_path << " Error";
                    return Status::Error("Failed to create table path");
                }
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN ADDING TABLE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN ADDING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN ADDING TABLE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN ADDING TABLE", er.what());
        } catch (std::exception &e) {
            return HandleException("Encounter exception when create table", e);
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DeleteTable(const std::string& table_id) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {

            MetricCollector metric;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::DeleteTable connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                //soft delete table
                Query deleteTableQuery = connectionPtr->query();
//
                deleteTableQuery << "UPDATE Tables " <<
                                    "SET state = " << std::to_string(TableSchema::TO_DELETE) << " " <<
                                    "WHERE table_id = " << quote << table_id << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::DeleteTable: " << deleteTableQuery.str();

                if (!deleteTableQuery.exec()) {
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN DELETING TABLE";
                    return Status::DBTransactionError("QUERY ERROR WHEN DELETING TABLE", deleteTableQuery.error());
                }

            } //Scoped Connection


            if (mode_ == Options::MODE::CLUSTER) {
                DeleteTableFiles(table_id);
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DELETING TABLE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DELETING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DELETING TABLE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DELETING TABLE", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DeleteTableFiles(const std::string& table_id) {
        try {
            MetricCollector metric;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::DeleteTableFiles connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                //soft delete table files
                Query deleteTableFilesQuery = connectionPtr->query();
                //
                deleteTableFilesQuery << "UPDATE TableFiles " <<
                                      "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << ", " <<
                                      "updated_time = " << std::to_string(utils::GetMicroSecTimeStamp()) << " " <<
                                      "WHERE table_id = " << quote << table_id << " AND " <<
                                      "file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::DeleteTableFiles: " << deleteTableFilesQuery.str();

                if (!deleteTableFilesQuery.exec()) {
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN DELETING TABLE FILES";
                    return Status::DBTransactionError("QUERY ERROR WHEN DELETING TABLE", deleteTableFilesQuery.error());
                }
            } //Scoped Connection
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN DELETING TABLE FILES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DELETING TABLE FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DELETING TABLE FILES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DELETING TABLE FILES", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DescribeTable(TableSchema &table_schema) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {

            MetricCollector metric;

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::DescribeTable connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query describeTableQuery = connectionPtr->query();
                describeTableQuery << "SELECT id, dimension, files_cnt, engine_type, store_raw_data " <<
                                      "FROM Tables " <<
                                      "WHERE table_id = " << quote << table_schema.table_id_ << " " <<
                                      "AND state <> " << std::to_string(TableSchema::TO_DELETE) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::DescribeTable: " << describeTableQuery.str();

                res = describeTableQuery.store();
            } //Scoped Connection

            if (res.num_rows() == 1) {
                const Row& resRow = res[0];

                table_schema.id_ = resRow["id"]; //implicit conversion

                table_schema.dimension_ = resRow["dimension"];

                table_schema.files_cnt_ = resRow["files_cnt"];

                table_schema.engine_type_ = resRow["engine_type"];

                int store_raw_data = resRow["store_raw_data"];
                table_schema.store_raw_data_ = (store_raw_data == 1);
            }
            else {
                return Status::NotFound("Table " + table_schema.table_id_ + " not found");
            }

            auto table_path = GetTablePath(table_schema.table_id_);
            table_schema.location_ = table_path;

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN DESCRIBING TABLE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DESCRIBING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DESCRIBING TABLE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DESCRIBING TABLE", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {

            MetricCollector metric;

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::HasTable connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query hasTableQuery = connectionPtr->query();
                //since table_id is a unique column we just need to check whether it exists or not
                hasTableQuery << "SELECT EXISTS " <<
                              "(SELECT 1 FROM Tables " <<
                              "WHERE table_id = " << quote << table_id << " " <<
                              "AND state <> " << std::to_string(TableSchema::TO_DELETE) << ") " <<
                              "AS " << quote << "check" << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::HasTable: " << hasTableQuery.str();

                res = hasTableQuery.store();
            } //Scoped Connection

            int check = res[0]["check"];
            has_or_not = (check == 1);

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN CHECKING IF TABLE EXISTS" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN CHECKING IF TABLE EXISTS", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN CHECKING IF TABLE EXISTS" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN CHECKING IF TABLE EXISTS", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::AllTables(std::vector<TableSchema>& table_schema_array) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {

            MetricCollector metric;

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::AllTables connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query allTablesQuery = connectionPtr->query();
                allTablesQuery << "SELECT id, table_id, dimension, files_cnt, engine_type, store_raw_data " <<
                               "FROM Tables " <<
                               "WHERE state <> " << std::to_string(TableSchema::TO_DELETE) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::AllTables: " << allTablesQuery.str();

                res = allTablesQuery.store();
            } //Scoped Connection

            for (auto& resRow : res) {
                TableSchema table_schema;

                table_schema.id_ = resRow["id"]; //implicit conversion

                std::string table_id;
                resRow["table_id"].to_string(table_id);
                table_schema.table_id_ = table_id;

                table_schema.dimension_ = resRow["dimension"];

                table_schema.files_cnt_ = resRow["files_cnt"];

                table_schema.engine_type_ = resRow["engine_type"];

                int store_raw_data = resRow["store_raw_data"];
                table_schema.store_raw_data_ = (store_raw_data == 1);

                table_schema_array.emplace_back(table_schema);
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN DESCRIBING ALL TABLES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DESCRIBING ALL TABLES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DESCRIBING ALL TABLES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DESCRIBING ALL TABLES", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::CreateTableFile(TableFileSchema &file_schema) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

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

            std::string id = "NULL"; //auto-increment
            std::string table_id = file_schema.table_id_;
            std::string engine_type = std::to_string(file_schema.engine_type_);
            std::string file_id = file_schema.file_id_;
            std::string file_type = std::to_string(file_schema.file_type_);
            std::string size = std::to_string(file_schema.size_);
            std::string updated_time = std::to_string(file_schema.updated_time_);
            std::string created_on = std::to_string(file_schema.created_on_);
            std::string date = std::to_string(file_schema.date_);

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::CreateTableFile connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query createTableFileQuery = connectionPtr->query();

                createTableFileQuery << "INSERT INTO TableFiles VALUES" <<
                                     "(" << id << ", " << quote << table_id << ", " << engine_type << ", " <<
                                     quote << file_id << ", " << file_type << ", " << size << ", " <<
                                     updated_time << ", " << created_on << ", " << date << ");";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CreateTableFile: " << createTableFileQuery.str();

                if (SimpleResult res = createTableFileQuery.execute()) {
                    file_schema.id_ = res.insert_id(); //Might need to use SELECT LAST_INSERT_ID()?

                    //Consume all results to avoid "Commands out of sync" error
//                while (createTableFileQuery.more_results()) {
//                    createTableFileQuery.store_next();
//                }
                } else {
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN ADDING TABLE FILE";
                    return Status::DBTransactionError("Add file Error", createTableFileQuery.error());
                }
            } // Scoped Connection

            auto partition_path = GetTableDatePartitionPath(file_schema.table_id_, file_schema.date_);

            if (!boost::filesystem::is_directory(partition_path)) {
                auto ret = boost::filesystem::create_directory(partition_path);
                if (!ret) {
                    ENGINE_LOG_ERROR << "Create directory " << partition_path << " Error";
                    return Status::DBTransactionError("Failed to create partition directory");
                }
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN ADDING TABLE FILE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN ADDING TABLE FILE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN ADDING TABLE FILE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN ADDING TABLE FILE", er.what());
        } catch (std::exception& ex) {
            return HandleException("Encounter exception when create table file", ex);
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToIndex(TableFilesSchema &files) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        files.clear();

        try {

            MetricCollector metric;

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::FilesToIndex connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query filesToIndexQuery = connectionPtr->query();
                filesToIndexQuery << "SELECT id, table_id, engine_type, file_id, file_type, size, date " <<
                                     "FROM TableFiles " <<
                                     "WHERE file_type = " << std::to_string(TableFileSchema::TO_INDEX) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToIndex: " << filesToIndexQuery.str();

                res = filesToIndexQuery.store();
            } //Scoped Connection

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
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN FINDING TABLE FILES TO INDEX" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN FINDING TABLE FILES TO INDEX", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN FINDING TABLE FILES TO INDEX" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN FINDING TABLE FILES TO INDEX", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToSearch(const std::string &table_id,
                                     const DatesT &partition,
                                     DatePartionedTableFilesSchema &files) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        files.clear();

        try {

            MetricCollector metric;

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }
//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::FilesToSearch connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                if (partition.empty()) {

                    Query filesToSearchQuery = connectionPtr->query();
                    filesToSearchQuery << "SELECT id, table_id, engine_type, file_id, file_type, size, date " <<
                                       "FROM TableFiles " <<
                                       "WHERE table_id = " << quote << table_id << " AND " <<
                                       "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                                       "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                                       "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToSearch: " << filesToSearchQuery.str();

                    res = filesToSearchQuery.store();

                } else {

                    Query filesToSearchQuery = connectionPtr->query();

                    std::stringstream partitionListSS;
                    for (auto &date : partition) {
                        partitionListSS << std::to_string(date) << ", ";
                    }
                    std::string partitionListStr = partitionListSS.str();
                    partitionListStr = partitionListStr.substr(0, partitionListStr.size() - 2); //remove the last ", "

                    filesToSearchQuery << "SELECT id, table_id, engine_type, file_id, file_type, size, date " <<
                                       "FROM TableFiles " <<
                                       "WHERE table_id = " << quote << table_id << " AND " <<
                                       "date IN (" << partitionListStr << ") AND " <<
                                       "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                                       "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                                       "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToSearch: " << filesToSearchQuery.str();

                    res = filesToSearchQuery.store();

                }
            } //Scoped Connection

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
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN FINDING TABLE FILES TO SEARCH" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN FINDING TABLE FILES TO SEARCH", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN FINDING TABLE FILES TO SEARCH" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN FINDING TABLE FILES TO SEARCH", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::FilesToMerge(const std::string &table_id,
                                    DatePartionedTableFilesSchema &files) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        files.clear();

        try {
            MetricCollector metric;

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::FilesToMerge connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query filesToMergeQuery = connectionPtr->query();
                filesToMergeQuery << "SELECT id, table_id, file_id, file_type, size, date " <<
                                  "FROM TableFiles " <<
                                  "WHERE table_id = " << quote << table_id << " AND " <<
                                  "file_type = " << std::to_string(TableFileSchema::RAW) << " " <<
                                  "ORDER BY size DESC" << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToMerge: " << filesToMergeQuery.str();

                res = filesToMergeQuery.store();
            } //Scoped Connection

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
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN FINDING TABLE FILES TO MERGE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN FINDING TABLE FILES TO MERGE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN FINDING TABLE FILES TO MERGE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN FINDING TABLE FILES TO MERGE", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::GetTableFiles(const std::string& table_id,
                                        const std::vector<size_t>& ids,
                                        TableFilesSchema& table_files) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        if (ids.empty()) {
            return Status::OK();
        }

        std::stringstream idSS;
        for (auto& id : ids) {
            idSS << "id = " << std::to_string(id) << " OR ";
        }
        std::string idStr = idSS.str();
        idStr = idStr.substr(0, idStr.size() - 4); //remove the last " OR "

        try {

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::GetTableFiles connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query getTableFileQuery = connectionPtr->query();
                getTableFileQuery << "SELECT id, engine_type, file_id, file_type, size, date " <<
                                      "FROM TableFiles " <<
                                      "WHERE table_id = " << quote << table_id << " AND " <<
                                      "(" << idStr << ");";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::GetTableFiles: " << getTableFileQuery.str();

                res = getTableFileQuery.store();
            } //Scoped Connection

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);
            if (!status.ok()) {
                return status;
            }

            for (auto& resRow : res) {

                TableFileSchema file_schema;

                file_schema.id_ = resRow["id"];

                file_schema.table_id_ = table_id;

                file_schema.engine_type_ = resRow["engine_type"];

                std::string file_id;
                resRow["file_id"].to_string(file_id);
                file_schema.file_id_ = file_id;

                file_schema.file_type_ = resRow["file_type"];

                file_schema.size_ = resRow["size"];

                file_schema.date_ = resRow["date"];

                file_schema.dimension_ = table_schema.dimension_;

                GetTableFilePath(file_schema);

                table_files.emplace_back(file_schema);
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN RETRIEVING TABLE FILES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING TABLE FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN RETRIEVING TABLE FILES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN RETRIEVING TABLE FILES", er.what());
        }

        return Status::OK();
    }

// PXU TODO: Support Swap
    Status MySQLMetaImpl::Archive() {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

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

                    ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                    if (connectionPtr == nullptr) {
                        return Status::Error("Failed to connect to database server");
                    }

//                    if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                        ENGINE_LOG_WARNING << "MySQLMetaImpl::Archive connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                    }

                    Query archiveQuery = connectionPtr->query();
                    archiveQuery << "UPDATE TableFiles " <<
                                    "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                    "WHERE created_on < " << std::to_string(now - usecs) << " AND " <<
                                    "file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::Archive: " << archiveQuery.str();

                    if (!archiveQuery.exec()) {
                        return Status::DBTransactionError("QUERY ERROR DURING ARCHIVE", archiveQuery.error());
                    }

                } catch (const BadQuery& er) {
                    // Handle any query errors
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN DURING ARCHIVE" << ": " << er.what();
                    return Status::DBTransactionError("QUERY ERROR WHEN DURING ARCHIVE", er.what());
                } catch (const Exception& er) {
                    // Catch-all for any other MySQL++ exceptions
                    ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DURING ARCHIVE" << ": " << er.what();
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

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        result = 0;
        try {

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::Size connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query getSizeQuery = connectionPtr->query();
                getSizeQuery << "SELECT IFNULL(SUM(size),0) AS sum " <<
                             "FROM TableFiles " <<
                             "WHERE file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::Size: " << getSizeQuery.str();

                res = getSizeQuery.store();
            } //Scoped Connection

//            if (!res) {
////                std::cout << "result is NULL" << std::endl;
//                return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING SIZE", getSizeQuery.error());
//            }
            if (res.empty()) {
                result = 0;
//                std::cout << "result = 0" << std::endl;
            }
            else {
                result = res[0]["sum"];
//                std::cout << "result = " << std::to_string(result) << std::endl;
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN RETRIEVING SIZE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING SIZE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN RETRIEVING SIZE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN RETRIEVING SIZE", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::DiscardFiles(long long to_discard_size) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        if (to_discard_size <= 0) {
//            std::cout << "in" << std::endl;
            return Status::OK();
        }
        ENGINE_LOG_DEBUG << "About to discard size=" << to_discard_size;

        try {

            MetricCollector metric;

            bool status;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::DiscardFiles connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query discardFilesQuery = connectionPtr->query();
                discardFilesQuery << "SELECT id, size " <<
                                  "FROM TableFiles " <<
                                  "WHERE file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                                  "ORDER BY id ASC " <<
                                  "LIMIT 10;";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::DiscardFiles: " << discardFilesQuery.str();

                //            std::cout << discardFilesQuery.str() << std::endl;
                StoreQueryResult res = discardFilesQuery.store();

                if (res.num_rows() == 0) {
                    return Status::OK();
                }

                TableFileSchema table_file;
                std::stringstream idsToDiscardSS;
                for (auto &resRow : res) {
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

                discardFilesQuery << "UPDATE TableFiles " <<
                                  "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << ", " <<
                                  "updated_time = " << std::to_string(utils::GetMicroSecTimeStamp()) << " " <<
                                  "WHERE " << idsToDiscardStr << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::DiscardFiles: " << discardFilesQuery.str();

                status = discardFilesQuery.exec();
                if (!status) {
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN DISCARDING FILES";
                    return Status::DBTransactionError("QUERY ERROR WHEN DISCARDING FILES", discardFilesQuery.error());
                }
            } //Scoped Connection

            return DiscardFiles(to_discard_size);

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN DISCARDING FILES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DISCARDING FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DISCARDING FILES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DISCARDING FILES", er.what());
        }
    }

    //ZR: this function assumes all fields in file_schema have value
    Status MySQLMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
        try {

            MetricCollector metric;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::UpdateTableFile connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query updateTableFileQuery = connectionPtr->query();

                //if the table has been deleted, just mark the table file as TO_DELETE
                //clean thread will delete the file later
                updateTableFileQuery << "SELECT state FROM Tables " <<
                                     "WHERE table_id = " << quote << file_schema.table_id_ << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFile: " << updateTableFileQuery.str();

                StoreQueryResult res = updateTableFileQuery.store();

                if (res.num_rows() == 1) {
                    int state = res[0]["state"];
                    if (state == TableSchema::TO_DELETE) {
                        file_schema.file_type_ = TableFileSchema::TO_DELETE;
                    }
                } else {
                    file_schema.file_type_ = TableFileSchema::TO_DELETE;
                }

                std::string id = std::to_string(file_schema.id_);
                std::string table_id = file_schema.table_id_;
                std::string engine_type = std::to_string(file_schema.engine_type_);
                std::string file_id = file_schema.file_id_;
                std::string file_type = std::to_string(file_schema.file_type_);
                std::string size = std::to_string(file_schema.size_);
                std::string updated_time = std::to_string(file_schema.updated_time_);
                std::string created_on = std::to_string(file_schema.created_on_);
                std::string date = std::to_string(file_schema.date_);

                updateTableFileQuery << "UPDATE TableFiles " <<
                                     "SET table_id = " << quote << table_id << ", " <<
                                     "engine_type = " << engine_type << ", " <<
                                     "file_id = " << quote << file_id << ", " <<
                                     "file_type = " << file_type << ", " <<
                                     "size = " << size << ", " <<
                                     "updated_time = " << updated_time << ", " <<
                                     "created_on = " << created_on << ", " <<
                                     "date = " << date << " " <<
                                     "WHERE id = " << id << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFile: " << updateTableFileQuery.str();

                //            std::cout << updateTableFileQuery.str() << std::endl;

                if (!updateTableFileQuery.exec()) {
                    ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN UPDATING TABLE FILE";
                    return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILE",
                                                      updateTableFileQuery.error());
                }
            } //Scoped Connection

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN UPDATING TABLE FILE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN UPDATING TABLE FILE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN UPDATING TABLE FILE", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::UpdateTableFiles(TableFilesSchema &files) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {
            MetricCollector metric;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::UpdateTableFiles connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query updateTableFilesQuery = connectionPtr->query();

                std::map<std::string, bool> has_tables;
                for (auto &file_schema : files) {

                    if (has_tables.find(file_schema.table_id_) != has_tables.end()) {
                        continue;
                    }

                    updateTableFilesQuery << "SELECT EXISTS " <<
                                          "(SELECT 1 FROM Tables " <<
                                          "WHERE table_id = " << quote << file_schema.table_id_ << " " <<
                                          "AND state <> " << std::to_string(TableSchema::TO_DELETE) << ") " <<
                                          "AS " << quote << "check" << ";";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFiles: " << updateTableFilesQuery.str();

                    StoreQueryResult res = updateTableFilesQuery.store();

                    int check = res[0]["check"];
                    has_tables[file_schema.table_id_] = (check == 1);
                }

                for (auto &file_schema : files) {

                    if (!has_tables[file_schema.table_id_]) {
                        file_schema.file_type_ = TableFileSchema::TO_DELETE;
                    }
                    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();

                    std::string id = std::to_string(file_schema.id_);
                    std::string table_id = file_schema.table_id_;
                    std::string engine_type = std::to_string(file_schema.engine_type_);
                    std::string file_id = file_schema.file_id_;
                    std::string file_type = std::to_string(file_schema.file_type_);
                    std::string size = std::to_string(file_schema.size_);
                    std::string updated_time = std::to_string(file_schema.updated_time_);
                    std::string created_on = std::to_string(file_schema.created_on_);
                    std::string date = std::to_string(file_schema.date_);

                    updateTableFilesQuery << "UPDATE TableFiles " <<
                                          "SET table_id = " << quote << table_id << ", " <<
                                          "engine_type = " << engine_type << ", " <<
                                          "file_id = " << quote << file_id << ", " <<
                                          "file_type = " << file_type << ", " <<
                                          "size = " << size << ", " <<
                                          "updated_time = " << updated_time << ", " <<
                                          "created_on = " << created_on << ", " <<
                                          "date = " << date << " " <<
                                          "WHERE id = " << id << ";";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFiles: " << updateTableFilesQuery.str();

                    if (!updateTableFilesQuery.exec()) {
                        ENGINE_LOG_ERROR << "QUERY ERROR WHEN UPDATING TABLE FILES";
                        return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILES",
                                                          updateTableFilesQuery.error());
                    }
                }
            } //Scoped Connection

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN UPDATING TABLE FILES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN UPDATING TABLE FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN UPDATING TABLE FILES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN UPDATING TABLE FILES", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
//        static int b_count = 0;
//        b_count++;
//        std::cout << "CleanUpFilesWithTTL: " << b_count << std::endl;
//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        auto now = utils::GetMicroSecTimeStamp();
        try {
            MetricCollector metric;

            {

//                ENGINE_LOG_WARNING << "MySQLMetaImpl::CleanUpFilesWithTTL: clean table files: connection in use before creating ScopedConnection = "
//                << mysql_connection_pool_->getConnectionsInUse();

                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::CleanUpFilesWithTTL: clean table files: connection in use after creating ScopedConnection = "
//                    << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query cleanUpFilesWithTTLQuery = connectionPtr->query();
                cleanUpFilesWithTTLQuery << "SELECT id, table_id, file_id, date " <<
                                         "FROM TableFiles " <<
                                         "WHERE file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " AND " <<
                                         "updated_time < " << std::to_string(now - seconds * US_PS) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                StoreQueryResult res = cleanUpFilesWithTTLQuery.store();

                TableFileSchema table_file;
                std::vector<std::string> idsToDelete;

                for (auto &resRow : res) {

                    table_file.id_ = resRow["id"]; //implicit conversion

                    std::string table_id;
                    resRow["table_id"].to_string(table_id);
                    table_file.table_id_ = table_id;

                    std::string file_id;
                    resRow["file_id"].to_string(file_id);
                    table_file.file_id_ = file_id;

                    table_file.date_ = resRow["date"];

                    GetTableFilePath(table_file);

                    ENGINE_LOG_DEBUG << "Removing deleted id =" << table_file.id_ << " location = "
                                     << table_file.location_ << std::endl;
                    boost::filesystem::remove(table_file.location_);

                    idsToDelete.emplace_back(std::to_string(table_file.id_));
                }

                if (!idsToDelete.empty()) {

                    std::stringstream idsToDeleteSS;
                    for (auto &id : idsToDelete) {
                        idsToDeleteSS << "id = " << id << " OR ";
                    }

                    std::string idsToDeleteStr = idsToDeleteSS.str();
                    idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4); //remove the last " OR "
                    cleanUpFilesWithTTLQuery << "DELETE FROM TableFiles WHERE " <<
                                             idsToDeleteStr << ";";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                    if (!cleanUpFilesWithTTLQuery.exec()) {
                        ENGINE_LOG_ERROR << "QUERY ERROR WHEN CLEANING UP FILES WITH TTL";
                        return Status::DBTransactionError("CleanUpFilesWithTTL Error",
                                                          cleanUpFilesWithTTLQuery.error());
                    }
                }
            } //Scoped Connection

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN CLEANING UP FILES WITH TTL" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN CLEANING UP FILES WITH TTL", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN CLEANING UP FILES WITH TTL" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN CLEANING UP FILES WITH TTL", er.what());
        }

        try {
            MetricCollector metric;

            {
//                ENGINE_LOG_WARNING << "MySQLMetaImpl::CleanUpFilesWithTTL: clean tables: connection in use before creating ScopedConnection = "
//                                   << mysql_connection_pool_->getConnectionsInUse();

                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::CleanUpFilesWithTTL: clean tables: connection in use after creating ScopedConnection = "
//                    << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query cleanUpFilesWithTTLQuery = connectionPtr->query();
                cleanUpFilesWithTTLQuery << "SELECT id, table_id " <<
                                         "FROM Tables " <<
                                         "WHERE state = " << std::to_string(TableSchema::TO_DELETE) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                StoreQueryResult res = cleanUpFilesWithTTLQuery.store();
//            std::cout << res.num_rows() << std::endl;

                if (!res.empty()) {

                    std::stringstream idsToDeleteSS;
                    for (auto &resRow : res) {
                        size_t id = resRow["id"];
                        std::string table_id;
                        resRow["table_id"].to_string(table_id);

                        auto table_path = GetTablePath(table_id);

                        ENGINE_LOG_DEBUG << "Remove table folder: " << table_path;
                        boost::filesystem::remove_all(table_path);

                        idsToDeleteSS << "id = " << std::to_string(id) << " OR ";
                    }
                    std::string idsToDeleteStr = idsToDeleteSS.str();
                    idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4); //remove the last " OR "
                    cleanUpFilesWithTTLQuery << "DELETE FROM Tables WHERE " <<
                                             idsToDeleteStr << ";";

                    ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                    if (!cleanUpFilesWithTTLQuery.exec()) {
                        ENGINE_LOG_ERROR << "QUERY ERROR WHEN CLEANING UP FILES WITH TTL";
                        return Status::DBTransactionError("QUERY ERROR WHEN CLEANING UP FILES WITH TTL",
                                                          cleanUpFilesWithTTLQuery.error());
                    }
                }
           } //Scoped Connection

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN CLEANING UP FILES WITH TTL" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN CLEANING UP FILES WITH TTL", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN CLEANING UP FILES WITH TTL" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN CLEANING UP FILES WITH TTL", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::CleanUp() {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

            if (connectionPtr == nullptr) {
                return Status::Error("Failed to connect to database server");
            }

//            if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                ENGINE_LOG_WARNING << "MySQLMetaImpl::CleanUp: connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//            }

            Query cleanUpQuery = connectionPtr->query();
            cleanUpQuery << "SELECT table_name " <<
                         "FROM information_schema.tables " <<
                         "WHERE table_schema = " << quote << mysql_connection_pool_->getDB() << quote << " " <<
                         "AND table_name = " << quote << "TableFiles" << quote << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUp: " << cleanUpQuery.str();

            StoreQueryResult res = cleanUpQuery.store();

            if (!res.empty()) {
                ENGINE_LOG_DEBUG << "Remove table file type as NEW";
                cleanUpQuery << "DELETE FROM TableFiles WHERE file_type = " << std::to_string(TableFileSchema::NEW) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUp: " << cleanUpQuery.str();

                if (!cleanUpQuery.exec()) {
                    ENGINE_LOG_ERROR << "QUERY ERROR WHEN CLEANING UP FILES";
                    return Status::DBTransactionError("Clean up Error", cleanUpQuery.error());
                }
            }

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN CLEANING UP FILES" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN CLEANING UP FILES", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN CLEANING UP FILES" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN CLEANING UP FILES", er.what());
        }

        return Status::OK();
    }

    Status MySQLMetaImpl::Count(const std::string &table_id, uint64_t &result) {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        try {
            MetricCollector metric;

            TableSchema table_schema;
            table_schema.table_id_ = table_id;
            auto status = DescribeTable(table_schema);

            if (!status.ok()) {
                return status;
            }

            StoreQueryResult res;

            {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

                if (connectionPtr == nullptr) {
                    return Status::Error("Failed to connect to database server");
                }

//                if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                    ENGINE_LOG_WARNING << "MySQLMetaImpl::Count: connection in use = " << mysql_connection_pool_->getConnectionsInUse();
//                }

                Query countQuery = connectionPtr->query();
                countQuery << "SELECT size " <<
                           "FROM TableFiles " <<
                           "WHERE table_id = " << quote << table_id << " AND " <<
                           "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                           "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                           "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::Count: " << countQuery.str();

                res = countQuery.store();
            } //Scoped Connection

            result = 0;
            for (auto &resRow : res) {
                size_t size = resRow["size"];
                result += size;
            }

            if (table_schema.dimension_ <= 0) {
                std::stringstream errorMsg;
                errorMsg << "MySQLMetaImpl::Count: " << "table dimension = " << std::to_string(table_schema.dimension_) << ", table_id = " << table_id;
                ENGINE_LOG_ERROR << errorMsg.str();
                return Status::Error(errorMsg.str());
            }
            result /= table_schema.dimension_;
            result /= sizeof(float);

        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN RETRIEVING COUNT" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN RETRIEVING COUNT", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN RETRIEVING COUNT" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN RETRIEVING COUNT", er.what());
        }
        return Status::OK();
    }

    Status MySQLMetaImpl::DropAll() {

//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);

        if (boost::filesystem::is_directory(options_.path)) {
            boost::filesystem::remove_all(options_.path);
        }
        try {

            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab);

            if (connectionPtr == nullptr) {
                return Status::Error("Failed to connect to database server");
            }

//            if (mysql_connection_pool_->getConnectionsInUse() <= 0) {
//                ENGINE_LOG_WARNING << "MySQLMetaImpl::DropAll: connection in use  = " << mysql_connection_pool_->getConnectionsInUse();
//            }

            Query dropTableQuery = connectionPtr->query();
            dropTableQuery << "DROP TABLE IF EXISTS Tables, TableFiles;";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropAll: " << dropTableQuery.str();

            if (dropTableQuery.exec()) {
                return Status::OK();
            }
            else {
                ENGINE_LOG_ERROR << "QUERY ERROR WHEN DROPPING TABLE";
                return Status::DBTransactionError("DROP TABLE ERROR", dropTableQuery.error());
            }
        } catch (const BadQuery& er) {
            // Handle any query errors
            ENGINE_LOG_ERROR << "QUERY ERROR WHEN DROPPING TABLE" << ": " << er.what();
            return Status::DBTransactionError("QUERY ERROR WHEN DROPPING TABLE", er.what());
        } catch (const Exception& er) {
            // Catch-all for any other MySQL++ exceptions
            ENGINE_LOG_ERROR << "GENERAL ERROR WHEN DROPPING TABLE" << ": " << er.what();
            return Status::DBTransactionError("GENERAL ERROR WHEN DROPPING TABLE", er.what());
        }
        return Status::OK();
    }

    MySQLMetaImpl::~MySQLMetaImpl() {
//        std::lock_guard<std::recursive_mutex> lock(mysql_mutex);
        if (mode_ != Options::MODE::READ_ONLY) {
            CleanUp();
        }
    }

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
