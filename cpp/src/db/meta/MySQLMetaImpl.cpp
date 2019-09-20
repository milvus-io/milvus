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

#include "MySQLMetaImpl.h"
#include "db/IDGenerator.h"
#include "db/Utils.h"
#include "utils/Log.h"
#include "utils/Exception.h"
#include "MetaConsts.h"
#include "metrics/Metrics.h"

#include <unistd.h>
#include <sstream>
#include <iostream>
#include <chrono>
#include <fstream>
#include <regex>
#include <string>
#include <mutex>
#include <thread>
#include <string.h>
#include <boost/filesystem.hpp>
#include <mysql++/mysql++.h>


namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

using namespace mysqlpp;

namespace {

Status HandleException(const std::string &desc, const char* what = nullptr) {
    if(what == nullptr) {
        ENGINE_LOG_ERROR << desc;
        return Status(DB_META_TRANSACTION_FAILED, desc);
    } else {
        std::string msg = desc + ":" + what;
        ENGINE_LOG_ERROR << msg;
        return Status(DB_META_TRANSACTION_FAILED, msg);
    }
}

class MetaField {
public:
    MetaField(const std::string& name, const std::string& type, const std::string& setting)
        : name_(name),
          type_(type),
          setting_(setting) {
    }

    std::string name() const {
        return name_;
    }

    std::string ToString() const {
        return name_ + " " + type_ + " " + setting_;
    }

    // mysql field type has additional information. for instance, a filed type is defined as 'BIGINT'
    // we get the type from sql is 'bigint(20)', so we need to ignore the '(20)'
    bool IsEqual(const MetaField& field) const {
        size_t name_len_min = field.name_.length() > name_.length() ? name_.length() : field.name_.length();
        size_t type_len_min = field.type_.length() > type_.length() ? type_.length() : field.type_.length();
        return strncasecmp(field.name_.c_str(), name_.c_str(), name_len_min) == 0 &&
                strncasecmp(field.type_.c_str(), type_.c_str(), type_len_min) == 0;
    }

private:
    std::string name_;
    std::string type_;
    std::string setting_;
};

using MetaFields = std::vector<MetaField>;
class MetaSchema {
public:
    MetaSchema(const std::string& name, const MetaFields& fields)
        : name_(name),
          fields_(fields) {
    }

    std::string name() const {
        return name_;
    }

    std::string ToString() const {
        std::string result;
        for(auto& field : fields_) {
            if(!result.empty()) {
                result += ",";
            }
            result += field.ToString();
        }
        return result;
    }

    //if the outer fields contains all this MetaSchema fields, return true
    //otherwise return false
    bool IsEqual(const MetaFields& fields) const {
        std::vector<std::string> found_field;
        for(const auto& this_field : fields_) {
            for(const auto& outer_field : fields) {
                if(this_field.IsEqual(outer_field)) {
                    found_field.push_back(this_field.name());
                    break;
                }
            }
        }

        return found_field.size() == fields_.size();
    }

private:
    std::string name_;
    MetaFields fields_;
};

//Tables schema
static const MetaSchema TABLES_SCHEMA(META_TABLES, {
    MetaField("id", "BIGINT", "PRIMARY KEY AUTO_INCREMENT"),
    MetaField("table_id", "VARCHAR(255)", "UNIQUE NOT NULL"),
    MetaField("state", "INT", "NOT NULL"),
    MetaField("dimension", "SMALLINT", "NOT NULL"),
    MetaField("created_on", "BIGINT", "NOT NULL"),
    MetaField("flag", "BIGINT", "DEFAULT 0 NOT NULL"),
    MetaField("index_file_size", "BIGINT", "DEFAULT 1024 NOT NULL"),
    MetaField("engine_type", "INT", "DEFAULT 1 NOT NULL"),
    MetaField("nlist", "INT", "DEFAULT 16384 NOT NULL"),
    MetaField("metric_type", "INT", "DEFAULT 1 NOT NULL"),
});

//TableFiles schema
static const MetaSchema TABLEFILES_SCHEMA(META_TABLEFILES, {
    MetaField("id", "BIGINT", "PRIMARY KEY AUTO_INCREMENT"),
    MetaField("table_id", "VARCHAR(255)", "NOT NULL"),
    MetaField("engine_type", "INT", "DEFAULT 1 NOT NULL"),
    MetaField("file_id", "VARCHAR(255)", "NOT NULL"),
    MetaField("file_type", "INT", "DEFAULT 0 NOT NULL"),
    MetaField("file_size", "BIGINT", "DEFAULT 0 NOT NULL"),
    MetaField("row_count", "BIGINT", "DEFAULT 0 NOT NULL"),
    MetaField("updated_time", "BIGINT", "NOT NULL"),
    MetaField("created_on", "BIGINT", "NOT NULL"),
    MetaField("date", "INT", "DEFAULT -1 NOT NULL"),
});

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
MySQLMetaImpl::MySQLMetaImpl(const DBMetaOptions &options_, const int &mode)
    : options_(options_),
      mode_(mode) {
    Initialize();
}

MySQLMetaImpl::~MySQLMetaImpl() {

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

void MySQLMetaImpl::ValidateMetaSchema() {
    if(nullptr == mysql_connection_pool_) {
        return;
    }

    ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
    if (connectionPtr == nullptr) {
        return;
    }

    auto validate_func = [&](const MetaSchema& schema) {
        Query query_statement = connectionPtr->query();
        query_statement << "DESC " << schema.name() << ";";

        MetaFields exist_fields;

        try {
            StoreQueryResult res = query_statement.store();
            for (size_t i = 0; i < res.num_rows(); i++) {
                const Row &row = res[i];
                std::string name, type;
                row["Field"].to_string(name);
                row["Type"].to_string(type);

                exist_fields.push_back(MetaField(name, type, ""));
            }
        } catch (std::exception &e) {
            ENGINE_LOG_DEBUG << "Meta table '" << schema.name() << "' not exist and will be created";
        }

        if(exist_fields.empty()) {
            return true;
        }

        return schema.IsEqual(exist_fields);
    };

    //verify Tables
    if (!validate_func(TABLES_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }

    //verufy TableFiles
    if (!validate_func(TABLEFILES_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta TableFiles schema is created by Milvus old version");
    }
}

Status MySQLMetaImpl::Initialize() {
    //step 1: create db root path
    if (!boost::filesystem::is_directory(options_.path_)) {
        auto ret = boost::filesystem::create_directory(options_.path_);
        if (!ret) {
            std::string msg = "Failed to create db directory " + options_.path_;
            ENGINE_LOG_ERROR << msg;
            return Status(DB_META_TRANSACTION_FAILED, msg);
        }
    }

    std::string uri = options_.backend_uri_;

    //step 2: parse and check meta uri
    utils::MetaUriInfo uri_info;
    auto status = utils::ParseMetaUri(uri, uri_info);
    if(!status.ok()) {
        std::string msg = "Wrong URI format: " + uri;
        ENGINE_LOG_ERROR << msg;
        throw Exception(DB_INVALID_META_URI, msg);
    }

    if (strcasecmp(uri_info.dialect_.c_str(), "mysql") != 0) {
        std::string msg = "URI's dialect is not MySQL";
        ENGINE_LOG_ERROR << msg;
        throw Exception(DB_INVALID_META_URI, msg);
    }

    //step 3: connect mysql
    int thread_hint = std::thread::hardware_concurrency();
    int max_pool_size = (thread_hint == 0) ? 8 : thread_hint;
    unsigned int port = 0;
    if (!uri_info.port_.empty()) {
        port = std::stoi(uri_info.port_);
    }

    mysql_connection_pool_ =
            std::make_shared<MySQLConnectionPool>(uri_info.db_name_, uri_info.username_,
                    uri_info.password_, uri_info.host_, port, max_pool_size);
    ENGINE_LOG_DEBUG << "MySQL connection pool: maximum pool size = " << std::to_string(max_pool_size);

    //step 4: validate to avoid open old version schema
    ValidateMetaSchema();

    //step 5: create meta tables
    try {

        if (mode_ != DBOptions::MODE::READ_ONLY) {
            CleanUp();
        }

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }


            if (!connectionPtr->thread_aware()) {
                ENGINE_LOG_ERROR << "MySQL++ wasn't built with thread awareness! Can't run without it.";
                return Status(DB_ERROR, "MySQL++ wasn't built with thread awareness! Can't run without it.");
            }
            Query InitializeQuery = connectionPtr->query();

            InitializeQuery << "CREATE TABLE IF NOT EXISTS " <<
                            TABLES_SCHEMA.name() << " (" << TABLES_SCHEMA.ToString() + ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::Initialize: " << InitializeQuery.str();

            if (!InitializeQuery.exec()) {
                return HandleException("Initialization Error", InitializeQuery.error());
            }

            InitializeQuery << "CREATE TABLE IF NOT EXISTS " <<
                            TABLEFILES_SCHEMA.name() << " (" << TABLEFILES_SCHEMA.ToString() + ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::Initialize: " << InitializeQuery.str();

            if (!InitializeQuery.exec()) {
                return HandleException("Initialization Error", InitializeQuery.error());
            }
        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR DURING INITIALIZATION", e.what());
    }

    return Status::OK();
}

// PXU TODO: Temp solution. Will fix later
Status MySQLMetaImpl::DropPartitionsByDates(const std::string &table_id,
                                            const DatesT &dates) {
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
        std::stringstream dateListSS;
        for (auto &date : dates) {
            dateListSS << std::to_string(date) << ", ";
        }
        std::string dateListStr = dateListSS.str();
        dateListStr = dateListStr.substr(0, dateListStr.size() - 2); //remove the last ", "

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }


            Query dropPartitionsByDatesQuery = connectionPtr->query();

            dropPartitionsByDatesQuery << "UPDATE " <<
                                       META_TABLEFILES << " " <<
                                       "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << "," <<
                                       "updated_time = " << utils::GetMicroSecTimeStamp() << " " <<
                                       "WHERE table_id = " << quote << table_id << " AND " <<
                                       "date in (" << dateListStr << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropPartitionsByDates: " << dropPartitionsByDatesQuery.str();

            if (!dropPartitionsByDatesQuery.exec()) {
                return HandleException("QUERY ERROR WHEN DROPPING PARTITIONS BY DATES", dropPartitionsByDatesQuery.error());
            }
        } //Scoped Connection
    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DROPPING PARTITIONS BY DATES", e.what());
    }
    return Status::OK();
}

Status MySQLMetaImpl::CreateTable(TableSchema &table_schema) {
    try {
        server::MetricCollector metric;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query createTableQuery = connectionPtr->query();

            if (table_schema.table_id_.empty()) {
                NextTableId(table_schema.table_id_);
            } else {
                createTableQuery << "SELECT state FROM " <<
                                 META_TABLES << " " <<
                                 "WHERE table_id = " << quote << table_schema.table_id_ << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CreateTable: " << createTableQuery.str();

                StoreQueryResult res = createTableQuery.store();

                if (res.num_rows() == 1) {
                    int state = res[0]["state"];
                    if (TableSchema::TO_DELETE == state) {
                        return Status(DB_ERROR, "Table already exists and it is in delete state, please wait a second");
                    } else {
                        return Status(DB_ALREADY_EXIST, "Table already exists");
                    }
                }
            }

            table_schema.id_ = -1;
            table_schema.created_on_ = utils::GetMicroSecTimeStamp();

            std::string id = "NULL"; //auto-increment
            std::string table_id = table_schema.table_id_;
            std::string state = std::to_string(table_schema.state_);
            std::string dimension = std::to_string(table_schema.dimension_);
            std::string created_on = std::to_string(table_schema.created_on_);
            std::string flag = std::to_string(table_schema.flag_);
            std::string index_file_size = std::to_string(table_schema.index_file_size_);
            std::string engine_type = std::to_string(table_schema.engine_type_);
            std::string nlist = std::to_string(table_schema.nlist_);
            std::string metric_type = std::to_string(table_schema.metric_type_);

            createTableQuery << "INSERT INTO " <<
                             META_TABLES << " " <<
                             "VALUES(" << id << ", " << quote << table_id << ", " << state << ", " << dimension << ", " <<
                             created_on << ", " << flag << ", " << index_file_size << ", " << engine_type << ", " <<
                             nlist << ", " << metric_type << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::CreateTable: " << createTableQuery.str();

            if (SimpleResult res = createTableQuery.execute()) {
                table_schema.id_ = res.insert_id(); //Might need to use SELECT LAST_INSERT_ID()?

                //Consume all results to avoid "Commands out of sync" error
            } else {
                return HandleException("Add Table Error", createTableQuery.error());
            }
        } //Scoped Connection

        return utils::CreateTablePath(options_, table_schema.table_id_);

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CREATING TABLE", e.what());
    }
}

Status MySQLMetaImpl::FilesByType(const std::string &table_id,
                                  const std::vector<int> &file_types,
                                  std::vector<std::string> &file_ids) {
    if(file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    try {
        file_ids.clear();

        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            std::string types;
            for(auto type : file_types) {
                if(!types.empty()) {
                    types += ",";
                }
                types += std::to_string(type);
            }

            Query hasNonIndexFilesQuery = connectionPtr->query();
            //since table_id is a unique column we just need to check whether it exists or not
            hasNonIndexFilesQuery << "SELECT file_id, file_type FROM " <<
                                  META_TABLEFILES << " " <<
                                  "WHERE table_id = " << quote << table_id << " AND " <<
                                  "file_type in (" << types << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesByType: " << hasNonIndexFilesQuery.str();

            res = hasNonIndexFilesQuery.store();
        } //Scoped Connection

        if (res.num_rows() > 0) {
            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
            int to_index_count = 0, index_count = 0, backup_count = 0;
            for (auto &resRow : res) {
                std::string file_id;
                resRow["file_id"].to_string(file_id);
                file_ids.push_back(file_id);

                int32_t file_type = resRow["file_type"];
                switch (file_type) {
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
                    case (int) TableFileSchema::INDEX:
                        index_count++;
                        break;
                    case (int) TableFileSchema::BACKUP:
                        backup_count++;
                        break;
                    default:
                        break;
                }
            }

            ENGINE_LOG_DEBUG << "Table " << table_id << " currently has raw files:" << raw_count
                             << " new files:" << new_count << " new_merge files:" << new_merge_count
                             << " new_index files:" << new_index_count << " to_index files:" << to_index_count
                             << " index files:" << index_count << " backup files:" << backup_count;
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN GET FILE BY TYPE", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::UpdateTableIndex(const std::string &table_id, const TableIndex& index) {
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query updateTableIndexParamQuery = connectionPtr->query();
            updateTableIndexParamQuery << "SELECT id, state, dimension, created_on FROM " <<
                                       META_TABLES << " " <<
                                       "WHERE table_id = " << quote << table_id << " AND " <<
                                       "state <> " << std::to_string(TableSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableIndex: " << updateTableIndexParamQuery.str();

            StoreQueryResult res = updateTableIndexParamQuery.store();

            if (res.num_rows() == 1) {
                const Row &resRow = res[0];

                size_t id = resRow["id"];
                int32_t state = resRow["state"];
                uint16_t dimension = resRow["dimension"];
                int64_t created_on = resRow["created_on"];

                updateTableIndexParamQuery << "UPDATE " <<
                                           META_TABLES << " " <<
                                           "SET id = " << id << ", " <<
                                           "state = " << state << ", " <<
                                           "dimension = " << dimension << ", " <<
                                           "created_on = " << created_on << ", " <<
                                           "engine_type = " << index.engine_type_ << ", " <<
                                           "nlist = " << index.nlist_ << ", " <<
                                           "metric_type = " << index.metric_type_ << " " <<
                                           "WHERE table_id = " << quote << table_id << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableIndex: " << updateTableIndexParamQuery.str();


                if (!updateTableIndexParamQuery.exec()) {
                    return HandleException("QUERY ERROR WHEN UPDATING TABLE INDEX PARAM", updateTableIndexParamQuery.error());
                }
            } else {
                return Status(DB_NOT_FOUND, "Table " + table_id + " not found");
            }

        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN UPDATING TABLE INDEX PARAM", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::UpdateTableFlag(const std::string &table_id, int64_t flag) {
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query updateTableFlagQuery = connectionPtr->query();
            updateTableFlagQuery << "UPDATE " <<
                                 META_TABLES << " " <<
                                 "SET flag = " << flag << " " <<
                                 "WHERE table_id = " << quote << table_id << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFlag: " << updateTableFlagQuery.str();

            if (!updateTableFlagQuery.exec()) {
                return HandleException("QUERY ERROR WHEN UPDATING TABLE FLAG", updateTableFlagQuery.error());
            }

        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN UPDATING TABLE FLAG", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DescribeTableIndex(const std::string &table_id, TableIndex& index) {
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query describeTableIndexQuery = connectionPtr->query();
            describeTableIndexQuery << "SELECT engine_type, nlist, index_file_size, metric_type FROM " <<
                                       META_TABLES << " " <<
                                       "WHERE table_id = " << quote << table_id << " AND " <<
                                       "state <> " << std::to_string(TableSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DescribeTableIndex: " << describeTableIndexQuery.str();

            StoreQueryResult res = describeTableIndexQuery.store();

            if (res.num_rows() == 1) {
                const Row &resRow = res[0];

                index.engine_type_ = resRow["engine_type"];
                index.nlist_ = resRow["nlist"];
                index.metric_type_ = resRow["metric_type"];
            } else {
                return Status(DB_NOT_FOUND, "Table " + table_id + " not found");
            }

        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN UPDATING TABLE FLAG", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DropTableIndex(const std::string &table_id) {
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query dropTableIndexQuery = connectionPtr->query();

            //soft delete index files
            dropTableIndexQuery << "UPDATE " <<
                                META_TABLEFILES << " " <<
                                "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << "," <<
                                "updated_time = " << utils::GetMicroSecTimeStamp() << " " <<
                                "WHERE table_id = " << quote << table_id << " AND " <<
                                "file_type = " << std::to_string(TableFileSchema::INDEX) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropTableIndex: " << dropTableIndexQuery.str();

            if (!dropTableIndexQuery.exec()) {
                return HandleException("QUERY ERROR WHEN DROPPING TABLE INDEX", dropTableIndexQuery.error());
            }

            //set all backup file to raw
            dropTableIndexQuery << "UPDATE " <<
                                META_TABLEFILES << " " <<
                                "SET file_type = " << std::to_string(TableFileSchema::RAW) << "," <<
                                "updated_time = " << utils::GetMicroSecTimeStamp() << " " <<
                                "WHERE table_id = " << quote << table_id << " AND " <<
                                "file_type = " << std::to_string(TableFileSchema::BACKUP) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropTableIndex: " << dropTableIndexQuery.str();

            if (!dropTableIndexQuery.exec()) {
                return HandleException("QUERY ERROR WHEN DROPPING TABLE INDEX", dropTableIndexQuery.error());
            }

            //set table index type to raw
            dropTableIndexQuery << "UPDATE " <<
                                META_TABLES << " " <<
                                "SET engine_type = " << std::to_string(DEFAULT_ENGINE_TYPE) << "," <<
                                "nlist = " << std::to_string(DEFAULT_NLIST) << ", " <<
                                "metric_type = " << std::to_string(DEFAULT_METRIC_TYPE) << " " <<
                                "WHERE table_id = " << quote << table_id << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropTableIndex: " << dropTableIndexQuery.str();

            if (!dropTableIndexQuery.exec()) {
                return HandleException("QUERY ERROR WHEN DROPPING TABLE INDEX", dropTableIndexQuery.error());
            }

        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DROPPING TABLE INDEX", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DeleteTable(const std::string &table_id) {
    try {
        server::MetricCollector metric;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            //soft delete table
            Query deleteTableQuery = connectionPtr->query();
//
            deleteTableQuery << "UPDATE " <<
                             META_TABLES << " " <<
                             "SET state = " << std::to_string(TableSchema::TO_DELETE) << " " <<
                             "WHERE table_id = " << quote << table_id << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DeleteTable: " << deleteTableQuery.str();

            if (!deleteTableQuery.exec()) {
                return HandleException("QUERY ERROR WHEN DELETING TABLE", deleteTableQuery.error());
            }

        } //Scoped Connection

        if (mode_ == DBOptions::MODE::CLUSTER) {
            DeleteTableFiles(table_id);
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DELETING TABLE", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DeleteTableFiles(const std::string &table_id) {
    try {
        server::MetricCollector metric;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            //soft delete table files
            Query deleteTableFilesQuery = connectionPtr->query();
            //
            deleteTableFilesQuery << "UPDATE " <<
                                  META_TABLEFILES << " " <<
                                  "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << ", " <<
                                  "updated_time = " << std::to_string(utils::GetMicroSecTimeStamp()) << " " <<
                                  "WHERE table_id = " << quote << table_id << " AND " <<
                                  "file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DeleteTableFiles: " << deleteTableFilesQuery.str();

            if (!deleteTableFilesQuery.exec()) {
                return HandleException("QUERY ERROR WHEN DELETING TABLE FILES", deleteTableFilesQuery.error());
            }
        } //Scoped Connection
    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DELETING TABLE FILES", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DescribeTable(TableSchema &table_schema) {
    try {
        server::MetricCollector metric;
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query describeTableQuery = connectionPtr->query();
            describeTableQuery << "SELECT id, state, dimension, created_on, flag, index_file_size, engine_type, nlist, metric_type FROM " <<
                               META_TABLES << " " <<
                               "WHERE table_id = " << quote << table_schema.table_id_ << " " <<
                               "AND state <> " << std::to_string(TableSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DescribeTable: " << describeTableQuery.str();

            res = describeTableQuery.store();
        } //Scoped Connection

        if (res.num_rows() == 1) {
            const Row &resRow = res[0];

            table_schema.id_ = resRow["id"]; //implicit conversion

            table_schema.state_ = resRow["state"];

            table_schema.dimension_ = resRow["dimension"];

            table_schema.created_on_ = resRow["created_on"];

            table_schema.flag_ = resRow["flag"];

            table_schema.index_file_size_ = resRow["index_file_size"];

            table_schema.engine_type_ = resRow["engine_type"];

            table_schema.nlist_ = resRow["nlist"];

            table_schema.metric_type_ = resRow["metric_type"];
        } else {
            return Status(DB_NOT_FOUND, "Table " + table_schema.table_id_ + " not found");
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DESCRIBING TABLE", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::HasTable(const std::string &table_id, bool &has_or_not) {
    try {
        server::MetricCollector metric;
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query hasTableQuery = connectionPtr->query();
            //since table_id is a unique column we just need to check whether it exists or not
            hasTableQuery << "SELECT EXISTS " <<
                          "(SELECT 1 FROM " <<
                          META_TABLES << " " <<
                          "WHERE table_id = " << quote << table_id << " " <<
                          "AND state <> " << std::to_string(TableSchema::TO_DELETE) << ") " <<
                          "AS " << quote << "check" << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::HasTable: " << hasTableQuery.str();

            res = hasTableQuery.store();
        } //Scoped Connection

        int check = res[0]["check"];
        has_or_not = (check == 1);

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CHECKING IF TABLE EXISTS", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::AllTables(std::vector<TableSchema> &table_schema_array) {
    try {
        server::MetricCollector metric;
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query allTablesQuery = connectionPtr->query();
            allTablesQuery << "SELECT id, table_id, dimension, engine_type, nlist, index_file_size, metric_type FROM " <<
                           META_TABLES << " " <<
                           "WHERE state <> " << std::to_string(TableSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::AllTables: " << allTablesQuery.str();

            res = allTablesQuery.store();
        } //Scoped Connection

        for (auto &resRow : res) {
            TableSchema table_schema;

            table_schema.id_ = resRow["id"]; //implicit conversion

            std::string table_id;
            resRow["table_id"].to_string(table_id);
            table_schema.table_id_ = table_id;

            table_schema.dimension_ = resRow["dimension"];

            table_schema.index_file_size_ = resRow["index_file_size"];

            table_schema.engine_type_ = resRow["engine_type"];

            table_schema.nlist_ = resRow["nlist"];

            table_schema.metric_type_ = resRow["metric_type"];

            table_schema_array.emplace_back(table_schema);
        }
    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DESCRIBING ALL TABLES", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::CreateTableFile(TableFileSchema &file_schema) {
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

        std::string id = "NULL"; //auto-increment
        std::string table_id = file_schema.table_id_;
        std::string engine_type = std::to_string(file_schema.engine_type_);
        std::string file_id = file_schema.file_id_;
        std::string file_type = std::to_string(file_schema.file_type_);
        std::string file_size = std::to_string(file_schema.file_size_);
        std::string row_count = std::to_string(file_schema.row_count_);
        std::string updated_time = std::to_string(file_schema.updated_time_);
        std::string created_on = std::to_string(file_schema.created_on_);
        std::string date = std::to_string(file_schema.date_);

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query createTableFileQuery = connectionPtr->query();

            createTableFileQuery << "INSERT INTO " <<
                                 META_TABLEFILES << " " <<
                                 "VALUES(" << id << ", " << quote << table_id << ", " << engine_type << ", " <<
                                 quote << file_id << ", " << file_type << ", " << file_size << ", " <<
                                 row_count << ", " << updated_time << ", " << created_on << ", " << date << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::CreateTableFile: " << createTableFileQuery.str();

            if (SimpleResult res = createTableFileQuery.execute()) {
                file_schema.id_ = res.insert_id(); //Might need to use SELECT LAST_INSERT_ID()?

                //Consume all results to avoid "Commands out of sync" error
            } else {
                return HandleException("QUERY ERROR WHEN CREATING TABLE FILE", createTableFileQuery.error());
            }
        } // Scoped Connection

        return utils::CreateTableFilePath(options_, file_schema);

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CREATING TABLE FILE", e.what());
    }
}

Status MySQLMetaImpl::FilesToIndex(TableFilesSchema &files) {
    files.clear();

    try {
        server::MetricCollector metric;
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query filesToIndexQuery = connectionPtr->query();
            filesToIndexQuery << "SELECT id, table_id, engine_type, file_id, file_type, file_size, row_count, date, created_on FROM " <<
                              META_TABLEFILES << " " <<
                              "WHERE file_type = " << std::to_string(TableFileSchema::TO_INDEX) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToIndex: " << filesToIndexQuery.str();

            res = filesToIndexQuery.store();
        } //Scoped Connection

        Status ret;
        std::map<std::string, TableSchema> groups;
        TableFileSchema table_file;
        for (auto &resRow : res) {

            table_file.id_ = resRow["id"]; //implicit conversion

            std::string table_id;
            resRow["table_id"].to_string(table_id);
            table_file.table_id_ = table_id;

            table_file.engine_type_ = resRow["engine_type"];

            std::string file_id;
            resRow["file_id"].to_string(file_id);
            table_file.file_id_ = file_id;

            table_file.file_type_ = resRow["file_type"];

            table_file.file_size_ = resRow["file_size"];

            table_file.row_count_ = resRow["row_count"];

            table_file.date_ = resRow["date"];

            table_file.created_on_ = resRow["created_on"];

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

            auto status = utils::GetTableFilePath(options_, table_file);
            if(!status.ok()) {
                ret = status;
            }

            files.push_back(table_file);
        }

        return ret;

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN FINDING TABLE FILES TO INDEX", e.what());
    }
}

Status MySQLMetaImpl::FilesToSearch(const std::string &table_id,
                                    const std::vector<size_t> &ids,
                                    const DatesT &partition,
                                    DatePartionedTableFilesSchema &files) {
    files.clear();

    try {
        server::MetricCollector metric;
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query filesToSearchQuery = connectionPtr->query();
            filesToSearchQuery << "SELECT id, table_id, engine_type, file_id, file_type, file_size, row_count, date FROM " <<
                               META_TABLEFILES << " " <<
                               "WHERE table_id = " << quote << table_id;

            if (!partition.empty()) {
                std::stringstream partitionListSS;
                for (auto &date : partition) {
                    partitionListSS << std::to_string(date) << ", ";
                }
                std::string partitionListStr = partitionListSS.str();

                partitionListStr = partitionListStr.substr(0, partitionListStr.size() - 2); //remove the last ", "
                filesToSearchQuery << " AND " << "date IN (" << partitionListStr << ")";
            }

            if (!ids.empty()) {
                std::stringstream idSS;
                for (auto &id : ids) {
                    idSS << "id = " << std::to_string(id) << " OR ";
                }
                std::string idStr = idSS.str();
                idStr = idStr.substr(0, idStr.size() - 4); //remove the last " OR "

                filesToSearchQuery  << " AND " << "(" << idStr << ")";

            }
            // End
            filesToSearchQuery << " AND " <<
                               "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                               "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                               "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToSearch: " << filesToSearchQuery.str();

            res = filesToSearchQuery.store();
        } //Scoped Connection

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);
        if (!status.ok()) {
            return status;
        }

        Status ret;
        TableFileSchema table_file;
        for (auto &resRow : res) {

            table_file.id_ = resRow["id"]; //implicit conversion

            std::string table_id_str;
            resRow["table_id"].to_string(table_id_str);
            table_file.table_id_ = table_id_str;

            table_file.index_file_size_ = table_schema.index_file_size_;

            table_file.engine_type_ = resRow["engine_type"];

            table_file.nlist_ = table_schema.nlist_;

            table_file.metric_type_ = table_schema.metric_type_;

            std::string file_id;
            resRow["file_id"].to_string(file_id);
            table_file.file_id_ = file_id;

            table_file.file_type_ = resRow["file_type"];

            table_file.file_size_ = resRow["file_size"];

            table_file.row_count_ = resRow["row_count"];

            table_file.date_ = resRow["date"];

            table_file.dimension_ = table_schema.dimension_;

            auto status = utils::GetTableFilePath(options_, table_file);
            if(!status.ok()) {
                ret = status;
            }

            auto dateItr = files.find(table_file.date_);
            if (dateItr == files.end()) {
                files[table_file.date_] = TableFilesSchema();
            }

            files[table_file.date_].push_back(table_file);
        }

        return ret;
    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN FINDING TABLE FILES TO SEARCH", e.what());
    }
}

Status MySQLMetaImpl::FilesToMerge(const std::string &table_id,
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

        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query filesToMergeQuery = connectionPtr->query();
            filesToMergeQuery << "SELECT id, table_id, file_id, file_type, file_size, row_count, date, engine_type, created_on FROM " <<
                              META_TABLEFILES << " " <<
                              "WHERE table_id = " << quote << table_id << " AND " <<
                              "file_type = " << std::to_string(TableFileSchema::RAW) << " " <<
                              "ORDER BY row_count DESC" << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::FilesToMerge: " << filesToMergeQuery.str();

            res = filesToMergeQuery.store();
        } //Scoped Connection

        Status ret;
        for (auto &resRow : res) {
            TableFileSchema table_file;
            table_file.file_size_ = resRow["file_size"];
            if(table_file.file_size_ >= table_schema.index_file_size_) {
                continue;//skip large file
            }

            table_file.id_ = resRow["id"]; //implicit conversion

            std::string table_id_str;
            resRow["table_id"].to_string(table_id_str);
            table_file.table_id_ = table_id_str;

            std::string file_id;
            resRow["file_id"].to_string(file_id);
            table_file.file_id_ = file_id;

            table_file.file_type_ = resRow["file_type"];

            table_file.row_count_ = resRow["row_count"];

            table_file.date_ = resRow["date"];

            table_file.index_file_size_ = table_schema.index_file_size_;

            table_file.engine_type_ = resRow["engine_type"];

            table_file.nlist_ = table_schema.nlist_;

            table_file.metric_type_ = table_schema.metric_type_;

            table_file.created_on_ = resRow["created_on"];

            table_file.dimension_ = table_schema.dimension_;

            auto status = utils::GetTableFilePath(options_, table_file);
            if(!status.ok()) {
                ret = status;
            }

            auto dateItr = files.find(table_file.date_);
            if (dateItr == files.end()) {
                files[table_file.date_] = TableFilesSchema();
            }

            files[table_file.date_].push_back(table_file);
        }

        return ret;

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN FINDING TABLE FILES TO MERGE", e.what());
    }
}

Status MySQLMetaImpl::GetTableFiles(const std::string &table_id,
                                    const std::vector<size_t> &ids,
                                    TableFilesSchema &table_files) {
    if (ids.empty()) {
        return Status::OK();
    }

    std::stringstream idSS;
    for (auto &id : ids) {
        idSS << "id = " << std::to_string(id) << " OR ";
    }
    std::string idStr = idSS.str();
    idStr = idStr.substr(0, idStr.size() - 4); //remove the last " OR "

    try {
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query getTableFileQuery = connectionPtr->query();
            getTableFileQuery << "SELECT id, engine_type, file_id, file_type, file_size, row_count, date, created_on FROM " <<
                              META_TABLEFILES << " " <<
                              "WHERE table_id = " << quote << table_id << " AND " <<
                              "(" << idStr << ") AND " <<
                              "file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::GetTableFiles: " << getTableFileQuery.str();

            res = getTableFileQuery.store();
        } //Scoped Connection

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        DescribeTable(table_schema);

        Status ret;
        for (auto &resRow : res) {

            TableFileSchema file_schema;

            file_schema.id_ = resRow["id"];

            file_schema.table_id_ = table_id;

            file_schema.index_file_size_ = table_schema.index_file_size_;

            file_schema.engine_type_ = resRow["engine_type"];

            file_schema.nlist_ = table_schema.nlist_;

            file_schema.metric_type_ = table_schema.metric_type_;

            std::string file_id;
            resRow["file_id"].to_string(file_id);
            file_schema.file_id_ = file_id;

            file_schema.file_type_ = resRow["file_type"];

            file_schema.file_size_ = resRow["file_size"];

            file_schema.row_count_ = resRow["row_count"];

            file_schema.date_ = resRow["date"];

            file_schema.created_on_ = resRow["created_on"];

            file_schema.dimension_ = table_schema.dimension_;

            utils::GetTableFilePath(options_, file_schema);

            table_files.emplace_back(file_schema);
        }

        return ret;

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN RETRIEVING TABLE FILES", e.what());
    }
}

// PXU TODO: Support Swap
Status MySQLMetaImpl::Archive() {
    auto &criterias = options_.archive_conf_.GetCriterias();
    if (criterias.empty()) {
        return Status::OK();
    }

    for (auto &kv : criterias) {
        auto &criteria = kv.first;
        auto &limit = kv.second;
        if (criteria == "days") {
            size_t usecs = limit * D_SEC * US_PS;
            long now = utils::GetMicroSecTimeStamp();

            try {
                ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

                if (connectionPtr == nullptr) {
                    return Status(DB_ERROR, "Failed to connect to database server");
                }

                Query archiveQuery = connectionPtr->query();
                archiveQuery << "UPDATE " <<
                             META_TABLEFILES << " " <<
                             "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                             "WHERE created_on < " << std::to_string(now - usecs) << " AND " <<
                             "file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::Archive: " << archiveQuery.str();

                if (!archiveQuery.exec()) {
                    return HandleException("QUERY ERROR DURING ARCHIVE", archiveQuery.error());
                }

            } catch (std::exception &e) {
                return HandleException("GENERAL ERROR WHEN DURING ARCHIVE", e.what());
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
        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query getSizeQuery = connectionPtr->query();
            getSizeQuery << "SELECT IFNULL(SUM(file_size),0) AS sum FROM " <<
                         META_TABLEFILES << " " <<
                         "WHERE file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::Size: " << getSizeQuery.str();

            res = getSizeQuery.store();
        } //Scoped Connection

        if (res.empty()) {
            result = 0;
        } else {
            result = res[0]["sum"];
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN RETRIEVING SIZE", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DiscardFiles(long long to_discard_size) {
    if (to_discard_size <= 0) {

        return Status::OK();
    }
    ENGINE_LOG_DEBUG << "About to discard size=" << to_discard_size;

    try {
        server::MetricCollector metric;
        bool status;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query discardFilesQuery = connectionPtr->query();
            discardFilesQuery << "SELECT id, file_size FROM " <<
                              META_TABLEFILES << " " <<
                              "WHERE file_type <> " << std::to_string(TableFileSchema::TO_DELETE) << " " <<
                              "ORDER BY id ASC " <<
                              "LIMIT 10;";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DiscardFiles: " << discardFilesQuery.str();

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
                table_file.file_size_ = resRow["file_size"];
                idsToDiscardSS << "id = " << std::to_string(table_file.id_) << " OR ";
                ENGINE_LOG_DEBUG << "Discard table_file.id=" << table_file.file_id_
                                 << " table_file.size=" << table_file.file_size_;
                to_discard_size -= table_file.file_size_;
            }

            std::string idsToDiscardStr = idsToDiscardSS.str();
            idsToDiscardStr = idsToDiscardStr.substr(0, idsToDiscardStr.size() - 4); //remove the last " OR "

            discardFilesQuery << "UPDATE " <<
                              META_TABLEFILES << " " <<
                              "SET file_type = " << std::to_string(TableFileSchema::TO_DELETE) << ", " <<
                              "updated_time = " << std::to_string(utils::GetMicroSecTimeStamp()) << " " <<
                              "WHERE " << idsToDiscardStr << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::DiscardFiles: " << discardFilesQuery.str();

            status = discardFilesQuery.exec();
            if (!status) {
                return HandleException("QUERY ERROR WHEN DISCARDING FILES", discardFilesQuery.error());
            }
        } //Scoped Connection

        return DiscardFiles(to_discard_size);

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DISCARDING FILES", e.what());
    }
}

//ZR: this function assumes all fields in file_schema have value
Status MySQLMetaImpl::UpdateTableFile(TableFileSchema &file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();

    try {
        server::MetricCollector metric;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query updateTableFileQuery = connectionPtr->query();

            //if the table has been deleted, just mark the table file as TO_DELETE
            //clean thread will delete the file later
            updateTableFileQuery << "SELECT state FROM " <<
                                 META_TABLES << " " <<
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
            std::string file_size = std::to_string(file_schema.file_size_);
            std::string row_count = std::to_string(file_schema.row_count_);
            std::string updated_time = std::to_string(file_schema.updated_time_);
            std::string created_on = std::to_string(file_schema.created_on_);
            std::string date = std::to_string(file_schema.date_);

            updateTableFileQuery << "UPDATE " <<
                                 META_TABLEFILES << " " <<
                                 "SET table_id = " << quote << table_id << ", " <<
                                 "engine_type = " << engine_type << ", " <<
                                 "file_id = " << quote << file_id << ", " <<
                                 "file_type = " << file_type << ", " <<
                                 "file_size = " << file_size << ", " <<
                                 "row_count = " << row_count << ", " <<
                                 "updated_time = " << updated_time << ", " <<
                                 "created_on = " << created_on << ", " <<
                                 "date = " << date << " " <<
                                 "WHERE id = " << id << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFile: " << updateTableFileQuery.str();

            if (!updateTableFileQuery.exec()) {
                ENGINE_LOG_DEBUG << "table_id= " << file_schema.table_id_ << " file_id=" << file_schema.file_id_;
                return HandleException("QUERY ERROR WHEN UPDATING TABLE FILE", updateTableFileQuery.error());
            }
        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN UPDATING TABLE FILE", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::UpdateTableFilesToIndex(const std::string &table_id) {
    try {
        ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

        if (connectionPtr == nullptr) {
            return Status(DB_ERROR, "Failed to connect to database server");
        }

        Query updateTableFilesToIndexQuery = connectionPtr->query();

        updateTableFilesToIndexQuery << "UPDATE " <<
                                     META_TABLEFILES << " " <<
                                     "SET file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " " <<
                                     "WHERE table_id = " << quote << table_id << " AND " <<
                                     "file_type = " << std::to_string(TableFileSchema::RAW) << ";";

        ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFilesToIndex: " << updateTableFilesToIndexQuery.str();

        if (!updateTableFilesToIndexQuery.exec()) {
            return HandleException("QUERY ERROR WHEN UPDATING TABLE FILE TO INDEX", updateTableFilesToIndexQuery.error());
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN UPDATING TABLE FILES TO INDEX", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::UpdateTableFiles(TableFilesSchema &files) {
    try {
        server::MetricCollector metric;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query updateTableFilesQuery = connectionPtr->query();

            std::map<std::string, bool> has_tables;
            for (auto &file_schema : files) {

                if (has_tables.find(file_schema.table_id_) != has_tables.end()) {
                    continue;
                }

                updateTableFilesQuery << "SELECT EXISTS " <<
                                      "(SELECT 1 FROM " <<
                                      META_TABLES << " " <<
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
                std::string file_size = std::to_string(file_schema.file_size_);
                std::string row_count = std::to_string(file_schema.row_count_);
                std::string updated_time = std::to_string(file_schema.updated_time_);
                std::string created_on = std::to_string(file_schema.created_on_);
                std::string date = std::to_string(file_schema.date_);

                updateTableFilesQuery << "UPDATE " <<
                                      META_TABLEFILES << " " <<
                                      "SET table_id = " << quote << table_id << ", " <<
                                      "engine_type = " << engine_type << ", " <<
                                      "file_id = " << quote << file_id << ", " <<
                                      "file_type = " << file_type << ", " <<
                                      "file_size = " << file_size << ", " <<
                                      "row_count = " << row_count << ", " <<
                                      "updated_time = " << updated_time << ", " <<
                                      "created_on = " << created_on << ", " <<
                                      "date = " << date << " " <<
                                      "WHERE id = " << id << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::UpdateTableFiles: " << updateTableFilesQuery.str();

                if (!updateTableFilesQuery.exec()) {
                    return HandleException("QUERY ERROR WHEN UPDATING TABLE FILES", updateTableFilesQuery.error());
                }
            }
        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN UPDATING TABLE FILES", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::CleanUpFilesWithTTL(uint16_t seconds) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> table_ids;

    //remove to_delete files
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query cleanUpFilesWithTTLQuery = connectionPtr->query();
            cleanUpFilesWithTTLQuery << "SELECT id, table_id, file_id, date FROM " <<
                                     META_TABLEFILES << " " <<
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

                utils::DeleteTableFilePath(options_, table_file);

                ENGINE_LOG_DEBUG << "Removing file id:" << table_file.id_ << " location:" << table_file.location_;

                idsToDelete.emplace_back(std::to_string(table_file.id_));

                table_ids.insert(table_file.table_id_);
            }

            if (!idsToDelete.empty()) {

                std::stringstream idsToDeleteSS;
                for (auto &id : idsToDelete) {
                    idsToDeleteSS << "id = " << id << " OR ";
                }

                std::string idsToDeleteStr = idsToDeleteSS.str();
                idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4); //remove the last " OR "
                cleanUpFilesWithTTLQuery << "DELETE FROM " <<
                                         META_TABLEFILES << " " <<
                                         "WHERE " << idsToDeleteStr << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                if (!cleanUpFilesWithTTLQuery.exec()) {
                    return HandleException("QUERY ERROR WHEN CLEANING UP FILES WITH TTL", cleanUpFilesWithTTLQuery.error());
                }
            }
        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CLEANING UP FILES WITH TTL", e.what());
    }

    //remove to_delete tables
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            Query cleanUpFilesWithTTLQuery = connectionPtr->query();
            cleanUpFilesWithTTLQuery << "SELECT id, table_id FROM " <<
                                     META_TABLES << " " <<
                                     "WHERE state = " << std::to_string(TableSchema::TO_DELETE) << ";";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

            StoreQueryResult res = cleanUpFilesWithTTLQuery.store();

            if (!res.empty()) {

                std::stringstream idsToDeleteSS;
                for (auto &resRow : res) {
                    size_t id = resRow["id"];
                    std::string table_id;
                    resRow["table_id"].to_string(table_id);

                    utils::DeleteTablePath(options_, table_id, false);//only delete empty folder

                    idsToDeleteSS << "id = " << std::to_string(id) << " OR ";
                }
                std::string idsToDeleteStr = idsToDeleteSS.str();
                idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4); //remove the last " OR "
                cleanUpFilesWithTTLQuery << "DELETE FROM " <<
                                         META_TABLES << " " <<
                                         "WHERE " << idsToDeleteStr << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                if (!cleanUpFilesWithTTLQuery.exec()) {
                    return HandleException("QUERY ERROR WHEN CLEANING UP TABLES WITH TTL", cleanUpFilesWithTTLQuery.error());
                }
            }
        } //Scoped Connection

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CLEANING UP TABLES WITH TTL", e.what());
    }

    //remove deleted table folder
    //don't remove table folder until all its files has been deleted
    try {
        server::MetricCollector metric;

        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }

            for(auto& table_id : table_ids) {
                Query cleanUpFilesWithTTLQuery = connectionPtr->query();
                cleanUpFilesWithTTLQuery << "SELECT file_id FROM " <<
                                         META_TABLEFILES << " " <<
                                         "WHERE table_id = " << quote << table_id << ";";

                ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUpFilesWithTTL: " << cleanUpFilesWithTTLQuery.str();

                StoreQueryResult res = cleanUpFilesWithTTLQuery.store();

                if (res.empty()) {
                    utils::DeleteTablePath(options_, table_id);
                }
            }
        }
    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CLEANING UP TABLES WITH TTL", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::CleanUp() {
    try {
        ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

        if (connectionPtr == nullptr) {
            return Status(DB_ERROR, "Failed to connect to database server");
        }

        Query cleanUpQuery = connectionPtr->query();
        cleanUpQuery << "SELECT table_name " <<
                     "FROM information_schema.tables " <<
                     "WHERE table_schema = " << quote << mysql_connection_pool_->getDB() << " " <<
                     "AND table_name = " << quote << META_TABLEFILES << ";";

        ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUp: " << cleanUpQuery.str();

        StoreQueryResult res = cleanUpQuery.store();

        if (!res.empty()) {
            ENGINE_LOG_DEBUG << "Remove table file type as NEW";
            cleanUpQuery << "DELETE FROM " << META_TABLEFILES << " WHERE file_type IN ("
                    << std::to_string(TableFileSchema::NEW) << ","
                    << std::to_string(TableFileSchema::NEW_MERGE) << ","
                    << std::to_string(TableFileSchema::NEW_INDEX) << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::CleanUp: " << cleanUpQuery.str();

            if (!cleanUpQuery.exec()) {
                return HandleException("QUERY ERROR WHEN CLEANING UP FILES", cleanUpQuery.error());
            }
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN CLEANING UP FILES", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::Count(const std::string &table_id, uint64_t &result) {
    try {
        server::MetricCollector metric;

        TableSchema table_schema;
        table_schema.table_id_ = table_id;
        auto status = DescribeTable(table_schema);

        if (!status.ok()) {
            return status;
        }

        StoreQueryResult res;
        {
            ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to database server");
            }


            Query countQuery = connectionPtr->query();
            countQuery << "SELECT row_count FROM " <<
                       META_TABLEFILES << " " <<
                       "WHERE table_id = " << quote << table_id << " AND " <<
                       "(file_type = " << std::to_string(TableFileSchema::RAW) << " OR " <<
                       "file_type = " << std::to_string(TableFileSchema::TO_INDEX) << " OR " <<
                       "file_type = " << std::to_string(TableFileSchema::INDEX) << ");";

            ENGINE_LOG_DEBUG << "MySQLMetaImpl::Count: " << countQuery.str();

            res = countQuery.store();
        } //Scoped Connection

        result = 0;
        for (auto &resRow : res) {
            size_t size = resRow["row_count"];
            result += size;
        }

    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN RETRIEVING COUNT", e.what());
    }

    return Status::OK();
}

Status MySQLMetaImpl::DropAll() {
    try {
        ENGINE_LOG_DEBUG << "Drop all mysql meta";
        ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

        if (connectionPtr == nullptr) {
            return Status(DB_ERROR, "Failed to connect to database server");
        }

        Query dropTableQuery = connectionPtr->query();
        dropTableQuery << "DROP TABLE IF EXISTS " << TABLES_SCHEMA.name() << ", " << TABLEFILES_SCHEMA.name() << ";";

        ENGINE_LOG_DEBUG << "MySQLMetaImpl::DropAll: " << dropTableQuery.str();

        if (dropTableQuery.exec()) {
            return Status::OK();
        } else {
            return HandleException("QUERY ERROR WHEN DROPPING ALL", dropTableQuery.error());
        }
    } catch (std::exception &e) {
        return HandleException("GENERAL ERROR WHEN DROPPING ALL", e.what());
    }
}

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
