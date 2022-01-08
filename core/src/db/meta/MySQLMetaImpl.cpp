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

#include "db/meta/MySQLMetaImpl.h"

#include <fiu-local.h>
#include <mysql++/mysql++.h>
#include <string.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "MetaConsts.h"
#include "db/IDGenerator.h"
#include "db/Utils.h"
#include "db/meta/MetaSchema.h"
#include "metrics/Metrics.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace engine {
namespace meta {

namespace {

constexpr uint64_t SQL_BATCH_SIZE = 50;

template <typename T>
void
DistributeBatch(const T& id_array, std::vector<std::vector<std::string>>& id_groups) {
    std::vector<std::string> temp_group;
    //    constexpr uint64_t SQL_BATCH_SIZE = 50; // duplicate variable
    for (auto& id : id_array) {
        temp_group.push_back(id);
        if (temp_group.size() >= SQL_BATCH_SIZE) {
            id_groups.emplace_back(temp_group);
            temp_group.clear();
        }
    }

    if (!temp_group.empty()) {
        id_groups.emplace_back(temp_group);
    }
}

Status
HandleException(const std::string& desc, const char* what = nullptr) {
    if (what == nullptr) {
        LOG_ENGINE_ERROR_ << desc;
        return Status(DB_META_TRANSACTION_FAILED, desc);
    }

    std::string msg = desc + ":" + what;
    LOG_ENGINE_ERROR_ << msg;
    return Status(DB_META_TRANSACTION_FAILED, msg);
}

// Environment schema
static const MetaSchema ENVIRONMENT_SCHEMA(META_ENVIRONMENT, {
                                                                 MetaField("global_lsn", "BIGINT", "NOT NULL"),
                                                             });

// Tables schema
static const MetaSchema TABLES_SCHEMA(META_TABLES, {
                                                       MetaField("id", "BIGINT", "PRIMARY KEY AUTO_INCREMENT"),
                                                       MetaField("table_id", "VARCHAR(255)", "UNIQUE NOT NULL"),
                                                       MetaField("state", "INT", "NOT NULL"),
                                                       MetaField("dimension", "SMALLINT", "NOT NULL"),
                                                       MetaField("created_on", "BIGINT", "NOT NULL"),
                                                       MetaField("flag", "BIGINT", "DEFAULT 0 NOT NULL"),
                                                       MetaField("index_file_size", "BIGINT", "DEFAULT 1024 NOT NULL"),
                                                       MetaField("engine_type", "INT", "DEFAULT 1 NOT NULL"),
                                                       MetaField("index_params", "VARCHAR(512)", "NOT NULL"),
                                                       MetaField("metric_type", "INT", "DEFAULT 1 NOT NULL"),
                                                       MetaField("owner_table", "VARCHAR(255)", "NOT NULL"),
                                                       MetaField("partition_tag", "VARCHAR(255)", "NOT NULL"),
                                                       MetaField("version", "VARCHAR(64)",
                                                                 std::string("DEFAULT '") + CURRENT_VERSION + "'"),
                                                       MetaField("flush_lsn", "BIGINT", "DEFAULT 0 NOT NULL"),
                                                   });

// TableFiles schema
static const MetaSchema TABLEFILES_SCHEMA(META_TABLEFILES, {
                                                               MetaField("id", "BIGINT", "PRIMARY KEY AUTO_INCREMENT"),
                                                               MetaField("table_id", "VARCHAR(255)", "NOT NULL"),
                                                               MetaField("segment_id", "VARCHAR(255)", "NOT NULL"),
                                                               MetaField("engine_type", "INT", "DEFAULT 1 NOT NULL"),
                                                               MetaField("file_id", "VARCHAR(255)", "NOT NULL"),
                                                               MetaField("file_type", "INT", "DEFAULT 0 NOT NULL"),
                                                               MetaField("file_size", "BIGINT", "DEFAULT 0 NOT NULL"),
                                                               MetaField("row_count", "BIGINT", "DEFAULT 0 NOT NULL"),
                                                               MetaField("updated_time", "BIGINT", "NOT NULL"),
                                                               MetaField("created_on", "BIGINT", "NOT NULL"),
                                                               MetaField("date", "INT", "DEFAULT -1 NOT NULL"),
                                                               MetaField("flush_lsn", "BIGINT", "DEFAULT 0 NOT NULL"),
                                                           });

// Fields schema
static const MetaSchema FIELDS_SCHEMA(META_FIELDS, {
                                                       MetaField("collection_id", "VARCHAR(255)", "NOT NULL"),
                                                       MetaField("field_name", "VARCHAR(255)", "NOT NULL"),
                                                       MetaField("field_type", "INT", "DEFAULT 0 NOT NULL"),
                                                       MetaField("field_params", "VARCHAR(255)", "NOT NULL"),
                                                   });

}  // namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
MySQLMetaImpl::MySQLMetaImpl(const DBMetaOptions& options, const int& mode) : options_(options), mode_(mode) {
    Initialize();
}

MySQLMetaImpl::~MySQLMetaImpl() {
}

Status
MySQLMetaImpl::NextCollectionId(std::string& collection_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    ss << id_generator.GetNextIDNumber();
    collection_id = ss.str();
    return Status::OK();
}

Status
MySQLMetaImpl::NextFileId(std::string& file_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;

    SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    ss << id_generator.GetNextIDNumber();
    file_id = ss.str();
    return Status::OK();
}

void
MySQLMetaImpl::ValidateMetaSchema() {
    if (mysql_connection_pool_ == nullptr) {
        throw Exception(DB_ERROR, "MySQL connection pool is invalid");
        return;
    }

    mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
    if (connectionPtr == nullptr) {
        throw Exception(DB_ERROR, "Can't construct MySQL connection");
        return;
    }

    auto validate_func = [&](const MetaSchema& schema) {
        fiu_return_on("MySQLMetaImpl.ValidateMetaSchema.fail_validate", false);
        mysqlpp::Query query_statement = connectionPtr->query();
        query_statement << "DESC " << schema.name() << ";";

        MetaFields exist_fields;

        try {
            mysqlpp::StoreQueryResult res = query_statement.store();
            for (size_t i = 0; i < res.num_rows(); ++i) {
                const mysqlpp::Row& row = res[i];
                std::string name, type;
                row["Field"].to_string(name);
                row["Type"].to_string(type);

                exist_fields.push_back(MetaField(name, type, ""));
            }
        } catch (std::exception& e) {
            LOG_ENGINE_DEBUG_ << "Meta collection '" << schema.name() << "' not exist and will be created";
        }

        if (exist_fields.empty()) {
            return true;
        }

        return schema.IsEqual(exist_fields);
    };

    // verify Environment
    if (!validate_func(ENVIRONMENT_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Environment schema is created by Milvus old version");
    }

    // verify Tables
    if (!validate_func(TABLES_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }

    // verify TableFiles
    if (!validate_func(TABLEFILES_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta TableFiles schema is created by Milvus old version");
    }

    // verify Fields
    if (!validate_func(FIELDS_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Fields schema is created by milvus old version");
    }
}

Status
MySQLMetaImpl::Initialize() {
    // step 1: create db root path
    if (!boost::filesystem::is_directory(options_.path_)) {
        auto ret = boost::filesystem::create_directory(options_.path_);
        fiu_do_on("MySQLMetaImpl.Initialize.fail_create_directory", ret = false);
        if (!ret) {
            std::string msg = "Failed to create db directory " + options_.path_;
            LOG_ENGINE_ERROR_ << msg;
            throw Exception(DB_META_TRANSACTION_FAILED, msg);
        }
    }

    std::string uri = options_.backend_uri_;

    // step 2: parse and check meta uri
    utils::MetaUriInfo uri_info;
    auto status = utils::ParseMetaUri(uri, uri_info);
    if (!status.ok()) {
        std::string msg = "Wrong URI format: " + uri;
        LOG_ENGINE_ERROR_ << msg;
        throw Exception(DB_INVALID_META_URI, msg);
    }

    if (strcasecmp(uri_info.dialect_.c_str(), "mysql") != 0) {
        std::string msg = "URI's dialect is not MySQL";
        LOG_ENGINE_ERROR_ << msg;
        throw Exception(DB_INVALID_META_URI, msg);
    }

    // step 3: connect mysql
    unsigned int thread_hint = std::thread::hardware_concurrency();
    int max_pool_size = (thread_hint > 8) ? static_cast<int>(thread_hint) : 8;
    int port = 0;
    if (!uri_info.port_.empty()) {
        port = std::stoi(uri_info.port_);
    }

    mysql_connection_pool_ = std::make_shared<MySQLConnectionPool>(
        uri_info.db_name_, uri_info.username_, uri_info.password_, uri_info.host_, options_.ssl_ca_, options_.ssl_key_,
        options_.ssl_cert_, port, max_pool_size);
    LOG_ENGINE_DEBUG_ << "MySQL connection pool: maximum pool size = " << std::to_string(max_pool_size);

    // step 4: validate to avoid open old version schema
    ValidateMetaSchema();

    // step 5: clean shadow files
    if (mode_ != DBOptions::MODE::CLUSTER_READONLY) {
        CleanUpShadowFiles();
    }

    // step 6: try connect mysql server
    mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

    if (connectionPtr == nullptr) {
        std::string msg = "Failed to connect MySQL meta server: " + uri;
        LOG_ENGINE_ERROR_ << msg;
        throw Exception(DB_INVALID_META_URI, msg);
    }

    bool is_thread_aware = connectionPtr->thread_aware();
    fiu_do_on("MySQLMetaImpl.Initialize.is_thread_aware", is_thread_aware = false);
    if (!is_thread_aware) {
        std::string msg =
            "Failed to initialize MySQL meta backend: MySQL client component wasn't built with thread awareness";
        LOG_ENGINE_ERROR_ << msg;
        throw Exception(DB_INVALID_META_URI, msg);
    }

    auto create_meta_table = [&](const MetaSchema& schema) {
        mysqlpp::Query query_statement = connectionPtr->query();
        query_statement << "SHOW TABLES LIKE '" << schema.name() << "';";

        bool exist = false;
        try {
            mysqlpp::StoreQueryResult res = query_statement.store();
            exist = (res.num_rows() > 0);
        } catch (std::exception& e) {
        }

        fiu_do_on("MySQLMetaImpl.Initialize.meta_schema_exist", exist = false);
        if (!exist) {
            mysqlpp::Query InitializeQuery = connectionPtr->query();
            InitializeQuery << "CREATE TABLE IF NOT EXISTS " << schema.name() << " (" << schema.ToString() + ");";

            LOG_ENGINE_DEBUG_ << "Initialize: " << InitializeQuery.str();

            bool initialize_query_exec = InitializeQuery.exec();
            fiu_do_on("MySQLMetaImpl.Initialize.fail_create_meta_schema", initialize_query_exec = false);
            if (!initialize_query_exec) {
                std::string msg = "Failed to create meta table" + schema.name() + " in MySQL";
                LOG_ENGINE_ERROR_ << msg;
                throw Exception(DB_META_TRANSACTION_FAILED, msg);
            }
        }
    };

    // step 7: create meta collection Tables
    create_meta_table(TABLES_SCHEMA);

    // step 8: create meta collection TableFiles
    create_meta_table(TABLEFILES_SCHEMA);

    // step 9: create meta table Environment
    create_meta_table(ENVIRONMENT_SCHEMA);

    // step 10: create meta table Field
    create_meta_table(FIELDS_SCHEMA);

    return Status::OK();
}

Status
MySQLMetaImpl::CreateCollection(CollectionSchema& collection_schema) {
    try {
        server::MetricCollector metric;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.CreateCollection.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.CreateCollection.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();

            if (collection_schema.collection_id_.empty()) {
                NextCollectionId(collection_schema.collection_id_);
            } else {
                statement << "SELECT state FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                          << collection_schema.collection_id_ << ";";

                LOG_ENGINE_DEBUG_ << "CreateCollection: " << statement.str();

                mysqlpp::StoreQueryResult res = statement.store();

                if (res.num_rows() == 1) {
                    int state = res[0]["state"];
                    fiu_do_on("MySQLMetaImpl.CreateCollection.schema_TO_DELETE", state = CollectionSchema::TO_DELETE);
                    if (CollectionSchema::TO_DELETE == state) {
                        return Status(DB_ERROR,
                                      "Collection already exists and it is in delete state, please wait a second");
                    } else {
                        return Status(DB_ALREADY_EXIST, "Collection already exists");
                    }
                }
            }

            collection_schema.id_ = -1;
            collection_schema.created_on_ = utils::GetMicroSecTimeStamp();

            std::string id = "NULL";  // auto-increment
            std::string& collection_id = collection_schema.collection_id_;
            std::string state = std::to_string(collection_schema.state_);
            std::string dimension = std::to_string(collection_schema.dimension_);
            std::string created_on = std::to_string(collection_schema.created_on_);
            std::string flag = std::to_string(collection_schema.flag_);
            std::string index_file_size = std::to_string(collection_schema.index_file_size_);
            std::string engine_type = std::to_string(collection_schema.engine_type_);
            std::string& index_params = collection_schema.index_params_;
            std::string metric_type = std::to_string(collection_schema.metric_type_);
            std::string& owner_collection = collection_schema.owner_collection_;
            std::string& partition_tag = collection_schema.partition_tag_;
            std::string& version = collection_schema.version_;
            std::string flush_lsn = std::to_string(collection_schema.flush_lsn_);

            statement << "INSERT INTO " << META_TABLES << " VALUES(" << id << ", " << mysqlpp::quote << collection_id
                      << ", " << state << ", " << dimension << ", " << created_on << ", " << flag << ", "
                      << index_file_size << ", " << engine_type << ", " << mysqlpp::quote << index_params << ", "
                      << metric_type << ", " << mysqlpp::quote << owner_collection << ", " << mysqlpp::quote
                      << partition_tag << ", " << mysqlpp::quote << version << ", " << flush_lsn << ");";

            LOG_ENGINE_DEBUG_ << "CreateCollection: " << statement.str();

            if (mysqlpp::SimpleResult res = statement.execute()) {
                collection_schema.id_ = res.insert_id();  // Might need to use SELECT LAST_INSERT_ID()?

                // Consume all results to avoid "Commands out of sync" error
            } else {
                return HandleException("Failed to create collection", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully create collection: " << collection_schema.collection_id_;
        return utils::CreateCollectionPath(options_, collection_schema.collection_id_);
    } catch (std::exception& e) {
        return HandleException("Failed to create collection", e.what());
    }
}

Status
MySQLMetaImpl::DescribeCollection(CollectionSchema& collection_schema) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.DescribeCollection.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.DescribeCollection.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, state, dimension, created_on, flag, index_file_size, engine_type, index_params"
                      << " , metric_type ,owner_table, partition_tag, version, flush_lsn"
                      << " FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                      << collection_schema.collection_id_ << " AND state <> "
                      << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "DescribeCollection: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (res.num_rows() == 1) {
            const mysqlpp::Row& resRow = res[0];
            collection_schema.id_ = resRow["id"];  // implicit conversion
            collection_schema.state_ = resRow["state"];
            collection_schema.dimension_ = resRow["dimension"];
            collection_schema.created_on_ = resRow["created_on"];
            collection_schema.flag_ = resRow["flag"];
            collection_schema.index_file_size_ = resRow["index_file_size"];
            collection_schema.engine_type_ = resRow["engine_type"];
            resRow["index_params"].to_string(collection_schema.index_params_);
            collection_schema.metric_type_ = resRow["metric_type"];
            resRow["owner_table"].to_string(collection_schema.owner_collection_);
            resRow["partition_tag"].to_string(collection_schema.partition_tag_);
            resRow["version"].to_string(collection_schema.version_);
            collection_schema.flush_lsn_ = resRow["flush_lsn"];
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_schema.collection_id_ + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Failed to describe collection", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::HasCollection(const std::string& collection_id, bool& has_or_not, bool is_root) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.HasCollection.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.HasCollection.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query HasCollectionQuery = connectionPtr->query();
            // since collection_id is a unique column we just need to check whether it exists or not
            if (is_root) {
                HasCollectionQuery << "SELECT id FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                                   << collection_id << " AND state <> " << std::to_string(CollectionSchema::TO_DELETE)
                                   << " AND owner_table = " << mysqlpp::quote << ""
                                   << ";";
            } else {
                HasCollectionQuery << "SELECT id FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                                   << collection_id << " AND state <> " << std::to_string(CollectionSchema::TO_DELETE)
                                   << ";";
            }

            LOG_ENGINE_DEBUG_ << "HasCollection: " << HasCollectionQuery.str();

            res = HasCollectionQuery.store();
        }  // Scoped Connection

        has_or_not = (res.num_rows() > 0);
    } catch (std::exception& e) {
        return HandleException("Failed to check collection existence", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::AllCollections(std::vector<CollectionSchema>& collection_schema_array, bool is_root) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.AllCollection.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.AllCollection.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id, dimension, engine_type, index_params, index_file_size, metric_type"
                      << " ,owner_table, partition_tag, version, flush_lsn"
                      << " FROM " << META_TABLES << " WHERE state <> " << std::to_string(CollectionSchema::TO_DELETE);
            if (is_root) {
                statement << " AND owner_table = \"\";";
            } else {
                statement << ";";
            }

            LOG_ENGINE_DEBUG_ << "AllCollections: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        for (auto& resRow : res) {
            CollectionSchema collection_schema;
            collection_schema.id_ = resRow["id"];  // implicit conversion
            resRow["table_id"].to_string(collection_schema.collection_id_);
            collection_schema.dimension_ = resRow["dimension"];
            collection_schema.index_file_size_ = resRow["index_file_size"];
            collection_schema.engine_type_ = resRow["engine_type"];
            resRow["index_params"].to_string(collection_schema.index_params_);
            collection_schema.metric_type_ = resRow["metric_type"];
            resRow["owner_table"].to_string(collection_schema.owner_collection_);
            resRow["partition_tag"].to_string(collection_schema.partition_tag_);
            resRow["version"].to_string(collection_schema.version_);
            collection_schema.flush_lsn_ = resRow["flush_lsn"];

            collection_schema_array.emplace_back(collection_schema);
        }
    } catch (std::exception& e) {
        return HandleException("Failed to get all collections", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::DropCollections(const std::vector<std::string>& collection_id_array) {
    try {
        // distribute id array to batches
        std::vector<std::vector<std::string>> id_groups;
        DistributeBatch(collection_id_array, id_groups);

        server::MetricCollector metric;

        for (auto group : id_groups) {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.DropCollection.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.DropCollection.throw_exception", throw std::exception(););

            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            // soft delete collection
            mysqlpp::Query statement = connectionPtr->query();
            //
            statement << "UPDATE " << META_TABLES << " SET state = " << std::to_string(CollectionSchema::TO_DELETE)
                      << " WHERE table_id in(";
            for (size_t i = 0; i < group.size(); i++) {
                statement << mysqlpp::quote << group[i];
                if (i != group.size() - 1) {
                    statement << ",";
                }
            }
            statement << ")";

            LOG_ENGINE_DEBUG_ << "DropCollections: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to drop collections", statement.error());
            }
        }  // Scoped Connection

        auto status = DeleteCollectionFiles(collection_id_array);
        LOG_ENGINE_DEBUG_ << "Successfully delete collections";
        return status;
    } catch (std::exception& e) {
        return HandleException("Failed to drop collection", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::DeleteCollectionFiles(const std::vector<std::string>& collection_id_array) {
    try {
        // distribute id array to batches
        std::vector<std::vector<std::string>> id_groups;
        DistributeBatch(collection_id_array, id_groups);

        server::MetricCollector metric;
        for (auto group : id_groups) {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.DeleteCollectionFiles.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.DeleteCollectionFiles.throw_exception", throw std::exception(););

            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            // soft delete collection files
            mysqlpp::Query statement = connectionPtr->query();
            //
            statement << "UPDATE " << META_TABLEFILES << " SET file_type = " << std::to_string(SegmentSchema::TO_DELETE)
                      << " ,updated_time = " << std::to_string(utils::GetMicroSecTimeStamp()) << " WHERE table_id in (";
            for (size_t i = 0; i < group.size(); i++) {
                statement << mysqlpp::quote << group[i];
                if (i != group.size() - 1) {
                    statement << ",";
                }
            }
            statement << ") AND file_type <> " << std::to_string(SegmentSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "DeleteCollectionFiles: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to delete colletion files", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully delete collection files";
    } catch (std::exception& e) {
        return HandleException("Failed to delete colletion files", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::CreateCollectionFile(SegmentSchema& file_schema) {
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

        std::string id = "NULL";  // auto-increment
        std::string collection_id = file_schema.collection_id_;
        std::string segment_id = file_schema.segment_id_;
        std::string engine_type = std::to_string(file_schema.engine_type_);
        std::string file_id = file_schema.file_id_;
        std::string file_type = std::to_string(file_schema.file_type_);
        std::string file_size = std::to_string(file_schema.file_size_);
        std::string row_count = std::to_string(file_schema.row_count_);
        std::string updated_time = std::to_string(file_schema.updated_time_);
        std::string created_on = std::to_string(file_schema.created_on_);
        std::string date = std::to_string(file_schema.date_);
        std::string flush_lsn = std::to_string(file_schema.flush_lsn_);

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.CreateCollectionFiles.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.CreateCollectionFiles.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();

            statement << "INSERT INTO " << META_TABLEFILES << " VALUES(" << id << ", " << mysqlpp::quote
                      << collection_id << ", " << mysqlpp::quote << segment_id << ", " << engine_type << ", "
                      << mysqlpp::quote << file_id << ", " << file_type << ", " << file_size << ", " << row_count
                      << ", " << updated_time << ", " << created_on << ", " << date << ", " << flush_lsn << ");";

            LOG_ENGINE_DEBUG_ << "CreateCollectionFile: " << statement.str();

            if (mysqlpp::SimpleResult res = statement.execute()) {
                file_schema.id_ = res.insert_id();  // Might need to use SELECT LAST_INSERT_ID()?

                // Consume all results to avoid "Commands out of sync" error
            } else {
                return HandleException("Failed to create collection file", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully create collection file, file id = " << file_schema.file_id_;
        return utils::CreateCollectionFilePath(options_, file_schema);
    } catch (std::exception& e) {
        return HandleException("Failed to create collection file", e.what());
    }
}

Status
MySQLMetaImpl::GetCollectionFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                                  FilesHolder& files_holder) {
    if (ids.empty()) {
        return Status::OK();
    }

    std::stringstream idSS;
    for (auto& id : ids) {
        idSS << "id = " << std::to_string(id) << " OR ";
    }
    std::string idStr = idSS.str();
    idStr = idStr.substr(0, idStr.size() - 4);  // remove the last " OR "

    try {
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.GetCollectionFiles.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.GetCollectionFiles.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement
                << "SELECT id, segment_id, engine_type, file_id, file_type, file_size, row_count, date, created_on"
                << " FROM " << META_TABLEFILES << " WHERE table_id = " << mysqlpp::quote << collection_id << " AND ("
                << idStr << ")"
                << " AND file_type <> " << std::to_string(SegmentSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "GetCollectionFiles: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        DescribeCollection(collection_schema);

        Status ret;
        for (auto& resRow : res) {
            SegmentSchema file_schema;
            file_schema.id_ = resRow["id"];
            file_schema.collection_id_ = collection_id;
            resRow["segment_id"].to_string(file_schema.segment_id_);
            file_schema.index_file_size_ = collection_schema.index_file_size_;
            file_schema.engine_type_ = resRow["engine_type"];
            file_schema.index_params_ = collection_schema.index_params_;
            file_schema.metric_type_ = collection_schema.metric_type_;
            resRow["file_id"].to_string(file_schema.file_id_);
            file_schema.file_type_ = resRow["file_type"];
            file_schema.file_size_ = resRow["file_size"];
            file_schema.row_count_ = resRow["row_count"];
            file_schema.date_ = resRow["date"];
            file_schema.created_on_ = resRow["created_on"];
            file_schema.dimension_ = collection_schema.dimension_;

            utils::GetCollectionFilePath(options_, file_schema);
            files_holder.MarkFile(file_schema);
        }

        LOG_ENGINE_DEBUG_ << "Get " << res.size() << " files by id from collection " << collection_id;
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get collection files", e.what());
    }
}

Status
MySQLMetaImpl::GetCollectionFilesBySegmentId(const std::string& segment_id, FilesHolder& files_holder) {
    try {
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id, segment_id, engine_type, file_id, file_type, file_size, "
                      << "row_count, date, created_on"
                      << " FROM " << META_TABLEFILES << " WHERE segment_id = " << mysqlpp::quote << segment_id
                      << " AND file_type <> " << std::to_string(SegmentSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "GetCollectionFilesBySegmentId: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (!res.empty()) {
            CollectionSchema collection_schema;
            res[0]["table_id"].to_string(collection_schema.collection_id_);
            auto status = DescribeCollection(collection_schema);
            if (!status.ok()) {
                return status;
            }

            for (auto& resRow : res) {
                SegmentSchema file_schema;
                file_schema.id_ = resRow["id"];
                file_schema.collection_id_ = collection_schema.collection_id_;
                resRow["segment_id"].to_string(file_schema.segment_id_);
                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.engine_type_ = resRow["engine_type"];
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;
                resRow["file_id"].to_string(file_schema.file_id_);
                file_schema.file_type_ = resRow["file_type"];
                file_schema.file_size_ = resRow["file_size"];
                file_schema.row_count_ = resRow["row_count"];
                file_schema.date_ = resRow["date"];
                file_schema.created_on_ = resRow["created_on"];
                file_schema.dimension_ = collection_schema.dimension_;

                utils::GetCollectionFilePath(options_, file_schema);
                files_holder.MarkFile(file_schema);
            }
        }

        LOG_ENGINE_DEBUG_ << "Get " << res.size() << " files by segment id " << segment_id;
        return Status::OK();
    } catch (std::exception& e) {
        return HandleException("Failed to get collection files by segment id", e.what());
    }
}

Status
MySQLMetaImpl::UpdateCollectionIndex(const std::string& collection_id, const CollectionIndex& index) {
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionIndex.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionIndex.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, state, dimension, created_on"
                      << " FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote << collection_id
                      << " AND state <> " << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "UpdateCollectionIndex: " << statement.str();

            mysqlpp::StoreQueryResult res = statement.store();

            if (res.num_rows() == 1) {
                const mysqlpp::Row& resRow = res[0];

                size_t id = resRow["id"];
                int32_t state = resRow["state"];
                uint16_t dimension = resRow["dimension"];
                int64_t created_on = resRow["created_on"];

                statement << "UPDATE " << META_TABLES << " SET id = " << id << " ,state = " << state
                          << " ,dimension = " << dimension << " ,created_on = " << created_on
                          << " ,engine_type = " << index.engine_type_ << " ,index_params = " << mysqlpp::quote
                          << index.extra_params_.dump() << " ,metric_type = " << index.metric_type_
                          << " WHERE table_id = " << mysqlpp::quote << collection_id << ";";

                LOG_ENGINE_DEBUG_ << "UpdateCollectionIndex: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to update collection index", statement.error());
                }
            } else {
                return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully update collection index for " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Failed to update collection index", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::UpdateCollectionFlag(const std::string& collection_id, int64_t flag) {
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionFlag.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionFlag.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "UPDATE " << META_TABLES << " SET flag = " << flag << " WHERE table_id = " << mysqlpp::quote
                      << collection_id << ";";

            LOG_ENGINE_DEBUG_ << "UpdateCollectionFlag: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to update collection flag", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully update collection flag for " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Failed to update collection flag", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::UpdateCollectionFlushLSN(const std::string& collection_id, uint64_t flush_lsn) {
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "UPDATE " << META_TABLES << " SET flush_lsn = " << flush_lsn
                      << " WHERE table_id = " << mysqlpp::quote << collection_id << ";";

            LOG_ENGINE_DEBUG_ << "UpdateCollectionFlushLSN: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to update collection lsn", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully update collection flush_lsn for " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Failed to update collection lsn", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::GetCollectionFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) {
    try {
        server::MetricCollector metric;

        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT flush_lsn FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                      << collection_id << ";";

            LOG_ENGINE_DEBUG_ << "GetCollectionFlushLSN: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (!res.empty()) {
            flush_lsn = res[0]["flush_lsn"];
        }
    } catch (std::exception& e) {
        return HandleException("Failed to get collection lsn", e.what());
    }

    return Status::OK();
}

// ZR: this function assumes all fields in file_schema have value
Status
MySQLMetaImpl::UpdateCollectionFile(SegmentSchema& file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();

    try {
        server::MetricCollector metric;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionFile.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionFile.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();

            // if the collection has been deleted, just mark the collection file as TO_DELETE
            // clean thread will delete the file later
            statement << "SELECT state FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                      << file_schema.collection_id_ << ";";

            LOG_ENGINE_DEBUG_ << "UpdateCollectionFile: " << statement.str();

            mysqlpp::StoreQueryResult res = statement.store();

            if (res.num_rows() == 1) {
                int state = res[0]["state"];
                if (state == CollectionSchema::TO_DELETE) {
                    file_schema.file_type_ = SegmentSchema::TO_DELETE;
                }
            } else {
                file_schema.file_type_ = SegmentSchema::TO_DELETE;
            }

            std::string id = std::to_string(file_schema.id_);
            std::string collection_id = file_schema.collection_id_;
            std::string engine_type = std::to_string(file_schema.engine_type_);
            std::string file_id = file_schema.file_id_;
            std::string file_type = std::to_string(file_schema.file_type_);
            std::string file_size = std::to_string(file_schema.file_size_);
            std::string row_count = std::to_string(file_schema.row_count_);
            std::string updated_time = std::to_string(file_schema.updated_time_);
            std::string created_on = std::to_string(file_schema.created_on_);
            std::string date = std::to_string(file_schema.date_);

            statement << "UPDATE " << META_TABLEFILES << " SET table_id = " << mysqlpp::quote << collection_id
                      << " ,engine_type = " << engine_type << " ,file_id = " << mysqlpp::quote << file_id
                      << " ,file_type = " << file_type << " ,file_size = " << file_size << " ,row_count = " << row_count
                      << " ,updated_time = " << updated_time << " ,created_on = " << created_on << " ,date = " << date
                      << " WHERE id = " << id << ";";

            LOG_ENGINE_DEBUG_ << "UpdateCollectionFile: " << statement.str();

            if (!statement.exec()) {
                LOG_ENGINE_DEBUG_ << "collection_id= " << file_schema.collection_id_
                                  << " file_id=" << file_schema.file_id_;
                return HandleException("Failed to update collection file", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Update single collection file, file id: " << file_schema.file_id_;
    } catch (std::exception& e) {
        return HandleException("Failed to update collection file", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::UpdateCollectionFilesToIndex(const std::string& collection_id) {
    try {
        mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

        bool is_null_connection = (connectionPtr == nullptr);
        fiu_do_on("MySQLMetaImpl.UpdateCollectionFilesToIndex.null_connection", is_null_connection = true);
        fiu_do_on("MySQLMetaImpl.UpdateCollectionFilesToIndex.throw_exception", throw std::exception(););
        if (is_null_connection) {
            return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
        }

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(meta_mutex_);

        mysqlpp::Query statement = connectionPtr->query();

        statement << "UPDATE " << META_TABLEFILES << " SET file_type = " << std::to_string(SegmentSchema::TO_INDEX)
                  << " WHERE table_id = " << mysqlpp::quote << collection_id
                  << " AND row_count >= " << std::to_string(meta::BUILD_INDEX_THRESHOLD)
                  << " AND file_type = " << std::to_string(SegmentSchema::RAW) << ";";

        LOG_ENGINE_DEBUG_ << "UpdateCollectionFilesToIndex: " << statement.str();

        if (!statement.exec()) {
            return HandleException("Failed to update collection files to index", statement.error());
        }

        LOG_ENGINE_DEBUG_ << "Update files to to_index for " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Failed to update collection files to index", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::UpdateCollectionFiles(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionFiles.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.UpdateCollectionFiles.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();

            std::map<std::string, bool> has_collections;
            for (auto& file_schema : files) {
                if (has_collections.find(file_schema.collection_id_) != has_collections.end()) {
                    continue;
                }

                statement << "SELECT EXISTS"
                          << " (SELECT 1 FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote
                          << file_schema.collection_id_ << " AND state <> "
                          << std::to_string(CollectionSchema::TO_DELETE) << ")"
                          << " AS " << mysqlpp::quote << "check"
                          << ";";

                LOG_ENGINE_DEBUG_ << "UpdateCollectionFiles: " << statement.str();

                mysqlpp::StoreQueryResult res = statement.store();

                int check = res[0]["check"];
                has_collections[file_schema.collection_id_] = (check == 1);
            }

            for (auto& file_schema : files) {
                if (!has_collections[file_schema.collection_id_]) {
                    file_schema.file_type_ = SegmentSchema::TO_DELETE;
                }
                file_schema.updated_time_ = utils::GetMicroSecTimeStamp();

                std::string id = std::to_string(file_schema.id_);
                std::string& collection_id = file_schema.collection_id_;
                std::string engine_type = std::to_string(file_schema.engine_type_);
                std::string& file_id = file_schema.file_id_;
                std::string file_type = std::to_string(file_schema.file_type_);
                std::string file_size = std::to_string(file_schema.file_size_);
                std::string row_count = std::to_string(file_schema.row_count_);
                std::string updated_time = std::to_string(file_schema.updated_time_);
                std::string created_on = std::to_string(file_schema.created_on_);
                std::string date = std::to_string(file_schema.date_);

                statement << "UPDATE " << META_TABLEFILES << " SET table_id = " << mysqlpp::quote << collection_id
                          << " ,engine_type = " << engine_type << " ,file_id = " << mysqlpp::quote << file_id
                          << " ,file_type = " << file_type << " ,file_size = " << file_size
                          << " ,row_count = " << row_count << " ,updated_time = " << updated_time
                          << " ,created_on = " << created_on << " ,date = " << date << " WHERE id = " << id << ";";

                LOG_ENGINE_DEBUG_ << "UpdateCollectionFiles: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to update collection files", statement.error());
                }
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Update " << files.size() << " collection files";
    } catch (std::exception& e) {
        return HandleException("Failed to update collection files", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::UpdateCollectionFilesRowCount(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();

            for (auto& file : files) {
                std::string row_count = std::to_string(file.row_count_);
                std::string updated_time = std::to_string(utils::GetMicroSecTimeStamp());

                statement << "UPDATE " << META_TABLEFILES << " SET row_count = " << row_count
                          << " , updated_time = " << updated_time << " WHERE file_id = " << file.file_id_ << ";";

                LOG_ENGINE_DEBUG_ << "UpdateCollectionFilesRowCount: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to update collection files row count", statement.error());
                }

                LOG_ENGINE_DEBUG_ << "Update file " << file.file_id_ << " row count to " << file.row_count_;
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Update " << files.size() << " collection files";
    } catch (std::exception& e) {
        return HandleException("Failed to update collection files row count", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::DescribeCollectionIndex(const std::string& collection_id, CollectionIndex& index) {
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.DescribeCollectionIndex.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.DescribeCollectionIndex.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT engine_type, index_params, index_file_size, metric_type"
                      << " FROM " << META_TABLES << " WHERE table_id = " << mysqlpp::quote << collection_id
                      << " AND state <> " << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "DescribeCollectionIndex: " << statement.str();

            mysqlpp::StoreQueryResult res = statement.store();

            if (res.num_rows() == 1) {
                const mysqlpp::Row& resRow = res[0];

                index.engine_type_ = resRow["engine_type"];
                std::string str_index_params;
                resRow["index_params"].to_string(str_index_params);
                index.extra_params_ = milvus::json::parse(str_index_params);
                index.metric_type_ = resRow["metric_type"];
            } else {
                return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
            }
        }  // Scoped Connection
    } catch (std::exception& e) {
        return HandleException("Failed to describe collection index", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::DropCollectionIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.DropCollectionIndex.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.DropCollectionIndex.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();

            // soft delete index files
            statement << "UPDATE " << META_TABLEFILES << " SET file_type = " << std::to_string(SegmentSchema::TO_DELETE)
                      << " ,updated_time = " << utils::GetMicroSecTimeStamp() << " WHERE table_id = " << mysqlpp::quote
                      << collection_id << " AND file_type = " << std::to_string(SegmentSchema::INDEX) << ";";

            LOG_ENGINE_DEBUG_ << "DropCollectionIndex: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to drop collection index", statement.error());
            }

            // set all backup file to raw
            statement << "UPDATE " << META_TABLEFILES << " SET file_type = " << std::to_string(SegmentSchema::RAW)
                      << " ,updated_time = " << utils::GetMicroSecTimeStamp() << " WHERE table_id = " << mysqlpp::quote
                      << collection_id << " AND file_type = " << std::to_string(SegmentSchema::BACKUP) << ";";

            LOG_ENGINE_DEBUG_ << "DropCollectionIndex: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to drop collection index", statement.error());
            }

            // set collection index type to raw
            statement << "UPDATE " << META_TABLES << " SET engine_type = "
                      << " (CASE"
                      << " WHEN metric_type in (" << (int32_t)MetricType::HAMMING << " ,"
                      << (int32_t)MetricType::JACCARD << " ," << (int32_t)MetricType::TANIMOTO << ")"
                      << " THEN " << (int32_t)EngineType::FAISS_BIN_IDMAP << " ELSE "
                      << (int32_t)EngineType::FAISS_IDMAP << " END)"
                      << " , index_params = '{}'"
                      << " WHERE table_id = " << mysqlpp::quote << collection_id << ";";

            LOG_ENGINE_DEBUG_ << "DropCollectionIndex: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to drop collection index", statement.error());
            }
        }  // Scoped Connection

        LOG_ENGINE_DEBUG_ << "Successfully drop collection index for " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Failed to drop collection index", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::CreatePartition(const std::string& collection_id, const std::string& partition_name,
                               const std::string& tag, uint64_t lsn) {
    server::MetricCollector metric;

    CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_id;
    auto status = DescribeCollection(collection_schema);
    if (!status.ok()) {
        return status;
    }

    // not allow create partition under partition
    if (!collection_schema.owner_collection_.empty()) {
        return Status(DB_NOT_FOUND, "Nested partition is not allowed");
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
    fiu_do_on("MySQLMetaImpl.CreatePartition.aleady_exist", status = Status(DB_ALREADY_EXIST, ""));
    if (status.code() == DB_ALREADY_EXIST) {
        return Status(DB_ALREADY_EXIST, "Partition already exists");
    }

    return status;
}

Status
MySQLMetaImpl::HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;

        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT table_id FROM " << META_TABLES << " WHERE owner_table = " << mysqlpp::quote
                      << collection_id << " AND partition_tag = " << mysqlpp::quote << valid_tag << " AND state <> "
                      << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "HasPartition: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (res.num_rows() > 0) {
            has_or_not = true;
        } else {
            has_or_not = false;
        }
    } catch (std::exception& e) {
        return HandleException("Failed to lookup partition", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::DropPartition(const std::string& partition_name) {
    return DropCollections({partition_name});
}

Status
MySQLMetaImpl::ShowPartitions(const std::string& collection_id,
                              std::vector<meta::CollectionSchema>& partition_schema_array) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.ShowPartitions.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.ShowPartitions.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT table_id, id, state, dimension, created_on, flag, index_file_size,"
                      << " engine_type, index_params, metric_type, partition_tag, version, flush_lsn FROM "
                      << META_TABLES << " WHERE owner_table = " << mysqlpp::quote << collection_id << " AND state <> "
                      << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "ShowPartitions: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        for (auto& resRow : res) {
            meta::CollectionSchema partition_schema;
            resRow["table_id"].to_string(partition_schema.collection_id_);
            partition_schema.id_ = resRow["id"];  // implicit conversion
            partition_schema.state_ = resRow["state"];
            partition_schema.dimension_ = resRow["dimension"];
            partition_schema.created_on_ = resRow["created_on"];
            partition_schema.flag_ = resRow["flag"];
            partition_schema.index_file_size_ = resRow["index_file_size"];
            partition_schema.engine_type_ = resRow["engine_type"];
            resRow["index_params"].to_string(partition_schema.index_params_);
            partition_schema.metric_type_ = resRow["metric_type"];
            partition_schema.owner_collection_ = collection_id;
            resRow["partition_tag"].to_string(partition_schema.partition_tag_);
            resRow["version"].to_string(partition_schema.version_);
            partition_schema.flush_lsn_ = resRow["flush_lsn"];

            partition_schema_array.emplace_back(partition_schema);
        }
    } catch (std::exception& e) {
        return HandleException("Failed to show partitions", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::CountPartitions(const std::string& collection_id, int64_t& partition_count) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT count(*) FROM " << META_TABLES << " WHERE owner_table = " << mysqlpp::quote
                      << collection_id << " AND state <> " << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "CountPartitions: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        for (auto& resRow : res) {
            partition_count = resRow["count(*)"];
        }
    } catch (std::exception& e) {
        return HandleException("Failed to count partitions", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::GetPartitionName(const std::string& collection_id, const std::string& tag, std::string& partition_name) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;

        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.GetPartitionName.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.GetPartitionName.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT table_id FROM " << META_TABLES << " WHERE owner_table = " << mysqlpp::quote
                      << collection_id << " AND partition_tag = " << mysqlpp::quote << valid_tag << " AND state <> "
                      << std::to_string(CollectionSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "GetPartitionName: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (res.num_rows() > 0) {
            const mysqlpp::Row& resRow = res[0];
            resRow["table_id"].to_string(partition_name);
        } else {
            return Status(DB_NOT_FOUND, "Partition " + valid_tag + " of collection " + collection_id + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Failed to get partition name", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::FilesToSearch(const std::string& collection_id, FilesHolder& files_holder, 
                             bool is_all_search_file) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.FilesToSearch.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.FilesToSearch.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                      << " engine_type, created_on, updated_time"
                      << " FROM " << META_TABLEFILES << " WHERE table_id = " << mysqlpp::quote << collection_id;

            // End
            if (is_all_search_file) {
                statement << " AND"
                          << " (file_type = " << std::to_string(SegmentSchema::RAW)
                          << " OR file_type = " << std::to_string(SegmentSchema::TO_INDEX)
                          << " OR file_type = " << std::to_string(SegmentSchema::INDEX) << ");";
            } else {
                statement << " AND file_type = " << std::to_string(SegmentSchema::INDEX) << ";";
            }

            LOG_ENGINE_DEBUG_ << "FilesToSearch: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        Status ret;
        int64_t files_count = 0;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.id_ = resRow["id"];  // implicit conversion
            resRow["table_id"].to_string(collection_file.collection_id_);
            resRow["segment_id"].to_string(collection_file.segment_id_);
            resRow["file_id"].to_string(collection_file.file_id_);
            collection_file.file_type_ = resRow["file_type"];
            collection_file.file_size_ = resRow["file_size"];
            collection_file.row_count_ = resRow["row_count"];
            collection_file.date_ = resRow["date"];
            collection_file.engine_type_ = resRow["engine_type"];
            collection_file.created_on_ = resRow["created_on"];
            collection_file.updated_time_ = resRow["updated_time"];

            collection_file.dimension_ = collection_schema.dimension_;
            collection_file.index_file_size_ = collection_schema.index_file_size_;
            collection_file.index_params_ = collection_schema.index_params_;
            collection_file.metric_type_ = collection_schema.metric_type_;

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                ret = status;
                continue;
            }

            files_holder.MarkFile(collection_file);
            files_count++;
        }

        if (files_count == 0) {
            LOG_ENGINE_DEBUG_ << "No file to search for collection: " << collection_id;
        } else {
            LOG_ENGINE_DEBUG_ << "Collect " << files_count << " to-search files in collection " << collection_id;
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files to search", e.what());
    }
}

Status
MySQLMetaImpl::FilesToSearchEx(const std::string& root_collection, const std::set<std::string>& partition_id_array,
                               FilesHolder& files_holder, bool is_all_search_file) {
    try {
        server::MetricCollector metric;

        // get root collection information
        CollectionSchema collection_schema;
        collection_schema.collection_id_ = root_collection;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        // distribute id array to batches
        std::vector<std::vector<std::string>> id_groups;
        DistributeBatch(partition_id_array, id_groups);

        // perform query batch by batch
        int64_t files_count = 0;
        Status ret;
        for (auto group : id_groups) {
            mysqlpp::StoreQueryResult res;
            {
                mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

                bool is_null_connection = (connectionPtr == nullptr);
                fiu_do_on("MySQLMetaImpl.FilesToSearch.null_connection", is_null_connection = true);
                fiu_do_on("MySQLMetaImpl.FilesToSearch.throw_exception", throw std::exception(););
                if (is_null_connection) {
                    return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
                }

                // to ensure UpdateCollectionFiles to be a atomic operation
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                mysqlpp::Query statement = connectionPtr->query();
                statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                          << " engine_type, created_on, updated_time"
                          << " FROM " << META_TABLEFILES << " WHERE table_id in (";
                for (size_t i = 0; i < group.size(); i++) {
                    statement << mysqlpp::quote << group[i];
                    if (i != group.size() - 1) {
                        statement << ",";
                    }
                }
                statement << ")";

                // End
                if (is_all_search_file) {
                    statement << " AND"
                            << " (file_type = " << std::to_string(SegmentSchema::RAW)
                            << " OR file_type = " << std::to_string(SegmentSchema::TO_INDEX)
                            << " OR file_type = " << std::to_string(SegmentSchema::INDEX) << ");";
                } else {
                    statement << " AND file_type = " << std::to_string(SegmentSchema::INDEX) << ";";
                }

                LOG_ENGINE_DEBUG_ << "FilesToSearchEx: " << statement.str();

                res = statement.store();
            }  // Scoped Connection

            for (auto& resRow : res) {
                SegmentSchema collection_file;
                collection_file.id_ = resRow["id"];  // implicit conversion
                resRow["table_id"].to_string(collection_file.collection_id_);
                resRow["segment_id"].to_string(collection_file.segment_id_);
                resRow["file_id"].to_string(collection_file.file_id_);
                collection_file.file_type_ = resRow["file_type"];
                collection_file.file_size_ = resRow["file_size"];
                collection_file.row_count_ = resRow["row_count"];
                collection_file.date_ = resRow["date"];
                collection_file.engine_type_ = resRow["engine_type"];
                collection_file.created_on_ = resRow["created_on"];
                collection_file.updated_time_ = resRow["updated_time"];

                collection_file.dimension_ = collection_schema.dimension_;
                collection_file.index_file_size_ = collection_schema.index_file_size_;
                collection_file.index_params_ = collection_schema.index_params_;
                collection_file.metric_type_ = collection_schema.metric_type_;

                auto status = utils::GetCollectionFilePath(options_, collection_file);
                if (!status.ok()) {
                    ret = status;
                    continue;
                }

                files_holder.MarkFile(collection_file);
                files_count++;
            }
        }

        if (files_count == 0) {
            LOG_ENGINE_DEBUG_ << "No file to search for collection: " << root_collection;
        } else {
            LOG_ENGINE_DEBUG_ << "Collect " << files_count << " to-search files in collection " << root_collection;
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files to search", e.what());
    }
}

Status
MySQLMetaImpl::FilesToMerge(const std::string& collection_id, FilesHolder& files_holder) {
    try {
        server::MetricCollector metric;

        // check collection existence
        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.FilesToMerge.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.FilesToMerge.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                         " engine_type, created_on, updated_time"
                      << " FROM " << META_TABLEFILES << " WHERE table_id = " << mysqlpp::quote << collection_id
                      << " AND file_type = " << std::to_string(SegmentSchema::RAW) << " ORDER BY row_count DESC;";

            LOG_ENGINE_DEBUG_ << "FilesToMerge: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        Status ret;
        SegmentsSchema files;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.file_size_ = resRow["file_size"];
            if ((int64_t)(collection_file.file_size_) >= collection_schema.index_file_size_) {
                continue;  // skip large file
            }

            collection_file.id_ = resRow["id"];  // implicit conversion
            resRow["table_id"].to_string(collection_file.collection_id_);
            resRow["segment_id"].to_string(collection_file.segment_id_);
            resRow["file_id"].to_string(collection_file.file_id_);
            collection_file.file_type_ = resRow["file_type"];
            collection_file.file_size_ = resRow["file_size"];
            collection_file.row_count_ = resRow["row_count"];
            collection_file.date_ = resRow["date"];
            collection_file.engine_type_ = resRow["engine_type"];
            collection_file.created_on_ = resRow["created_on"];
            collection_file.updated_time_ = resRow["updated_time"];

            collection_file.dimension_ = collection_schema.dimension_;
            collection_file.index_file_size_ = collection_schema.index_file_size_;
            collection_file.index_params_ = collection_schema.index_params_;
            collection_file.metric_type_ = collection_schema.metric_type_;

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                ret = status;
                continue;
            }

            files.emplace_back(collection_file);
        }

        // no need to merge if files count  less than 2
        if (files.size() > 1) {
            LOG_ENGINE_DEBUG_ << "Collect " << files.size() << " to-merge files in collection " << collection_id;
            for (auto& file : files) {
                files_holder.MarkFile(file);
            }
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files to merge", e.what());
    }
}

Status
MySQLMetaImpl::FilesToIndex(FilesHolder& files_holder) {
    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.FilesToIndex.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.FilesToIndex.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                      << " engine_type, created_on, updated_time"
                      << " FROM " << META_TABLEFILES << " WHERE file_type = " << std::to_string(SegmentSchema::TO_INDEX)
                      << ";";

            // LOG_ENGINE_DEBUG_ << "FilesToIndex: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        Status ret;
        int64_t files_count = 0;
        std::map<std::string, CollectionSchema> groups;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.id_ = resRow["id"];  // implicit conversion
            resRow["table_id"].to_string(collection_file.collection_id_);
            resRow["segment_id"].to_string(collection_file.segment_id_);
            resRow["file_id"].to_string(collection_file.file_id_);
            collection_file.file_type_ = resRow["file_type"];
            collection_file.file_size_ = resRow["file_size"];
            collection_file.row_count_ = resRow["row_count"];
            collection_file.date_ = resRow["date"];
            collection_file.engine_type_ = resRow["engine_type"];
            collection_file.created_on_ = resRow["created_on"];
            collection_file.updated_time_ = resRow["updated_time"];

            auto groupItr = groups.find(collection_file.collection_id_);
            if (groupItr == groups.end()) {
                CollectionSchema collection_schema;
                collection_schema.collection_id_ = collection_file.collection_id_;
                auto status = DescribeCollection(collection_schema);
                if (!status.ok()) {
                    return status;
                }
                groups[collection_file.collection_id_] = collection_schema;
            }
            collection_file.dimension_ = groups[collection_file.collection_id_].dimension_;
            collection_file.index_file_size_ = groups[collection_file.collection_id_].index_file_size_;
            collection_file.index_params_ = groups[collection_file.collection_id_].index_params_;
            collection_file.metric_type_ = groups[collection_file.collection_id_].metric_type_;

            auto status = utils::GetCollectionFilePath(options_, collection_file);
            if (!status.ok()) {
                ret = status;
            }

            files_holder.MarkFile(collection_file);
            files_count++;
        }

        if (files_count > 0) {
            LOG_ENGINE_DEBUG_ << "Collect " << files_count << " to-index files";
        }
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files to index", e.what());
    }
}

Status
MySQLMetaImpl::FilesByType(const std::string& collection_id, const std::vector<int>& file_types,
                           FilesHolder& files_holder) {
    if (file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    Status ret = Status::OK();
    try {
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.FilesByType.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.FilesByType.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            std::string types;
            for (auto type : file_types) {
                if (!types.empty()) {
                    types += ",";
                }
                types += std::to_string(type);
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            // since collection_id is a unique column we just need to check whether it exists or not
            statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                      << " engine_type, created_on, updated_time"
                      << " FROM " << META_TABLEFILES << " WHERE table_id = " << mysqlpp::quote << collection_id
                      << " AND file_type in (" << types << ");";

            LOG_ENGINE_DEBUG_ << "FilesByType: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        if (res.num_rows() > 0) {
            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
            int to_index_count = 0, index_count = 0, backup_count = 0;
            for (auto& resRow : res) {
                SegmentSchema file_schema;
                file_schema.id_ = resRow["id"];
                file_schema.collection_id_ = collection_id;
                resRow["segment_id"].to_string(file_schema.segment_id_);
                resRow["file_id"].to_string(file_schema.file_id_);
                file_schema.file_type_ = resRow["file_type"];
                file_schema.file_size_ = resRow["file_size"];
                file_schema.row_count_ = resRow["row_count"];
                file_schema.date_ = resRow["date"];
                file_schema.engine_type_ = resRow["engine_type"];
                file_schema.created_on_ = resRow["created_on"];
                file_schema.updated_time_ = resRow["updated_time"];

                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;
                file_schema.dimension_ = collection_schema.dimension_;

                auto status = utils::GetCollectionFilePath(options_, file_schema);
                if (!status.ok()) {
                    ret = status;
                }

                files_holder.MarkFile(file_schema);

                int32_t file_type = resRow["file_type"];
                switch (file_type) {
                    case (int)SegmentSchema::RAW:
                        ++raw_count;
                        break;
                    case (int)SegmentSchema::NEW:
                        ++new_count;
                        break;
                    case (int)SegmentSchema::NEW_MERGE:
                        ++new_merge_count;
                        break;
                    case (int)SegmentSchema::NEW_INDEX:
                        ++new_index_count;
                        break;
                    case (int)SegmentSchema::TO_INDEX:
                        ++to_index_count;
                        break;
                    case (int)SegmentSchema::INDEX:
                        ++index_count;
                        break;
                    case (int)SegmentSchema::BACKUP:
                        ++backup_count;
                        break;
                    default:
                        break;
                }
            }

            std::string msg = "Get collection files by type.";
            for (int file_type : file_types) {
                switch (file_type) {
                    case (int)SegmentSchema::RAW:
                        msg = msg + " raw files:" + std::to_string(raw_count);
                        break;
                    case (int)SegmentSchema::NEW:
                        msg = msg + " new files:" + std::to_string(new_count);
                        break;
                    case (int)SegmentSchema::NEW_MERGE:
                        msg = msg + " new_merge files:" + std::to_string(new_merge_count);
                        break;
                    case (int)SegmentSchema::NEW_INDEX:
                        msg = msg + " new_index files:" + std::to_string(new_index_count);
                        break;
                    case (int)SegmentSchema::TO_INDEX:
                        msg = msg + " to_index files:" + std::to_string(to_index_count);
                        break;
                    case (int)SegmentSchema::INDEX:
                        msg = msg + " index files:" + std::to_string(index_count);
                        break;
                    case (int)SegmentSchema::BACKUP:
                        msg = msg + " backup files:" + std::to_string(backup_count);
                        break;
                    default:
                        break;
                }
            }
            LOG_ENGINE_DEBUG_ << msg;
        }
    } catch (std::exception& e) {
        return HandleException("Failed to get files by type", e.what());
    }

    return ret;
}

Status
MySQLMetaImpl::FilesByTypeEx(const std::vector<meta::CollectionSchema>& collections, const std::vector<int>& file_types,
                             FilesHolder& files_holder) {
    try {
        server::MetricCollector metric;

        // distribute id array to batches
        std::vector<std::vector<std::string>> id_groups;
        std::vector<std::string> temp_group;
        std::unordered_map<std::string, meta::CollectionSchema> map_collections;
        for (auto& collection : collections) {
            map_collections.insert(std::make_pair(collection.collection_id_, collection));
            temp_group.push_back(collection.collection_id_);
            if (temp_group.size() >= SQL_BATCH_SIZE) {
                id_groups.emplace_back(temp_group);
                temp_group.clear();
            }
        }

        if (!temp_group.empty()) {
            id_groups.emplace_back(temp_group);
        }

        // perform query batch by batch
        Status ret;
        int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
        int to_index_count = 0, index_count = 0, backup_count = 0;
        for (auto group : id_groups) {
            mysqlpp::StoreQueryResult res;
            {
                mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

                bool is_null_connection = (connectionPtr == nullptr);
                fiu_do_on("MySQLMetaImpl.FilesByTypeEx.null_connection", is_null_connection = true);
                fiu_do_on("MySQLMetaImpl.FilesByTypeEx.throw_exception", throw std::exception(););
                if (is_null_connection) {
                    return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
                }

                std::string types;
                for (auto type : file_types) {
                    if (!types.empty()) {
                        types += ",";
                    }
                    types += std::to_string(type);
                }

                // to ensure UpdateCollectionFiles to be a atomic operation
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                mysqlpp::Query statement = connectionPtr->query();
                // since collection_id is a unique column we just need to check whether it exists or not
                statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                          << " engine_type, created_on, updated_time"
                          << " FROM " << META_TABLEFILES << " WHERE table_id in (";
                for (size_t i = 0; i < group.size(); i++) {
                    statement << mysqlpp::quote << group[i];
                    if (i != group.size() - 1) {
                        statement << ",";
                    }
                }
                statement << ") AND file_type in (" << types << ");";

                LOG_ENGINE_DEBUG_ << "FilesByTypeEx: " << statement.str();

                res = statement.store();
            }  // Scoped Connection

            for (auto& resRow : res) {
                SegmentSchema file_schema;
                file_schema.id_ = resRow["id"];  // implicit conversion
                resRow["table_id"].to_string(file_schema.collection_id_);
                resRow["segment_id"].to_string(file_schema.segment_id_);
                resRow["file_id"].to_string(file_schema.file_id_);
                file_schema.file_type_ = resRow["file_type"];
                file_schema.file_size_ = resRow["file_size"];
                file_schema.row_count_ = resRow["row_count"];
                file_schema.date_ = resRow["date"];
                file_schema.engine_type_ = resRow["engine_type"];
                file_schema.created_on_ = resRow["created_on"];
                file_schema.updated_time_ = resRow["updated_time"];

                auto& collection_schema = map_collections[file_schema.collection_id_];
                file_schema.dimension_ = collection_schema.dimension_;
                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;

                auto status = utils::GetCollectionFilePath(options_, file_schema);
                if (!status.ok()) {
                    ret = status;
                    continue;
                }

                files_holder.MarkFile(file_schema);

                int32_t file_type = resRow["file_type"];
                switch (file_type) {
                    case (int)SegmentSchema::RAW:
                        ++raw_count;
                        break;
                    case (int)SegmentSchema::NEW:
                        ++new_count;
                        break;
                    case (int)SegmentSchema::NEW_MERGE:
                        ++new_merge_count;
                        break;
                    case (int)SegmentSchema::NEW_INDEX:
                        ++new_index_count;
                        break;
                    case (int)SegmentSchema::TO_INDEX:
                        ++to_index_count;
                        break;
                    case (int)SegmentSchema::INDEX:
                        ++index_count;
                        break;
                    case (int)SegmentSchema::BACKUP:
                        ++backup_count;
                        break;
                    default:
                        break;
                }
            }
        }

        std::string msg = "Get collection files by type.";
        for (int file_type : file_types) {
            switch (file_type) {
                case (int)SegmentSchema::RAW:
                    msg = msg + " raw files:" + std::to_string(raw_count);
                    break;
                case (int)SegmentSchema::NEW:
                    msg = msg + " new files:" + std::to_string(new_count);
                    break;
                case (int)SegmentSchema::NEW_MERGE:
                    msg = msg + " new_merge files:" + std::to_string(new_merge_count);
                    break;
                case (int)SegmentSchema::NEW_INDEX:
                    msg = msg + " new_index files:" + std::to_string(new_index_count);
                    break;
                case (int)SegmentSchema::TO_INDEX:
                    msg = msg + " to_index files:" + std::to_string(to_index_count);
                    break;
                case (int)SegmentSchema::INDEX:
                    msg = msg + " index files:" + std::to_string(index_count);
                    break;
                case (int)SegmentSchema::BACKUP:
                    msg = msg + " backup files:" + std::to_string(backup_count);
                    break;
                default:
                    break;
            }
        }
        LOG_ENGINE_DEBUG_ << msg;
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files by type", e.what());
    }
}

Status
MySQLMetaImpl::FilesByID(const std::vector<size_t>& ids, FilesHolder& files_holder) {
    if (ids.empty()) {
        return Status::OK();
    }

    try {
        server::MetricCollector metric;
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.FilesByID.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.FilesByID.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                      << " engine_type, created_on, updated_time"
                      << " FROM " << META_TABLEFILES;

            std::stringstream idSS;
            for (auto& id : ids) {
                idSS << "id = " << std::to_string(id) << " OR ";
            }
            std::string idStr = idSS.str();
            idStr = idStr.substr(0, idStr.size() - 4);  // remove the last " OR "

            statement << " WHERE (" << idStr << ")";

            LOG_ENGINE_DEBUG_ << "FilesByID: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        std::map<std::string, meta::CollectionSchema> collections;
        Status ret;
        int64_t files_count = 0;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.id_ = resRow["id"];  // implicit conversion
            resRow["table_id"].to_string(collection_file.collection_id_);
            resRow["segment_id"].to_string(collection_file.segment_id_);
            resRow["file_id"].to_string(collection_file.file_id_);
            collection_file.file_type_ = resRow["file_type"];
            collection_file.file_size_ = resRow["file_size"];
            collection_file.row_count_ = resRow["row_count"];
            collection_file.date_ = resRow["date"];
            collection_file.engine_type_ = resRow["engine_type"];
            collection_file.created_on_ = resRow["created_on"];
            collection_file.updated_time_ = resRow["updated_time"];

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
                continue;
            }

            files_holder.MarkFile(collection_file);
            files_count++;
        }

        milvus::engine::meta::SegmentsSchema& files = files_holder.HoldFiles();
        for (auto& collection_file : files) {
            CollectionSchema& collection_schema = collections[collection_file.collection_id_];
            collection_file.dimension_ = collection_schema.dimension_;
            collection_file.index_file_size_ = collection_schema.index_file_size_;
            collection_file.index_params_ = collection_schema.index_params_;
            collection_file.metric_type_ = collection_schema.metric_type_;
        }

        if (files_count == 0) {
            LOG_ENGINE_ERROR_ << "No file to search in file id list";
        } else {
            LOG_ENGINE_DEBUG_ << "Collect " << files_count << " files by id";
        }

        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files by id", e.what());
    }
}

// TODO(myh): Support swap to cloud storage
Status
MySQLMetaImpl::Archive() {
    auto& criterias = options_.archive_conf_.GetCriterias();
    if (criterias.empty()) {
        return Status::OK();
    }

    for (auto& kv : criterias) {
        auto& criteria = kv.first;
        auto& limit = kv.second;
        if (criteria == engine::ARCHIVE_CONF_DAYS) {
            size_t usecs = limit * DAY * US_PS;
            int64_t now = utils::GetMicroSecTimeStamp();

            try {
                mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

                bool is_null_connection = (connectionPtr == nullptr);
                fiu_do_on("MySQLMetaImpl.Archive.null_connection", is_null_connection = true);
                fiu_do_on("MySQLMetaImpl.Archive.throw_exception", throw std::exception(););
                if (is_null_connection) {
                    return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
                }

                mysqlpp::Query statement = connectionPtr->query();
                statement << "UPDATE " << META_TABLEFILES
                          << " SET file_type = " << std::to_string(SegmentSchema::TO_DELETE) << " WHERE created_on < "
                          << std::to_string(now - usecs) << " AND file_type <> "
                          << std::to_string(SegmentSchema::TO_DELETE) << ";";

                LOG_ENGINE_DEBUG_ << "Archive: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to archive", statement.error());
                }

                LOG_ENGINE_DEBUG_ << "Archive old files";
            } catch (std::exception& e) {
                return HandleException("Failed to archive", e.what());
            }
        }
        if (criteria == engine::ARCHIVE_CONF_DISK) {
            uint64_t sum = 0;
            Size(sum);

            auto to_delete = (sum - limit * GB);
            DiscardFiles(to_delete);

            LOG_ENGINE_DEBUG_ << "Archive files to free disk";
        }
    }

    return Status::OK();
}

Status
MySQLMetaImpl::Size(uint64_t& result) {
    result = 0;

    try {
        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.Size.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.Size.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT IFNULL(SUM(file_size),0) AS sum"
                      << " FROM " << META_TABLEFILES << " WHERE file_type <> "
                      << std::to_string(SegmentSchema::TO_DELETE) << ";";

            LOG_ENGINE_DEBUG_ << "Size: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (res.empty()) {
            result = 0;
        } else {
            result = res[0]["sum"];
        }
    } catch (std::exception& e) {
        return HandleException("Failed to get total files size", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::CleanUpShadowFiles() {
    try {
        mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

        bool is_null_connection = (connectionPtr == nullptr);
        fiu_do_on("MySQLMetaImpl.CleanUpShadowFiles.null_connection", is_null_connection = true);
        fiu_do_on("MySQLMetaImpl.CleanUpShadowFiles.throw_exception", throw std::exception(););
        if (is_null_connection) {
            return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
        }

        mysqlpp::Query statement = connectionPtr->query();
        statement << "SELECT table_name"
                  << " FROM information_schema.tables"
                  << " WHERE table_schema = " << mysqlpp::quote << mysql_connection_pool_->db_name()
                  << " AND table_name = " << mysqlpp::quote << META_TABLEFILES << ";";

        LOG_ENGINE_DEBUG_ << "CleanUpShadowFiles: " << statement.str();

        mysqlpp::StoreQueryResult res = statement.store();

        if (!res.empty()) {
            LOG_ENGINE_DEBUG_ << "Remove collection file type as NEW";
            statement << "DELETE FROM " << META_TABLEFILES << " WHERE file_type IN ("
                      << std::to_string(SegmentSchema::NEW) << "," << std::to_string(SegmentSchema::NEW_MERGE) << ","
                      << std::to_string(SegmentSchema::NEW_INDEX) << ");";

            LOG_ENGINE_DEBUG_ << "CleanUp: " << statement.str();

            if (!statement.exec()) {
                return HandleException("Failed to clean shadow files", statement.error());
            }
        }

        if (res.size() > 0) {
            LOG_ENGINE_DEBUG_ << "Clean " << res.size() << " files";
        }
    } catch (std::exception& e) {
        return HandleException("Failed to clean shadow files", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::CleanUpFilesWithTTL(uint64_t seconds /*, CleanUpFilter* filter*/) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> collection_ids;
    std::map<std::string, SegmentSchema> segment_ids;

    // remove to_delete files
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RomoveToDeleteFiles_NullConnection",
                      is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RomoveToDeleteFiles_ThrowException", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            mysqlpp::StoreQueryResult res;
            {
                // to ensure UpdateCollectionFiles to be a atomic operation
                std::lock_guard<std::mutex> meta_lock(meta_mutex_);

                statement << "SELECT id, table_id, segment_id, engine_type, file_id, file_type, date"
                          << " FROM " << META_TABLEFILES << " WHERE file_type IN ("
                          << std::to_string(SegmentSchema::TO_DELETE) << "," << std::to_string(SegmentSchema::BACKUP)
                          << ")"
                          << " AND updated_time < " << std::to_string(now - seconds * US_PS) << ";";

                //                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement.str();

                res = statement.store();
            }

            SegmentSchema collection_file;
            std::vector<std::string> delete_ids;

            int64_t clean_files = 0;
            for (auto& resRow : res) {
                collection_file.id_ = resRow["id"];  // implicit conversion
                resRow["table_id"].to_string(collection_file.collection_id_);
                resRow["segment_id"].to_string(collection_file.segment_id_);
                collection_file.engine_type_ = resRow["engine_type"];
                resRow["file_id"].to_string(collection_file.file_id_);
                collection_file.date_ = resRow["date"];
                collection_file.file_type_ = resRow["file_type"];

                // check if the file can be deleted
                if (!FilesHolder::CanBeDeleted(collection_file)) {
                    LOG_ENGINE_DEBUG_ << "File:" << collection_file.file_id_
                                      << " currently is in use, not able to delete now";
                    continue;  // ignore this file, don't delete it
                }

                // erase file data from cache
                // because GetCollectionFilePath won't able to generate file path after the file is deleted
                utils::GetCollectionFilePath(options_, collection_file);
                server::CommonUtil::EraseFromCache(collection_file.location_);

                if (collection_file.file_type_ == (int)SegmentSchema::TO_DELETE) {
                    // delete file from disk storage
                    utils::DeleteCollectionFilePath(options_, collection_file);
                    LOG_ENGINE_DEBUG_ << "Remove file id:" << collection_file.id_
                                      << " location:" << collection_file.location_;

                    delete_ids.emplace_back(std::to_string(collection_file.id_));
                    collection_ids.insert(collection_file.collection_id_);
                    segment_ids.insert(std::make_pair(collection_file.segment_id_, collection_file));

                    clean_files++;
                }
            }

            // delete file from meta
            if (!delete_ids.empty()) {
                std::stringstream idsToDeleteSS;
                for (auto& id : delete_ids) {
                    idsToDeleteSS << "id = " << id << " OR ";
                }

                std::string idsToDeleteStr = idsToDeleteSS.str();
                idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4);  // remove the last " OR "
                statement << "DELETE FROM " << META_TABLEFILES << " WHERE " << idsToDeleteStr << ";";

                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to clean up with ttl", statement.error());
                }
            }

            if (clean_files > 0) {
                LOG_ENGINE_DEBUG_ << "Clean " << clean_files << " files expired in " << seconds << " seconds";
            }
        }  // Scoped Connection
    } catch (std::exception& e) {
        return HandleException("Failed to clean up with ttl", e.what());
    }

    // remove to_delete collections
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteCollections_NUllConnection",
                      is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveToDeleteCollections_ThrowException",
                      throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, table_id"
                      << " FROM " << META_TABLES << " WHERE state = " << std::to_string(CollectionSchema::TO_DELETE)
                      << ";";

            //            LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement.str();

            mysqlpp::StoreQueryResult res = statement.store();

            int64_t remove_collections = 0;
            if (!res.empty()) {
                std::stringstream idsToDeleteSS;
                for (auto& resRow : res) {
                    size_t id = resRow["id"];
                    std::string collection_id;
                    resRow["table_id"].to_string(collection_id);

                    utils::DeleteCollectionPath(options_, collection_id, false);  // only delete empty folder
                    ++remove_collections;
                    idsToDeleteSS << "id = " << std::to_string(id) << " OR ";
                }
                std::string idsToDeleteStr = idsToDeleteSS.str();
                idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4);  // remove the last " OR "
                statement << "DELETE FROM " << META_TABLES << " WHERE " << idsToDeleteStr << ";";

                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to clean up with ttl", statement.error());
                }
            }

            if (remove_collections > 0) {
                LOG_ENGINE_DEBUG_ << "Remove " << remove_collections << " collections from meta";
            }
        }  // Scoped Connection
    } catch (std::exception& e) {
        return HandleException("Failed to clean up with ttl", e.what());
    }

    // remove deleted collection folder
    // don't remove collection folder until all its files has been deleted
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedCollectionFolder_NUllConnection",
                      is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedCollectionFolder_ThrowException",
                      throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            for (auto& collection_id : collection_ids) {
                mysqlpp::Query statement = connectionPtr->query();
                statement << "SELECT file_id"
                          << " FROM " << META_TABLEFILES << " WHERE table_id = " << mysqlpp::quote << collection_id
                          << ";";

                //                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement.str();

                mysqlpp::StoreQueryResult res = statement.store();

                if (res.empty()) {
                    utils::DeleteCollectionPath(options_, collection_id);
                }
            }

            if (collection_ids.size() > 0) {
                LOG_ENGINE_DEBUG_ << "Remove " << collection_ids.size() << " collections folder";
            }
        }
    } catch (std::exception& e) {
        return HandleException("Failed to clean up with ttl", e.what());
    }

    // remove deleted segment folder
    // don't remove segment folder until all its files has been deleted
    try {
        server::MetricCollector metric;

        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedSegmentFolder_NUllConnection",
                      is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.CleanUpFilesWithTTL.RemoveDeletedSegmentFolder_ThrowException",
                      throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            int64_t remove_segments = 0;
            for (auto& segment_id : segment_ids) {
                mysqlpp::Query statement = connectionPtr->query();
                statement << "SELECT id"
                          << " FROM " << META_TABLEFILES << " WHERE segment_id = " << mysqlpp::quote << segment_id.first
                          << ";";

                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement.str();

                mysqlpp::StoreQueryResult res = statement.store();

                if (res.empty()) {
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
        }
    } catch (std::exception& e) {
        return HandleException("Failed to clean up with ttl", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::Count(const std::string& collection_id, uint64_t& result) {
    try {
        server::MetricCollector metric;

        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.Count.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.Count.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(meta_mutex_);

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT row_count"
                      << " FROM " << META_TABLEFILES << " WHERE table_id = " << mysqlpp::quote << collection_id
                      << " AND (file_type = " << std::to_string(SegmentSchema::RAW)
                      << " OR file_type = " << std::to_string(SegmentSchema::TO_INDEX)
                      << " OR file_type = " << std::to_string(SegmentSchema::INDEX) << ");";

            LOG_ENGINE_DEBUG_ << "Count: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        result = 0;
        for (auto& resRow : res) {
            size_t size = resRow["row_count"];
            result += size;
        }
    } catch (std::exception& e) {
        return HandleException("Failed to clean up with ttl", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::DropAll() {
    try {
        LOG_ENGINE_DEBUG_ << "Drop all mysql meta";
        mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

        bool is_null_connection = (connectionPtr == nullptr);
        fiu_do_on("MySQLMetaImpl.DropAll.null_connection", is_null_connection = true);
        fiu_do_on("MySQLMetaImpl.DropAll.throw_exception", throw std::exception(););
        if (is_null_connection) {
            return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
        }

        mysqlpp::Query statement = connectionPtr->query();
        statement << "DROP TABLE IF EXISTS " << TABLES_SCHEMA.name() << ", " << TABLEFILES_SCHEMA.name() << ", "
                  << ENVIRONMENT_SCHEMA.name() << ", " << FIELDS_SCHEMA.name() << ";";

        LOG_ENGINE_DEBUG_ << "DropAll: " << statement.str();

        if (statement.exec()) {
            return Status::OK();
        }
        return HandleException("Failed to drop all", statement.error());
    } catch (std::exception& e) {
        return HandleException("Failed to drop all", e.what());
    }
}

Status
MySQLMetaImpl::DiscardFiles(int64_t to_discard_size) {
    if (to_discard_size <= 0) {
        return Status::OK();
    }
    LOG_ENGINE_DEBUG_ << "About to discard size=" << to_discard_size;

    try {
        server::MetricCollector metric;
        bool status;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);

            bool is_null_connection = (connectionPtr == nullptr);
            fiu_do_on("MySQLMetaImpl.DiscardFiles.null_connection", is_null_connection = true);
            fiu_do_on("MySQLMetaImpl.DiscardFiles.throw_exception", throw std::exception(););
            if (is_null_connection) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT id, file_size"
                      << " FROM " << META_TABLEFILES << " WHERE file_type <> "
                      << std::to_string(SegmentSchema::TO_DELETE) << " ORDER BY id ASC "
                      << " LIMIT 10;";

            LOG_ENGINE_DEBUG_ << "DiscardFiles: " << statement.str();

            mysqlpp::StoreQueryResult res = statement.store();
            if (res.num_rows() == 0) {
                return Status::OK();
            }

            SegmentSchema collection_file;
            std::stringstream idsToDiscardSS;
            for (auto& resRow : res) {
                if (to_discard_size <= 0) {
                    break;
                }
                collection_file.id_ = resRow["id"];
                collection_file.file_size_ = resRow["file_size"];
                idsToDiscardSS << "id = " << std::to_string(collection_file.id_) << " OR ";
                LOG_ENGINE_DEBUG_ << "Discard file id=" << collection_file.file_id_
                                  << " file size=" << collection_file.file_size_;
                to_discard_size -= collection_file.file_size_;
            }

            std::string idsToDiscardStr = idsToDiscardSS.str();
            idsToDiscardStr = idsToDiscardStr.substr(0, idsToDiscardStr.size() - 4);  // remove the last " OR "

            statement << "UPDATE " << META_TABLEFILES << " SET file_type = " << std::to_string(SegmentSchema::TO_DELETE)
                      << " ,updated_time = " << std::to_string(utils::GetMicroSecTimeStamp()) << " WHERE "
                      << idsToDiscardStr << ";";

            LOG_ENGINE_DEBUG_ << "DiscardFiles: " << statement.str();

            status = statement.exec();
            if (!status) {
                return HandleException("Failed to discard files", statement.error());
            }
        }  // Scoped Connection

        return DiscardFiles(to_discard_size);
    } catch (std::exception& e) {
        return HandleException("Failed to discard files", e.what());
    }
}

Status
MySQLMetaImpl::SetGlobalLastLSN(uint64_t lsn) {
    try {
        server::MetricCollector metric;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            bool first_create = false;
            uint64_t last_lsn = 0;
            {
                mysqlpp::StoreQueryResult res;
                mysqlpp::Query statement = connectionPtr->query();
                statement << "SELECT global_lsn FROM " << META_ENVIRONMENT << ";";
                res = statement.store();
                if (res.num_rows() == 0) {
                    first_create = true;
                } else {
                    last_lsn = res[0]["global_lsn"];
                }
            }

            if (first_create) {  // first time to get global lsn
                mysqlpp::Query statement = connectionPtr->query();
                statement << "INSERT INTO " << META_ENVIRONMENT << " VALUES(" << lsn << ");";
                LOG_ENGINE_DEBUG_ << "SetGlobalLastLSN: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("QUERY ERROR WHEN SET GLOBAL LSN", statement.error());
                }
            } else if (lsn > last_lsn) {
                mysqlpp::Query statement = connectionPtr->query();
                statement << "UPDATE " << META_ENVIRONMENT << " SET global_lsn = " << lsn << ";";
                LOG_ENGINE_DEBUG_ << "SetGlobalLastLSN: " << statement.str();

                if (!statement.exec()) {
                    return HandleException("Failed to set global lsn", statement.error());
                }
            }
        }  // Scoped Connection
    } catch (std::exception& e) {
        return HandleException("Failed to set global lsn", e.what());
    }

    return Status::OK();
}

Status
MySQLMetaImpl::GetGlobalLastLSN(uint64_t& lsn) {
    try {
        server::MetricCollector metric;

        mysqlpp::StoreQueryResult res;
        {
            mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
            if (connectionPtr == nullptr) {
                return Status(DB_ERROR, "Failed to connect to meta server(mysql)");
            }

            mysqlpp::Query statement = connectionPtr->query();
            statement << "SELECT global_lsn FROM " << META_ENVIRONMENT << ";";

            LOG_ENGINE_DEBUG_ << "GetGlobalLastLSN: " << statement.str();

            res = statement.store();
        }  // Scoped Connection

        if (!res.empty()) {
            lsn = res[0]["global_lsn"];
        }
    } catch (std::exception& e) {
        return HandleException("Failed to update global lsn", e.what());
    }

    return Status::OK();
}

}  // namespace meta
}  // namespace engine
}  // namespace milvus
