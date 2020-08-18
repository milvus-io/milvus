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

#include "db/meta/backend/MySqlEngine.h"

#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fiu/fiu-local.h>
#include <mysql++/mysql++.h>

#include "db/Utils.h"
#include "db/meta/MetaNames.h"
#include "db/meta/backend/MetaHelper.h"
#include "db/meta/backend/MetaSchema.h"
#include "utils//Log.h"
#include "utils/Exception.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

////////// private namespace //////////
namespace {
static const auto MetaIdField = MetaField(F_ID, "BIGINT", "PRIMARY KEY AUTO_INCREMENT");
static const MetaField MetaCollectionIdField = MetaField(F_COLLECTON_ID, "BIGINT", "NOT NULL");
static const MetaField MetaPartitionIdField = MetaField(F_PARTITION_ID, "BIGINT", "NOT NULL");
static const MetaField MetaSchemaIdField = MetaField(F_SCHEMA_ID, "BIGINT", "NOT NULL");
static const MetaField MetaSegmentIdField = MetaField(F_SEGMENT_ID, "BIGINT", "NOT NULL");
static const MetaField MetaFieldElementIdField = MetaField(F_FIELD_ELEMENT_ID, "BIGINT", "NOT NULL");
static const MetaField MetaFieldIdField = MetaField(F_FIELD_ID, "BIGINT", "NOT NULL");
static const MetaField MetaNameField = MetaField(F_NAME, "VARCHAR(255)", "NOT NULL");
static const MetaField MetaMappingsField = MetaField(F_MAPPINGS, "JSON", "NOT NULL");
static const MetaField MetaNumField = MetaField(F_NUM, "BIGINT", "NOT NULL");
static const MetaField MetaLSNField = MetaField(F_LSN, "BIGINT", "NOT NULL");
static const MetaField MetaFtypeField = MetaField(F_FTYPE, "BIGINT", "NOT NULL");
static const MetaField MetaStateField = MetaField(F_STATE, "TINYINT", "NOT NULL");
static const MetaField MetaCreatedOnField = MetaField(F_CREATED_ON, "BIGINT", "NOT NULL");
static const MetaField MetaUpdatedOnField = MetaField(F_UPDATED_ON, "BIGINT", "NOT NULL");
static const MetaField MetaParamsField = MetaField(F_PARAMS, "JSON", "NOT NULL");
static const MetaField MetaSizeField = MetaField(F_SIZE, "BIGINT", "NOT NULL");
static const MetaField MetaRowCountField = MetaField(F_ROW_COUNT, "BIGINT", "NOT NULL");
static const MetaField MetaTypeNameField = MetaField(F_TYPE_NAME, "VARCHAR(255)", "NOT NULL");

// Environment schema
static const MetaSchema COLLECTION_SCHEMA(TABLE_COLLECTION, {MetaIdField, MetaNameField, MetaLSNField, MetaParamsField,
                                                             MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

// Tables schema
static const MetaSchema COLLECTIONCOMMIT_SCHEMA(TABLE_COLLECTION_COMMIT,
                                                {MetaIdField, MetaCollectionIdField, MetaSchemaIdField,
                                                 MetaMappingsField, MetaRowCountField, MetaSizeField, MetaLSNField,
                                                 MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

// TableFiles schema
static const MetaSchema PARTITION_SCHEMA(TABLE_PARTITION,
                                         {MetaIdField, MetaNameField, MetaCollectionIdField, MetaLSNField,
                                          MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

// Fields schema
static const MetaSchema PARTITIONCOMMIT_SCHEMA(TABLE_PARTITION_COMMIT,
                                               {MetaIdField, MetaCollectionIdField, MetaPartitionIdField,
                                                MetaMappingsField, MetaRowCountField, MetaSizeField, MetaStateField,
                                                MetaLSNField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema SEGMENT_SCHEMA(TABLE_SEGMENT,
                                       {MetaIdField, MetaCollectionIdField, MetaPartitionIdField, MetaNumField,
                                        MetaLSNField, MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema SEGMENTCOMMIT_SCHEMA(TABLE_SEGMENT_COMMIT,
                                             {MetaIdField, MetaSchemaIdField, MetaPartitionIdField, MetaSegmentIdField,
                                              MetaMappingsField, MetaRowCountField, MetaSizeField, MetaLSNField,
                                              MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema SEGMENTFILE_SCHEMA(TABLE_SEGMENT_FILE,
                                           {MetaIdField, MetaCollectionIdField, MetaPartitionIdField,
                                            MetaSegmentIdField, MetaFieldElementIdField, MetaFtypeField,
                                            MetaRowCountField, MetaSizeField, MetaLSNField, MetaStateField,
                                            MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema SCHEMACOMMIT_SCHEMA(TABLE_SCHEMA_COMMIT,
                                            {MetaIdField, MetaCollectionIdField, MetaMappingsField, MetaLSNField,
                                             MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema FIELD_SCHEMA(TABLE_FIELD,
                                     {MetaIdField, MetaNameField, MetaNumField, MetaFtypeField, MetaParamsField,
                                      MetaLSNField, MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema FIELDCOMMIT_SCHEMA(TABLE_FIELD_COMMIT,
                                           {MetaIdField, MetaCollectionIdField, MetaFieldIdField, MetaMappingsField,
                                            MetaLSNField, MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema FIELDELEMENT_SCHEMA(TABLE_FIELD_ELEMENT,
                                            {MetaIdField, MetaCollectionIdField, MetaFieldIdField, MetaNameField,
                                             MetaFtypeField, MetaTypeNameField, MetaParamsField, MetaLSNField,
                                             MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

}  // namespace

/////////////// MySqlEngine ///////////////
Status
MySqlEngine::Initialize() {
    // step 1: create db root path
    //    if (!boost::filesystem::is_directory(options_.path_)) {
    //        auto ret = boost::filesystem::create_directory(options_.path_);
    //        fiu_do_on("MySQLMetaImpl.Initialize.fail_create_directory", ret = false);
    //        if (!ret) {
    //            std::string msg = "Failed to create db directory " + options_.path_;
    //            LOG_ENGINE_ERROR_ << msg;
    //            throw Exception(DB_META_TRANSACTION_FAILED, msg);
    //        }
    //    }
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

    mysql_connection_pool_ = std::make_shared<meta::MySQLConnectionPool>(
        uri_info.db_name_, uri_info.username_, uri_info.password_, uri_info.host_, port, max_pool_size);
    LOG_ENGINE_DEBUG_ << "MySQL connection pool: maximum pool size = " << std::to_string(max_pool_size);

    // step 4: validate to avoid open old version schema
    //    ValidateMetaSchema();

    // step 5: clean shadow files
    //    if (mode_ != DBOptions::MODE::CLUSTER_READONLY) {
    //        CleanUpShadowFiles();
    //    }

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

    mysqlpp::Query InitializeQuery = connectionPtr->query();

    auto create_schema = [&](const MetaSchema& schema) {
        std::string create_table_str = "CREATE TABLE IF NOT EXISTS " + schema.name() + "(" + schema.ToString() + ");";
        InitializeQuery << create_table_str;
        //        LOG_ENGINE_DEBUG_ << "Initialize: " << InitializeQuery.str();

        bool initialize_query_exec = InitializeQuery.exec();
        //        fiu_do_on("MySQLMetaImpl.Initialize.fail_create_collection_files", initialize_query_exec = false);
        if (!initialize_query_exec) {
            std::string msg = "Failed to create meta collection '" + schema.name() + "' in MySQL";
            LOG_ENGINE_ERROR_ << msg;
            throw Exception(DB_META_TRANSACTION_FAILED, msg);
        }
    };

    create_schema(COLLECTION_SCHEMA);
    create_schema(COLLECTIONCOMMIT_SCHEMA);
    create_schema(PARTITION_SCHEMA);
    create_schema(PARTITIONCOMMIT_SCHEMA);
    create_schema(SEGMENT_SCHEMA);
    create_schema(SEGMENTCOMMIT_SCHEMA);
    create_schema(SEGMENTFILE_SCHEMA);
    create_schema(SCHEMACOMMIT_SCHEMA);
    create_schema(FIELD_SCHEMA);
    create_schema(FIELDCOMMIT_SCHEMA);
    create_schema(FIELDELEMENT_SCHEMA);

    return Status::OK();
}

Status
MySqlEngine::Query(const MetaQueryContext& context, AttrsMapList& attrs) {
    auto status = Status::OK();
    mysqlpp::Connection::thread_start();

    try {
        mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
        if (connectionPtr == nullptr || !connectionPtr->connected()) {
            mysqlpp::Connection::thread_end();
            return Status(SS_TIMEOUT, "Mysql server is not accessed");
        }

        std::string sql;
        status = MetaHelper::MetaQueryContextToSql(context, sql);
        if (!status.ok()) {
            mysqlpp::Connection::thread_end();
            return status;
        }

        // std::lock_guard<std::mutex> lock(meta_mutex_);
        mysqlpp::Query query = connectionPtr->query(sql);
        auto res = query.store();
        if (!res) {
            mysqlpp::Connection::thread_end();
            std::stringstream ss;
            ss << "Mysql query fail: (" << query.errnum() << ": " << query.error() << ")";
            std::string err_msg = ss.str();
            LOG_ENGINE_ERROR_ << err_msg;
            return Status(DB_ERROR, err_msg);
        }

        auto names = res.field_names();
        for (auto& row : res) {
            AttrsMap attrs_map;
            for (auto& name : *names) {
                attrs_map.insert(std::make_pair(name, row[name.c_str()]));
            }
            attrs.push_back(attrs_map);
        }
    } catch (const mysqlpp::ConnectionFailed& er) {
        status = Status(SS_TIMEOUT, er.what());
    } catch (const mysqlpp::BadQuery& er) {
        LOG_ENGINE_ERROR_ << "Query error: " << er.what();
        if (er.errnum() == 2006) {
            status = Status(SS_TIMEOUT, er.what());
        } else {
            status = Status(DB_ERROR, er.what());
        }
    } catch (const mysqlpp::BadConversion& er) {
        // Handle bad conversions
        //        cerr << "Conversion error: " << er.what() << endl <<
        //             "\tretrieved data size: " << er.retrieved <<
        //             ", actual size: " << er.actual_size << endl;
        status = Status(DB_ERROR, er.what());
    } catch (const mysqlpp::Exception& er) {
        // Catch-all for any other MySQL++ exceptions
        //        cerr << "Error: " << er.what() << endl;
        status = Status(DB_ERROR, er.what());
    }

    mysqlpp::Connection::thread_end();
    return status;
}

Status
MySqlEngine::ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) {
    Status status;
    mysqlpp::Connection::thread_start();

    try {
        mysqlpp::ScopedConnection connectionPtr(*mysql_connection_pool_, safe_grab_);
        if (connectionPtr == nullptr || !connectionPtr->connected()) {
            mysqlpp::Connection::thread_end();
            return Status(SS_TIMEOUT, "Mysql server is not accessed");
        }

        mysqlpp::Transaction trans(*connectionPtr, mysqlpp::Transaction::serializable, mysqlpp::Transaction::session);

        // std::lock_guard<std::mutex> lock(meta_mutex_);
        for (auto& context : sql_contexts) {
            std::string sql;
            status = MetaHelper::MetaApplyContextToSql(context, sql);
            if (!status.ok()) {
                break;
            }

            auto query = connectionPtr->query(sql);
            auto res = query.execute();
            if (!res) {
                std::stringstream ss;
                ss << "Mysql execute fail: (" << query.errnum() << ": " << query.error() << ")";
                status = Status(DB_ERROR, ss.str());
                break;
            }

            if (context.op_ == oAdd) {
                auto id = res.insert_id();
                result_ids.push_back(id);
            } else {
                result_ids.push_back(context.id_);
            }
        }

        if (status.ok()) {
            trans.commit();
        } else {
            trans.rollback();
        }
    } catch (const mysqlpp::ConnectionFailed& er) {
        status = Status(SS_TIMEOUT, er.what());
    } catch (const mysqlpp::BadQuery& er) {
        LOG_ENGINE_ERROR_ << "MySql Error Code: " << er.errnum() << ": " << er.what();
        if (er.errnum() == 2006) {
            status = Status(SS_TIMEOUT, er.what());
        } else {
            status = Status(DB_ERROR, er.what());
        }
    } catch (const mysqlpp::BadConversion& er) {
        // Handle bad conversions
        //        cerr << "Conversion error: " << er.what() << endl <<
        //             "\tretrieved data size: " << er.retrieved <<
        //             ", actual size: " << er.actual_size << endl;
        //        return -1;
        //        std::cout << "[DB] Error: " << er.what() << std::endl;
        status = Status(DB_ERROR, er.what());
    } catch (const mysqlpp::Exception& er) {
        // Catch-all for any other MySQL++ exceptions
        //        cerr << "Error: " << er.what() << endl;
        //        return -1;
        //        std::cout << "[DB] Error: " << er.what() << std::endl;
        status = Status(DB_ERROR, er.what());
    }

    mysqlpp::Connection::thread_end();
    return status;
}

Status
MySqlEngine::TruncateAll() {
    static std::vector<std::string> collecton_names = {
        COLLECTION_SCHEMA.name(),      COLLECTIONCOMMIT_SCHEMA.name(), PARTITION_SCHEMA.name(),
        PARTITIONCOMMIT_SCHEMA.name(), SEGMENT_SCHEMA.name(),          SEGMENTCOMMIT_SCHEMA.name(),
        SEGMENTFILE_SCHEMA.name(),     SCHEMACOMMIT_SCHEMA.name(),     FIELD_SCHEMA.name(),
        FIELDCOMMIT_SCHEMA.name(),     FIELDELEMENT_SCHEMA.name(),
    };

    std::vector<MetaApplyContext> contexts;
    for (auto& name : collecton_names) {
        MetaApplyContext context;
        context.sql_ = "TRUNCATE " + name + ";";
        context.id_ = 0;

        contexts.push_back(context);
    }

    std::vector<snapshot::ID_TYPE> ids;
    return ExecuteTransaction(contexts, ids);
}

}  // namespace milvus::engine::meta
