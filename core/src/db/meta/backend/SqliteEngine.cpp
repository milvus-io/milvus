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

#include "db/meta/backend/SqliteEngine.h"

#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/meta/MetaNames.h"
#include "db/meta/backend/MetaHelper.h"
#include "db/meta/backend/MetaSchema.h"

namespace milvus::engine::meta {

////////// private namespace //////////
namespace {
static const auto MetaIdField = MetaField(F_ID, "INTEGER", "PRIMARY KEY AUTOINCREMENT");
static const MetaField MetaCollectionIdField = MetaField(F_COLLECTON_ID, "BIGINT", "NOT NULL");
static const MetaField MetaPartitionIdField = MetaField(F_PARTITION_ID, "BIGINT", "NOT NULL");
static const MetaField MetaSchemaIdField = MetaField(F_SCHEMA_ID, "BIGINT", "NOT NULL");
static const MetaField MetaSegmentIdField = MetaField(F_SEGMENT_ID, "BIGINT", "NOT NULL");
static const MetaField MetaFieldElementIdField = MetaField(F_FIELD_ELEMENT_ID, "BIGINT", "NOT NULL");
static const MetaField MetaFieldIdField = MetaField(F_FIELD_ID, "BIGINT", "NOT NULL");
static const MetaField MetaNameField = MetaField(F_NAME, "VARCHAR(255)", "NOT NULL");
static const MetaField MetaMappingsField = MetaField(F_MAPPINGS, "VARCHAR(255)", "NOT NULL");
static const MetaField MetaNumField = MetaField(F_NUM, "BIGINT", "NOT NULL");
static const MetaField MetaLSNField = MetaField(F_LSN, "BIGINT", "NOT NULL");
static const MetaField MetaFtypeField = MetaField(F_FTYPE, "BIGINT", "NOT NULL");
static const MetaField MetaStateField = MetaField(F_STATE, "TINYINT", "NOT NULL");
static const MetaField MetaCreatedOnField = MetaField(F_CREATED_ON, "BIGINT", "NOT NULL");
static const MetaField MetaUpdatedOnField = MetaField(F_UPDATED_ON, "BIGINT", "NOT NULL");
static const MetaField MetaParamsField = MetaField(F_PARAMS, "VARCHAR(255)", "NOT NULL");
static const MetaField MetaSizeField = MetaField(F_SIZE, "BIGINT", "NOT NULL");
static const MetaField MetaRowCountField = MetaField(F_ROW_COUNT, "BIGINT", "NOT NULL");

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

static const MetaSchema SEGMENT_SCHEMA(TABLE_SEGMENT, {
                                                          MetaIdField,
                                                          MetaCollectionIdField,
                                                          MetaPartitionIdField,
                                                          MetaNumField,
                                                          MetaLSNField,
                                                          MetaStateField,
                                                          MetaCreatedOnField,
                                                          MetaUpdatedOnField,
                                                      });

static const MetaSchema SEGMENTCOMMIT_SCHEMA(TABLE_SEGMENT_COMMIT, {
                                                                       MetaIdField,
                                                                       MetaSchemaIdField,
                                                                       MetaPartitionIdField,
                                                                       MetaSegmentIdField,
                                                                       MetaMappingsField,
                                                                       MetaRowCountField,
                                                                       MetaSizeField,
                                                                       MetaLSNField,
                                                                       MetaStateField,
                                                                       MetaCreatedOnField,
                                                                       MetaUpdatedOnField,
                                                                   });

static const MetaSchema SEGMENTFILE_SCHEMA(TABLE_SEGMENT_FILE,
                                           {MetaIdField, MetaCollectionIdField, MetaPartitionIdField,
                                            MetaSegmentIdField, MetaFieldElementIdField, MetaRowCountField,
                                            MetaSizeField, MetaLSNField, MetaStateField, MetaCreatedOnField,
                                            MetaUpdatedOnField});

static const MetaSchema SCHEMACOMMIT_SCHEMA(TABLE_SCHEMA_COMMIT, {
                                                                     MetaIdField,
                                                                     MetaCollectionIdField,
                                                                     MetaMappingsField,
                                                                     MetaLSNField,
                                                                     MetaStateField,
                                                                     MetaCreatedOnField,
                                                                     MetaUpdatedOnField,
                                                                 });

static const MetaSchema FIELD_SCHEMA(TABLE_FIELD,
                                     {MetaIdField, MetaNameField, MetaNumField, MetaFtypeField, MetaParamsField,
                                      MetaLSNField, MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema FIELDCOMMIT_SCHEMA(TABLE_FIELD_COMMIT,
                                           {MetaIdField, MetaCollectionIdField, MetaFieldIdField, MetaMappingsField,
                                            MetaLSNField, MetaStateField, MetaCreatedOnField, MetaUpdatedOnField});

static const MetaSchema FIELDELEMENT_SCHEMA(TABLE_FIELD_ELEMENT,
                                            {MetaIdField, MetaCollectionIdField, MetaFieldIdField, MetaNameField,
                                             MetaFtypeField, MetaParamsField, MetaLSNField, MetaStateField,
                                             MetaCreatedOnField, MetaUpdatedOnField});

/////////////////////////////////////////////////////
static AttrsMapList* QueryData = nullptr;
static AttrsMapList* InsertData = nullptr;
static std::vector<int64_t>* InsertedIDs;
static AttrsMapList* UpdateData = nullptr;
static AttrsMapList* DeleteData = nullptr;

static int
QueryCallback(void* data, int argc, char** argv, char** azColName) {
    AttrsMap raw;
    for (size_t i = 0; i < argc; i++) {
        // TODO: here check argv[i]. Refer to 'https://www.tutorialspoint.com/sqlite/sqlite_c_cpp.htm'
        raw.insert(std::make_pair(azColName[i], argv[i]));
    }

    if (!QueryData) {
        // TODO: check return value -1 or 1
        return -1;
    }

    QueryData->push_back(raw);

    return 0;
}

}  // namespace

SqliteEngine::SqliteEngine(const DBMetaOptions& options) : options_(options) {
    std::string meta_path = options_.path_ + "/meta.sqlite";
    int rc = sqlite3_open(meta_path.c_str(), &db_);
    if (rc) {
        std::string err = "Cannot open Sqlite database: ";
        err += sqlite3_errmsg(db_);
        std::cerr << err << std::endl;
        throw std::runtime_error(err);
    }

    Initialize();
}

SqliteEngine::~SqliteEngine() {
    sqlite3_close(db_);
}

Status
SqliteEngine::Initialize() {
    auto create_schema = [&](const MetaSchema& schema) {
        std::string create_table_str = "CREATE TABLE IF NOT EXISTS " + schema.name() + "(" + schema.ToString() + ");";
        auto rc = sqlite3_exec(db_, create_table_str.c_str(), nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            std::string err = "Cannot create Sqlite table: ";
            err += sqlite3_errmsg(db_);
            throw std::runtime_error(err);
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
SqliteEngine::Query(const MetaQueryContext& context, AttrsMapList& attrs) {
    std::string sql;

    STATUS_CHECK(MetaHelper::MetaQueryContextToSql(context, sql));
    std::lock_guard<std::mutex> lock(meta_mutex_);

    QueryData = &attrs;
    auto rc = sqlite3_exec(db_, sql.c_str(), QueryCallback, NULL, NULL);

    if (rc != SQLITE_OK) {
        std::string err = "Query fail:";
        err += sqlite3_errmsg(db_);
        std::cerr << err << std::endl;
        return Status(DB_META_QUERY_FAILED, err);
    }

    QueryData = nullptr;

    return Status::OK();
}

Status
SqliteEngine::ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) {
    std::vector<std::string> sqls;

    std::string sql;
    for (const auto& context : sql_contexts) {
        STATUS_CHECK(MetaHelper::MetaApplyContextToSql(context, sql));
        sqls.push_back(sql);
    }

    std::lock_guard<std::mutex> lock(meta_mutex_);
    int rc = SQLITE_OK;
    sqlite3_exec(db_, "BEGIN", NULL, NULL, NULL);

    for (size_t i = 0; i < sql_contexts.size(); i++) {
        rc = sqlite3_exec(db_, sqls[i].c_str(), NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            break;
        }

        if (!sql_contexts[i].sql_.empty()) {
            result_ids.push_back(sql_contexts[i].id_);
            continue;
        }

        if (sql_contexts[i].op_ == oAdd) {
            auto id = sqlite3_last_insert_rowid(db_);
            result_ids.push_back(id);
        } else if (sql_contexts[i].op_ == oUpdate || sql_contexts[i].op_ == oDelete) {
            result_ids.push_back(sql_contexts[i].id_);
        } else {
            sqlite3_exec(db_, "ROLLBACK", NULL, NULL, NULL);
            return Status(SERVER_UNEXPECTED_ERROR, "Unknown Op");
        }
    }
    if (SQLITE_OK != rc) {
        std::string err = "Execute Fail:";
        err += sqlite3_errmsg(db_);
        std::cerr << err << std::endl;
        sqlite3_exec(db_, "ROLLBACK", NULL, NULL, NULL);
        return Status(SERVER_UNEXPECTED_ERROR, err);
    }

    sqlite3_exec(db_, "COMMIT", NULL, NULL, NULL);
    return Status();
}

Status
SqliteEngine::TruncateAll() {
    static std::vector<std::string> collecton_names = {
        COLLECTION_SCHEMA.name(),      COLLECTIONCOMMIT_SCHEMA.name(), PARTITION_SCHEMA.name(),
        PARTITIONCOMMIT_SCHEMA.name(), SEGMENT_SCHEMA.name(),          SEGMENTCOMMIT_SCHEMA.name(),
        SEGMENTFILE_SCHEMA.name(),     SCHEMACOMMIT_SCHEMA.name(),     FIELD_SCHEMA.name(),
        FIELDCOMMIT_SCHEMA.name(),     FIELDELEMENT_SCHEMA.name(),
    };

    std::vector<MetaApplyContext> contexts;
    for (auto& name : collecton_names) {
        MetaApplyContext context;
        context.sql_ = "DELETE FROM " + name + ";";
        context.id_ = 0;

        contexts.push_back(context);
    }

    std::vector<snapshot::ID_TYPE> ids;
    return ExecuteTransaction(contexts, ids);
}

}  // namespace milvus::engine::meta
