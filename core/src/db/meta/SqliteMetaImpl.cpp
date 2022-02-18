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

#include "db/meta/SqliteMetaImpl.h"

#include <unistd.h>

#include <fiu-local.h>
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <unordered_map>

#include "MetaConsts.h"
#include "db/meta/MetaSchema.h"
#include "db/IDGenerator.h"
#include "db/Utils.h"
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

template<typename T>
void
DistributeBatch(const T& id_array, std::vector<std::vector<std::string>>& id_groups) {
    std::vector<std::string> temp_group;
    constexpr uint64_t SQL_BATCH_SIZE = 50;
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
    } else {
        std::string msg = desc + ":" + what;
        LOG_ENGINE_ERROR_ << msg;
        return Status(DB_META_TRANSACTION_FAILED, msg);
    }
}

std::string
ErrorMsg(sqlite3* db) {
    std::stringstream ss;
    ss << "(sqlite3 error code:" << sqlite3_errcode(db) << ", extend code: " << sqlite3_extended_errcode(db)
       << ", sys errno:" << sqlite3_system_errno(db) << ", sys err msg:" << strerror(errno)
       << ", msg:" << sqlite3_errmsg(db) << ")";
    return ss.str();
}

std::string
Quote(const std::string& str) {
    return "\'" + str + "\'";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Environment schema
static const MetaSchema ENVIRONMENT_SCHEMA(META_ENVIRONMENT, {
    MetaField("global_lsn", "INTEGER", "DEFAULT (0) NOT NULL"),
});

// Tables schema
static const MetaSchema TABLES_SCHEMA(META_TABLES, {
    MetaField("id", "INTEGER", "PRIMARY KEY NOT NULL"),
    MetaField("table_id", "TEXT", "UNIQUE NOT NULL"),
    MetaField("state", "INTEGER", "NOT NULL"),
    MetaField("dimension", "INTEGER", "NOT NULL"),
    MetaField("created_on", "INTEGER", "NOT NULL"),
    MetaField("flag", "INTEGER", "DEFAULT (0) NOT NULL"),
    MetaField("index_file_size", "INTEGER", "NOT NULL"),
    MetaField("engine_type", "INTEGER", "NOT NULL"),
    MetaField("index_params", "TEXT", "NOT NULL"),
    MetaField("metric_type", "INTEGER", "NOT NULL"),
    MetaField("owner_table", "TEXT", "DEFAULT ('') NOT NULL"),
    MetaField("partition_tag", "TEXT", "DEFAULT ('') NOT NULL"),
    MetaField("version", "TEXT",
              std::string("DEFAULT ('") + CURRENT_VERSION + "') NOT NULL"),
    MetaField("flush_lsn", "INTEGER", "NOT NULL"),
});

// TableFiles schema
static const MetaSchema TABLEFILES_SCHEMA(META_TABLEFILES, {
    MetaField("id", "INTEGER", "PRIMARY KEY NOT NULL"),
    MetaField("table_id", "TEXT", "NOT NULL"),
    MetaField("segment_id", "TEXT", "DEFAULT ('') NOT NULL"),
    MetaField("engine_type", "INTEGER", "NOT NULL"),
    MetaField("file_id", "TEXT", "NOT NULL"),
    MetaField("file_type", "INTEGER", "NOT NULL"),
    MetaField("file_size", "INTEGER", "DEFAULT (0) NOT NULL"),
    MetaField("row_count", "INTEGER", "DEFAULT (0) NOT NULL"),
    MetaField("updated_time", "INTEGER", "NOT NULL"),
    MetaField("created_on", "INTEGER", "NOT NULL"),
    MetaField("date", "INTEGER", "NOT NULL"),
    MetaField("flush_lsn", "INTEGER", "NOT NULL"),
});

// Fields schema
static const MetaSchema FIELDS_SCHEMA(META_FIELDS, {
    MetaField("collection_id", "TEXT", "NOT NULL"),
    MetaField("field_name", "TEXT", "NOT NULL"),
    MetaField("field_type", "INTEGER", "NOT NULL"),
    MetaField("field_params", "TEXT", "NOT NULL"),
});

/////////////////////////////////////////////////////
static AttrsMapList* QueryData = nullptr;

static int
QueryCallback(void* data, int argc, char** argv, char** azColName) {
    AttrsMap raw;
    for (size_t i = 0; i < argc; i++) {
        // TODO: here check argv[i]. Refer to 'https://www.tutorialspoint.com/sqlite/sqlite_c_cpp.htm'
        raw.insert(std::make_pair(azColName[i], argv[i]));
    }

    if (QueryData) {
        QueryData->push_back(raw);
    }

    return 0;
}

}  // namespace

SqliteMetaImpl::SqliteMetaImpl(const DBMetaOptions& options) : options_(options) {
    Initialize();
}

SqliteMetaImpl::~SqliteMetaImpl() {
    if (db_ != nullptr) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

Status
SqliteMetaImpl::NextCollectionId(std::string& collection_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    ss << id_generator.GetNextIDNumber();
    collection_id = ss.str();
    return Status::OK();
}

Status
SqliteMetaImpl::NextFileId(std::string& file_id) {
    std::lock_guard<std::mutex> lock(genid_mutex_);  // avoid duplicated id
    std::stringstream ss;
    SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    ss << id_generator.GetNextIDNumber();
    file_id = ss.str();
    return Status::OK();
}

void
SqliteMetaImpl::ValidateMetaSchema() {
    bool is_null_connector{db_ == nullptr};
    fiu_do_on("SqliteMetaImpl.ValidateMetaSchema.NullConnection", is_null_connector = true);
    if (is_null_connector) {
        throw Exception(DB_ERROR, "Connector is null pointer");
    }

    auto validate_schema = [&](const MetaSchema& schema) -> bool {
        auto& fields = schema.Fields();
        for (auto& field : fields) {
            const char *data_type = nullptr;
            const char *collseq = nullptr;
            int not_null = 0, primary_key = 0, autoinc = 0;
            int ret = sqlite3_table_column_metadata(db_, nullptr, schema.name().c_str(), field.name().c_str(),
                &data_type, &collseq, &not_null, &primary_key, &autoinc);
            if (ret == SQLITE_OK) {
                std::string str_type(data_type);
                if (str_type != field.type()) {
                    return false;
                }
                std::string settings = field.setting();
                if (primary_key) {
                    auto poz = settings.find("PRIMARY KEY");
                    if (poz == std::string::npos) {
                        return false;
                    }
                }
                if (not_null) {
                    auto poz = settings.find("NOT NULL");
                    if (poz == std::string::npos) {
                        return false;
                    }
                }
            }
        }

        return true;
    };

    // old meta could be recreated since schema changed, throw exception if meta schema is not compatible
    if (!validate_schema(TABLES_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Tables schema is created by Milvus old version");
    }

    if (!validate_schema(TABLEFILES_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta TableFiles schema is created by Milvus old version");
    }

    if (!validate_schema(FIELDS_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Fields schema is created by Milvus old version");
    }

    if (!validate_schema(ENVIRONMENT_SCHEMA)) {
        throw Exception(DB_INCOMPATIB_META, "Meta Environment schema is created by Milvus old version");
    }
}

Status
SqliteMetaImpl::SqlQuery(const std::string& sql, AttrsMapList* res) {
    try {
        std::lock_guard<std::mutex> meta_lock(sqlite_mutex_);

        int (* call_back)(void*, int, char**, char**) = nullptr;
        if (res) {
            QueryData = res;
            call_back = QueryCallback;
        }

        auto rc = sqlite3_exec(db_, sql.c_str(), call_back, nullptr, nullptr);
        QueryData = nullptr;
        if (rc != SQLITE_OK) {
            std::string err = ErrorMsg(db_);
            LOG_ENGINE_ERROR_ << err;
            return Status(DB_ERROR, err);
        }
    } catch (std::exception& e) {
        LOG_ENGINE_ERROR_ << e.what();
        return Status(DB_ERROR, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::SqlTransaction(const std::vector<std::string>& sql_statements) {
    try {
        std::lock_guard<std::mutex> meta_lock(sqlite_mutex_);

        if (SQLITE_OK != sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr)) {
            std::string sql_err = "Sqlite begin transaction failed: " + ErrorMsg(db_);
            return Status(DB_META_TRANSACTION_FAILED, sql_err);
        }

        int rc = SQLITE_OK;
        for (auto& sql : sql_statements) {
            rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, nullptr);
            if (rc != SQLITE_OK) {
                break;
            }
        }

        if (SQLITE_OK != rc || SQLITE_OK != sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr)) {
            std::string err = ErrorMsg(db_);
            LOG_ENGINE_ERROR_ << err;
            sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
            return Status(DB_META_TRANSACTION_FAILED, err);
        }
    } catch (std::exception& e) {
        return Status(DB_META_TRANSACTION_FAILED, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Initialize() {
    // create db path
    if (!boost::filesystem::is_directory(options_.path_)) {
        auto ret = boost::filesystem::create_directory(options_.path_);
        fiu_do_on("SqliteMetaImpl.Initialize.fail_create_directory", ret = false);
        if (!ret) {
            std::string msg = "Failed to create db directory " + options_.path_;
            LOG_ENGINE_ERROR_ << msg;
            throw Exception(DB_INVALID_PATH, msg);
        }
    }

    // open/create sqlite db
    std::string meta_path = options_.path_ + "/meta.sqlite";
    if (sqlite3_open(meta_path.c_str(), &db_) != SQLITE_OK) {
        std::string err = "Cannot open Sqlite database: " + ErrorMsg(db_);
        std::cerr << err << std::endl;
        throw std::runtime_error(err);
    }

    // set sqlite wal mode
    sqlite3_extended_result_codes(db_, 1);  // allow extend error code
    if (SQLITE_OK != sqlite3_exec(db_, "pragma journal_mode = WAL", nullptr, nullptr, nullptr)) {
        std::string errs = "Sqlite configure wal journal mode to WAL failed: " + ErrorMsg(db_);
        std::cerr << errs << std::endl;
        throw std::runtime_error(errs);
    }

    // validate exist schema
    ValidateMetaSchema();

    // create meta tables
    auto create_schema = [&](const MetaSchema& schema) {
        std::string create_table_str = "CREATE TABLE IF NOT EXISTS " + schema.name() + "(" + schema.ToString() + ");";
        LOG_ENGINE_DEBUG_ << "Initialize: " << create_table_str;
        std::vector<std::string> statements = {create_table_str};
        auto status = SqlTransaction(statements);
        if (!status.ok()) {
            std::string err = "Cannot create Sqlite table: ";
            err += status.message();
            throw std::runtime_error(err);
        }
    };

    create_schema(ENVIRONMENT_SCHEMA);
    create_schema(TABLES_SCHEMA);
    create_schema(TABLEFILES_SCHEMA);
    create_schema(FIELDS_SCHEMA);

    CleanUpShadowFiles();

    return Status::OK();
}

Status
SqliteMetaImpl::CreateCollection(CollectionSchema& collection_schema) {
    try {
        server::MetricCollector metric;

        if (collection_schema.collection_id_ == "") {
            NextCollectionId(collection_schema.collection_id_);
        } else {
            fiu_do_on("SqliteMetaImpl.CreateCollection.throw_exception", throw std::exception());
            std::string statement = "SELECT state FROM " + std::string(META_TABLES) + " WHERE table_id = "
                                    + Quote(collection_schema.collection_id_) + ";";
            LOG_ENGINE_DEBUG_ << "CreateCollection: " << statement;
            AttrsMapList res;
            auto status = SqlQuery(statement, &res);
            if (res.size() == 1) {
                if (res[0]["state"] == std::to_string(CollectionSchema::TO_DELETE)) {
                    return Status(DB_ERROR,
                                  "Collection already exists and it is in delete state, please wait a second");
                } else {
                    // Change from no error to already exist.
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

        std::string
            statement = "INSERT INTO " + std::string(META_TABLES) + " VALUES(" + id + ", " + Quote(collection_id)
                        + ", " + state + ", " + dimension + ", " + created_on + ", " + flag + ", "
                        + index_file_size + ", " + engine_type + ", " + Quote(index_params) + ", " + metric_type
                        + ", " + Quote(owner_collection) + ", " + Quote(partition_tag) + ", " + Quote(version)
                        + ", " + flush_lsn + ");";

        LOG_ENGINE_DEBUG_ << "CreateCollection: " << statement;

        fiu_do_on("SqliteMetaImpl.CreateCollection.insert_throw_exception", throw std::exception());
        auto status = SqlTransaction({statement});
        if (status.ok()) {
            collection_schema.id_ = sqlite3_last_insert_rowid(db_);
        } else {
            return HandleException("Failed to create collection", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Successfully create collection: " << collection_schema.collection_id_;
        return utils::CreateCollectionPath(options_, collection_schema.collection_id_);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection", e.what());
    }
}

Status
SqliteMetaImpl::DescribeCollection(CollectionSchema& collection_schema) {
    try {
        fiu_do_on("SqliteMetaImpl.DescribeCollection.throw_exception", throw std::exception());
        server::MetricCollector metric;
        std::string statement = "SELECT id, state, dimension, created_on, flag, index_file_size, engine_type,"
                                " index_params, metric_type ,owner_table, partition_tag, version, flush_lsn FROM "
                                + std::string(META_TABLES) + " WHERE table_id = "
                                + Quote(collection_schema.collection_id_) + " AND state <> "
                                + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "DescribeCollection: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (res.size() == 1) {
            auto& resRow = res[0];
            collection_schema.id_ = std::stoul(resRow["id"]);
            collection_schema.state_ = std::stoi(resRow["state"]);
            collection_schema.dimension_ = std::stoi(resRow["dimension"]);
            collection_schema.created_on_ = std::stol(resRow["created_on"]);
            collection_schema.flag_ = std::stol(resRow["flag"]);
            collection_schema.index_file_size_ = std::stol(resRow["index_file_size"]);
            collection_schema.engine_type_ = std::stol(resRow["engine_type"]);
            collection_schema.index_params_ = resRow["index_params"];
            collection_schema.metric_type_ = std::stol(resRow["metric_type"]);
            collection_schema.owner_collection_ = resRow["owner_table"];
            collection_schema.partition_tag_ = resRow["partition_tag"];
            collection_schema.version_ = resRow["version"];
            collection_schema.flush_lsn_ = std::stoul(resRow["flush_lsn"]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_schema.collection_id_ + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::HasCollection(const std::string& collection_id, bool& has_or_not, bool is_root) {
    has_or_not = false;

    try {
        fiu_do_on("SqliteMetaImpl.HasCollection.throw_exception", throw std::exception());
        server::MetricCollector metric;

        // since collection_id is a unique column we just need to check whether it exists or not
        std::string statement;
        if (is_root) {
            statement = "SELECT id FROM " + std::string(META_TABLES) + " WHERE table_id = " + Quote(collection_id)
                        + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE)
                        + " AND owner_table = ''" + ";";
        } else {
            statement = "SELECT id FROM " + std::string(META_TABLES) + " WHERE table_id = " + Quote(collection_id)
                        + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE) + ";";
        }
        LOG_ENGINE_DEBUG_ << "HasCollection: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        has_or_not = (res.size() > 0);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::AllCollections(std::vector<CollectionSchema>& collection_schema_array, bool is_root) {
    try {
        fiu_do_on("SqliteMetaImpl.AllCollections.throw_exception", throw std::exception());
        server::MetricCollector metric;
        std::string
            statement = "SELECT id, table_id, dimension, engine_type, index_params, index_file_size, metric_type,"
                        "owner_table, partition_tag, version, flush_lsn FROM " + Quote(META_TABLES)
                        + " WHERE state <> " + std::to_string(CollectionSchema::TO_DELETE);
        if (is_root) {
            statement += " AND owner_table = \"\";";
        } else {
            statement += ";";
        }
        LOG_ENGINE_DEBUG_ << "AllCollections: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        for (auto& resRow : res) {
            CollectionSchema collection_schema;
            collection_schema.id_ = std::stoul(resRow["id"]);
            collection_schema.collection_id_ = resRow["table_id"];
            collection_schema.dimension_ = std::stoi(resRow["dimension"]);
            collection_schema.index_file_size_ = std::stol(resRow["index_file_size"]);
            collection_schema.engine_type_ = std::stoi(resRow["engine_type"]);
            collection_schema.index_params_ = resRow["index_params"];
            collection_schema.metric_type_ = std::stoi(resRow["metric_type"]);
            collection_schema.owner_collection_ = resRow["owner_table"];
            collection_schema.partition_tag_ = resRow["partition_tag"];
            collection_schema.version_ = resRow["version"];
            collection_schema.flush_lsn_ = std::stoul(resRow["flush_lsn"]);

            collection_schema_array.emplace_back(collection_schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup all collections", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropCollections(const std::vector<std::string>& collection_id_array) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DropCollection.throw_exception", throw std::exception());

        // distribute id array to batches
        std::vector<std::vector<std::string>> id_groups;
        DistributeBatch(collection_id_array, id_groups);

        // soft delete collections
        std::vector<std::string> statements;
        for (auto group : id_groups) {
            std::string statement = "UPDATE " + std::string(META_TABLES) + " SET state = "
                                    + std::to_string(CollectionSchema::TO_DELETE) + " WHERE table_id in(";
            for (size_t i = 0; i < group.size(); i++) {
                statement += Quote(group[i]);
                if (i != group.size() - 1) {
                    statement += ",";
                }
            }
            statement += ");";
            statements.emplace_back(statement);
            LOG_ENGINE_DEBUG_ << "DropCollections: " << statement;
        }

        {
            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(operation_mutex_);

            auto status = SqlTransaction(statements);
            if (!status.ok()) {
                return HandleException("Failed to drop collections", status.message().c_str());
            }
        }

        auto status = DeleteCollectionFiles(collection_id_array);
        LOG_ENGINE_DEBUG_ << "Successfully delete collections";
        return status;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection", e.what());
    }
}

Status
SqliteMetaImpl::DeleteCollectionFiles(const std::vector<std::string>& collection_id_array) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DeleteCollectionFiles.throw_exception", throw std::exception());

        // distribute id array to batches
        std::vector<std::vector<std::string>> id_groups;
        DistributeBatch(collection_id_array, id_groups);

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        // soft delete collection files
        std::vector<std::string> statements;
        for (auto group : id_groups) {
            std::string statement = "UPDATE " + std::string(META_TABLEFILES) + " SET file_type = "
                                    + std::to_string(SegmentSchema::TO_DELETE) + " ,updated_time = "
                                    + std::to_string(utils::GetMicroSecTimeStamp()) + " WHERE table_id in (";
            for (size_t i = 0; i < group.size(); i++) {
                statement += Quote(group[i]);
                if (i != group.size() - 1) {
                    statement += ",";
                }
            }
            statement += (") AND file_type <> " + std::to_string(SegmentSchema::TO_DELETE) + ";");
            statements.emplace_back(statement);
            LOG_ENGINE_DEBUG_ << "DeleteCollectionFiles: " << statement;
        }

        auto status = SqlTransaction(statements);
        if (!status.ok()) {
            return HandleException("Failed to drop collection files", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Successfully delete collection files";
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreateCollectionFile(SegmentSchema& file_schema) {
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
        fiu_do_on("SqliteMetaImpl.CreateCollectionFile.throw_exception", throw std::exception());
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

        std::string statement = "INSERT INTO " + std::string(META_TABLEFILES) + " VALUES(" + id + ", "
                                + Quote(collection_id) + ", " + Quote(segment_id) + ", " + engine_type + ", "
                                + Quote(file_id) + ", " + file_type + ", " + file_size + ", " + row_count
                                + ", " + updated_time + ", " + created_on + ", " + date + ", " + flush_lsn + ");";
        LOG_ENGINE_DEBUG_ << "CreateCollectionFile: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        auto status = SqlTransaction({statement});
        if (!status.ok()) {
            return HandleException("Failed to create collection file", status.message().c_str());
        }

        file_schema.id_ = sqlite3_last_insert_rowid(db_);

        LOG_ENGINE_DEBUG_ << "Successfully create collection file, file id = " << file_schema.file_id_;
        return utils::CreateCollectionFilePath(options_, file_schema);
    } catch (std::exception& e) {
        return HandleException("Encounter exception when create collection file", e.what());
    }
}

Status
SqliteMetaImpl::GetCollectionFiles(const std::string& collection_id, const std::vector<size_t>& ids,
                                   FilesHolder& files_holder) {
    try {
        fiu_do_on("SqliteMetaImpl.GetCollectionFiles.throw_exception", throw std::exception());

        std::stringstream idSS;
        for (auto& id : ids) {
            idSS << "id = " << std::to_string(id) << " OR ";
        }
        std::string idStr = idSS.str();
        idStr = idStr.substr(0, idStr.size() - 4);  // remove the last " OR "

        std::string statement = "SELECT id, segment_id, engine_type, file_id, file_type, file_size,"
                                " row_count, date, created_on FROM " + std::string(META_TABLEFILES)
                                + " WHERE table_id = " + Quote(collection_id) + " AND (" + idStr + ")"
                                + " AND file_type <> " + std::to_string(SegmentSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "GetCollectionFiles: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        for (auto& resRow : res) {
            SegmentSchema file_schema;
            file_schema.id_ = std::stoul(resRow["id"]);
            file_schema.collection_id_ = collection_id;
            file_schema.segment_id_ = resRow["segment_id"];
            file_schema.index_file_size_ = collection_schema.index_file_size_;
            file_schema.engine_type_ = std::stoi(resRow["engine_type"]);
            file_schema.index_params_ = collection_schema.index_params_;
            file_schema.metric_type_ = collection_schema.metric_type_;
            file_schema.file_id_ = resRow["file_id"];
            file_schema.file_type_ = std::stoi(resRow["file_type"]);
            file_schema.file_size_ = std::stoul(resRow["file_size"]);
            file_schema.row_count_ = std::stoul(resRow["row_count"]);
            file_schema.date_ = std::stoi(resRow["date"]);
            file_schema.created_on_ = std::stol(resRow["created_on"]);
            file_schema.dimension_ = collection_schema.dimension_;

            utils::GetCollectionFilePath(options_, file_schema);
            files_holder.MarkFile(file_schema);
        }

        LOG_ENGINE_DEBUG_ << "Get " << res.size() << " files by id from collection " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetCollectionFilesBySegmentId(const std::string& segment_id, FilesHolder& files_holder) {
    try {
        std::string statement = "SELECT id, table_id, segment_id, engine_type, file_id, file_type, file_size,"
                                " row_count, date, created_on FROM " + std::string(META_TABLEFILES)
                                + " WHERE segment_id = " + Quote(segment_id) + " AND file_type <> "
                                + std::to_string(SegmentSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "GetCollectionFilesBySegmentId: " << statement;

        AttrsMapList res;
        {
            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(operation_mutex_);

            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }
        }

        if (!res.empty()) {
            CollectionSchema collection_schema;
            collection_schema.collection_id_ = res[0]["table_id"];
            auto status = DescribeCollection(collection_schema);
            if (!status.ok()) {
                return status;
            }

            for (auto& resRow : res) {
                SegmentSchema file_schema;
                file_schema.id_ = std::stoul(resRow["id"]);
                file_schema.collection_id_ = collection_schema.collection_id_;
                file_schema.segment_id_ = resRow["segment_id"];
                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.engine_type_ = std::stoi(resRow["engine_type"]);
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;
                file_schema.file_id_ = resRow["file_id"];
                file_schema.file_type_ = std::stoi(resRow["file_type"]);
                file_schema.file_size_ = std::stoul(resRow["file_size"]);
                file_schema.row_count_ = std::stoul(resRow["row_count"]);
                file_schema.date_ = std::stoi(resRow["date"]);
                file_schema.created_on_ = std::stol(resRow["created_on"]);
                file_schema.dimension_ = collection_schema.dimension_;

                utils::GetCollectionFilePath(options_, file_schema);
                files_holder.MarkFile(file_schema);
            }
        }

        LOG_ENGINE_DEBUG_ << "Get " << res.size() << " files by segment id" << segment_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup collection files by segment id", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFlag(const std::string& collection_id, int64_t flag) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFlag.throw_exception", throw std::exception());

        std::string statement = "UPDATE " + std::string(META_TABLES) + " SET flag = " + std::to_string(flag)
                                + " WHERE table_id = " + Quote(collection_id) + ";";
        LOG_ENGINE_DEBUG_ << "UpdateCollectionFlag: " << statement;

        auto status = SqlTransaction({statement});
        if (!status.ok()) {
            return HandleException("Failed to update collection flag", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Successfully update collection flag, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection flag: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFlushLSN(const std::string& collection_id, uint64_t flush_lsn) {
    try {
        server::MetricCollector metric;

        std::string statement = "UPDATE " + std::string(META_TABLES) + " SET flush_lsn = "
                                + std::to_string(flush_lsn) + " WHERE table_id = " + Quote(collection_id) + ";";
        LOG_ENGINE_DEBUG_ << "UpdateCollectionFlushLSN: " << statement;

        auto status = SqlTransaction({statement});
        if (!status.ok()) {
            return HandleException("Failed to update collection flush_lsn", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Successfully update collection flush_lsn, collection id = " << collection_id
                          << " flush_lsn = " << flush_lsn;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection lsn: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetCollectionFlushLSN(const std::string& collection_id, uint64_t& flush_lsn) {
    try {
        server::MetricCollector metric;

        std::string statement = "SELECT flush_lsn FROM " + std::string(META_TABLES) + " WHERE table_id = "
                                + Quote(collection_id) + ";";
        LOG_ENGINE_DEBUG_ << "GetCollectionFlushLSN: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() > 0) {
            flush_lsn = std::stoul(res[0]["flush_lsn"]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }

    } catch (std::exception& e) {
        return HandleException("Encounter exception when getting collection files by flush_lsn", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFile(SegmentSchema& file_schema) {
    file_schema.updated_time_ = utils::GetMicroSecTimeStamp();
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFile.throw_exception", throw std::exception());

        std::string statement = "SELECT state FROM " + std::string(META_TABLES) + " WHERE table_id = "
                                + Quote(file_schema.collection_id_) + ";";
        LOG_ENGINE_DEBUG_ << "UpdateCollectionFile: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        // if the collection has been deleted, just mark the collection file as TO_DELETE
        // clean thread will delete the file later
        if (res.size() < 1 || res[0]["state"] == std::to_string(CollectionSchema::TO_DELETE)) {
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

        statement = "UPDATE " + std::string(META_TABLEFILES) + " SET table_id = " + Quote(collection_id)
                    + " ,engine_type = " + engine_type + " ,file_id = " + Quote(file_id)
                    + " ,file_type = " + file_type + " ,file_size = " + file_size + " ,row_count = " + row_count
                    + " ,updated_time = " + updated_time + " ,created_on = " + created_on + " ,date = " + date
                    + " WHERE id = " + id + ";";
        LOG_ENGINE_DEBUG_ << "UpdateCollectionFile: " << statement;

        status = SqlTransaction({statement});
        if (!status.ok()) {
            return HandleException("Failed to update collection flush_lsn", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Update single collection file, file id = " << file_schema.file_id_;
    } catch (std::exception& e) {
        std::string msg = "Exception update collection file: collection_id = " + file_schema.collection_id_ +
                          " file_id = " + file_schema.file_id_;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFiles(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFiles.throw_exception", throw std::exception());

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        std::map<std::string, bool> has_collections;
        for (auto& file : files) {
            if (has_collections.find(file.collection_id_) != has_collections.end()) {
                continue;
            }

            std::string statement = "SELECT id FROM " + std::string(META_TABLES)
                                    + " WHERE table_id = " + Quote(file.collection_id_) + " AND state <> "
                                    + std::to_string(CollectionSchema::TO_DELETE) + ";";
            LOG_ENGINE_DEBUG_ << "UpdateCollectionFiles: " << statement;

            AttrsMapList res;
            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }

            if (res.size() >= 1) {
                has_collections[file.collection_id_] = true;
            } else {
                has_collections[file.collection_id_] = false;
            }
        }

        std::vector<std::string> statements;
        for (auto& file : files) {
            if (!has_collections[file.collection_id_]) {
                file.file_type_ = SegmentSchema::TO_DELETE;
            }

            file.updated_time_ = utils::GetMicroSecTimeStamp();

            std::string id = std::to_string(file.id_);
            std::string& collection_id = file.collection_id_;
            std::string engine_type = std::to_string(file.engine_type_);
            std::string& file_id = file.file_id_;
            std::string file_type = std::to_string(file.file_type_);
            std::string file_size = std::to_string(file.file_size_);
            std::string row_count = std::to_string(file.row_count_);
            std::string updated_time = std::to_string(file.updated_time_);
            std::string created_on = std::to_string(file.created_on_);
            std::string date = std::to_string(file.date_);

            std::string statement = "UPDATE " + std::string(META_TABLEFILES) + " SET table_id = "
                                    + Quote(collection_id) + " ,engine_type = " + engine_type + " ,file_id = "
                                    + Quote(file_id)
                                    + " ,file_type = " + file_type + " ,file_size = " + file_size
                                    + " ,row_count = " + row_count + " ,updated_time = " + updated_time
                                    + " ,created_on = " + created_on + " ,date = " + date + " WHERE id = " + id + ";";
            statements.emplace_back(statement);
            LOG_ENGINE_DEBUG_ << "UpdateCollectionFiles: " << statement;
        }

        auto status = SqlTransaction(statements);
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFiles.fail_commited", status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return HandleException("Failed to update collection flush_lsn", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Update " << files.size() << " collection files";
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFilesRowCount(SegmentsSchema& files) {
    try {
        server::MetricCollector metric;

        for (auto& file : files) {
            std::string row_count = std::to_string(file.row_count_);
            std::string updated_time = std::to_string(utils::GetMicroSecTimeStamp());

            std::string statement = "UPDATE " + std::string(META_TABLEFILES) + " SET row_count = " + row_count
                                    + " , updated_time = " + updated_time + " WHERE file_id = " + file.file_id_ + ";";
            LOG_ENGINE_DEBUG_ << "UpdateCollectionFilesRowCount: " << statement;

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(operation_mutex_);

            auto status = SqlTransaction({statement});
            if (!status.ok()) {
                return HandleException("Failed to update collection file row count", status.message().c_str());
            }

            LOG_ENGINE_DEBUG_ << "Update file " << file.file_id_ << " row count to " << file.row_count_;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files row count", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionIndex(const std::string& collection_id, const CollectionIndex& index) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionIndex.throw_exception", throw std::exception());

        std::string statement = "SELECT id, state, dimension, created_on FROM " + std::string(META_TABLES)
                                + " WHERE table_id = " + Quote(collection_id)
                                + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "UpdateCollectionIndex: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() == 1) {
            auto& resRow = res[0];

            auto id = resRow["id"];
            auto state = resRow["state"];
            auto dimension = resRow["dimension"];
            auto created_on = resRow["created_on"];

            statement = "UPDATE " + std::string(META_TABLES) + " SET id = " + id + " ,state = " + state
                        + " ,dimension = " + dimension + " ,created_on = " + created_on
                        + " ,engine_type = " + std::to_string(index.engine_type_) + " ,index_params = "
                        + Quote(index.extra_params_.dump()) + " ,metric_type = " + std::to_string(index.metric_type_)
                        + " WHERE table_id = " + Quote(collection_id) + ";";
            LOG_ENGINE_DEBUG_ << "UpdateCollectionIndex: " << statement;

            auto status = SqlTransaction({statement});
            if (!status.ok()) {
                return HandleException("Failed to update collection index", status.message().c_str());
            }
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }

        LOG_ENGINE_DEBUG_ << "Successfully update collection index, collection id = " << collection_id;
    } catch (std::exception& e) {
        std::string msg = "Encounter exception when update collection index: collection_id = " + collection_id;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::UpdateCollectionFilesToIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.UpdateCollectionFilesToIndex.throw_exception", throw std::exception());

        std::string statement = "UPDATE " + std::string(META_TABLEFILES) + " SET file_type = "
                                + std::to_string(SegmentSchema::TO_INDEX) + " WHERE table_id = " + Quote(collection_id)
                                + " AND row_count >= " + std::to_string(meta::BUILD_INDEX_THRESHOLD)
                                + " AND file_type = " + std::to_string(SegmentSchema::RAW) + ";";
        LOG_ENGINE_DEBUG_ << "UpdateCollectionFilesToIndex: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        auto status = SqlTransaction({statement});
        if (!status.ok()) {
            return HandleException("Failed to update collection files to index", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Update files to to_index, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when update collection files to to_index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DescribeCollectionIndex(const std::string& collection_id, CollectionIndex& index) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DescribeCollectionIndex.throw_exception", throw std::exception());

        std::string statement = "SELECT engine_type, index_params, metric_type FROM "
                                + std::string(META_TABLES) + " WHERE table_id = " + Quote(collection_id)
                                + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "DescribeCollectionIndex: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() == 1) {
            auto& resRow = res[0];

            index.engine_type_ = std::stoi(resRow["engine_type"]);
            std::string str_index_params = resRow["index_params"];
            index.extra_params_ = milvus::json::parse(str_index_params);
            index.metric_type_ = std::stoi(resRow["metric_type"]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when describe index", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropCollectionIndex(const std::string& collection_id) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.DropCollectionIndex.throw_exception", throw std::exception());

        std::vector<std::string> statements;
        // soft delete index files
        std::string statement = "UPDATE " + std::string(META_TABLEFILES) + " SET file_type = "
                                + std::to_string(SegmentSchema::TO_DELETE) + " ,updated_time = "
                                + std::to_string(utils::GetMicroSecTimeStamp()) + " WHERE table_id = "
                                + Quote(collection_id) + " AND file_type = " + std::to_string(SegmentSchema::INDEX)
                                + ";";
        statements.emplace_back(statement);
        LOG_ENGINE_DEBUG_ << "DropCollectionIndex: " << statement;

        // set all backup file to raw
        statement = "UPDATE " + std::string(META_TABLEFILES) + " SET file_type = "
                    + std::to_string(SegmentSchema::RAW) + " ,updated_time = "
                    + std::to_string(utils::GetMicroSecTimeStamp()) + " WHERE table_id = "
                    + Quote(collection_id) + " AND file_type = " + std::to_string(SegmentSchema::BACKUP) + ";";
        statements.emplace_back(statement);
        LOG_ENGINE_DEBUG_ << "DropCollectionIndex: " << statement;

        // set collection index type to raw
        statement = "UPDATE " + std::string(META_TABLES) + " SET engine_type = (CASE WHEN metric_type in ("
                    + std::to_string((int32_t)MetricType::HAMMING) + " ,"
                    + std::to_string((int32_t)MetricType::JACCARD) + " ,"
                    + std::to_string((int32_t)MetricType::TANIMOTO) + ")"
                    + " THEN " + std::to_string((int32_t)EngineType::FAISS_BIN_IDMAP)
                    + " ELSE " + std::to_string((int32_t)EngineType::FAISS_IDMAP) + " END)"
                    + " , index_params = '{}' WHERE table_id = " + Quote(collection_id) + ";";
        statements.emplace_back(statement);
        LOG_ENGINE_DEBUG_ << "DropCollectionIndex: " << statement;

        auto status = SqlTransaction(statements);
        if (!status.ok()) {
            return HandleException("Failed to drop collection index", status.message().c_str());
        }

        LOG_ENGINE_DEBUG_ << "Successfully drop collection index, collection id = " << collection_id;
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CreatePartition(const std::string& collection_id, const std::string& partition_name,
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
        return Status(DB_ERROR, "Nested partition is not allowed");
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
    if (status.code() == DB_ALREADY_EXIST) {
        return Status(DB_ALREADY_EXIST, "Partition already exists");
    }

    return status;
}

Status
SqliteMetaImpl::HasPartition(const std::string& collection_id, const std::string& tag, bool& has_or_not) {
    try {
        server::MetricCollector metric;

        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        std::string statement = "SELECT table_id FROM " + std::string(META_TABLES)
                                + " WHERE owner_table = " + Quote(collection_id)
                                + " AND partition_tag = " + Quote(valid_tag)
                                + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "HasPartition: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() > 0) {
            has_or_not = true;
        } else {
            has_or_not = false;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when lookup partition", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropPartition(const std::string& partition_name) {
    return DropCollections({partition_name});
}

Status
SqliteMetaImpl::ShowPartitions(const std::string& collection_id,
                               std::vector<meta::CollectionSchema>& partition_schema_array) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.ShowPartitions.throw_exception", throw std::exception());

        std::string statement = "SELECT table_id, id, state, dimension, created_on, flag, index_file_size,"
                                " engine_type, index_params, metric_type, partition_tag, version, flush_lsn FROM "
                                + std::string(META_TABLES) + " WHERE owner_table = "
                                + Quote(collection_id) + " AND state <> "
                                + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "ShowPartitions: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        for (auto& resRow : res) {
            meta::CollectionSchema partition_schema;
            partition_schema.collection_id_ = resRow["table_id"];
            partition_schema.id_ = std::stoul(resRow["id"]);
            partition_schema.state_ = std::stoi(resRow["state"]);
            partition_schema.dimension_ = std::stoi(resRow["dimension"]);
            partition_schema.created_on_ = std::stol(resRow["created_on"]);
            partition_schema.flag_ = std::stol(resRow["flag"]);
            partition_schema.index_file_size_ = std::stol(resRow["index_file_size"]);
            partition_schema.engine_type_ = std::stoi(resRow["engine_type"]);
            partition_schema.index_params_ = resRow["index_params"];
            partition_schema.metric_type_ = std::stoi(resRow["metric_type"]);
            partition_schema.owner_collection_ = collection_id;
            partition_schema.partition_tag_ = resRow["partition_tag"];
            partition_schema.version_ = resRow["version"];
            partition_schema.flush_lsn_ = std::stoul(resRow["flush_lsn"]);

            partition_schema_array.emplace_back(partition_schema);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when show partitions", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CountPartitions(const std::string& collection_id, int64_t& partition_count) {
    try {
        partition_count = 0;

        std::string statement = "SELECT count(*) FROM " + std::string(META_TABLES)
                                + " WHERE owner_table = " + Quote(collection_id)
                                + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "CountPartitions: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() == 1) {
            partition_count = std::stol(res[0]["count(*)"]);
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when count partitions", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetPartitionName(const std::string& collection_id, const std::string& tag,
                                 std::string& partition_name) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.GetPartitionName.throw_exception", throw std::exception());

        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        server::StringHelpFunctions::TrimStringBlank(valid_tag);

        std::string statement = "SELECT table_id FROM " + std::string(META_TABLES)
                                + " WHERE owner_table = " + Quote(collection_id)
                                + " AND partition_tag = " + Quote(valid_tag)
                                + " AND state <> " + std::to_string(CollectionSchema::TO_DELETE) + ";";
        LOG_ENGINE_DEBUG_ << "GetPartitionName: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() > 0) {
            partition_name = res[0]["table_id"];
        } else {
            return Status(DB_NOT_FOUND, "Collection " + collection_id + "'s partition " + valid_tag + " not found");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when get partition name", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::FilesToSearch(const std::string& collection_id, FilesHolder& files_holder, 
                              bool is_all_search_file) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.FilesToSearch.throw_exception", throw std::exception());

        std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, date,"
                                " engine_type, created_on, updated_time FROM " + std::string(META_TABLEFILES)
                                + " WHERE table_id = " + Quote(collection_id);
        if (is_all_search_file) {
            statement = statement + " AND (file_type = " + std::to_string(SegmentSchema::RAW)
                                    + " OR file_type = " + std::to_string(SegmentSchema::TO_INDEX)
                                    + " OR file_type = " + std::to_string(SegmentSchema::INDEX) + ");";
        } else {
            statement += (" AND file_type = " + std::to_string(SegmentSchema::INDEX) + ";");
        }
        LOG_ENGINE_DEBUG_ << "FilesToSearch: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        Status ret;
        int64_t files_count = 0;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.id_ = std::stoul(resRow["id"]);
            collection_file.collection_id_ = resRow["table_id"];
            collection_file.segment_id_ = resRow["segment_id"];
            collection_file.file_id_ = resRow["file_id"];
            collection_file.file_type_ = std::stoi(resRow["file_type"]);
            collection_file.file_size_ = std::stoul(resRow["file_size"]);
            collection_file.row_count_ = std::stoul(resRow["row_count"]);
            collection_file.date_ = std::stoi(resRow["date"]);
            collection_file.engine_type_ = std::stoi(resRow["engine_type"]);
            collection_file.created_on_ = std::stol(resRow["created_on"]);
            collection_file.updated_time_ = std::stol(resRow["updated_time"]);

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
        return HandleException("Encounter exception when iterate index files", e.what());
    }
}

Status
SqliteMetaImpl::FilesToSearchEx(const std::string& root_collection, const std::set<std::string>& partition_id_array,
                                FilesHolder& files_holder, bool is_all_search_file) {
    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.FilesToSearch.throw_exception", throw std::exception());

        // get root collection information
        CollectionSchema collection_schema;
        collection_schema.collection_id_ = root_collection;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        // distribute id array to batches
        const uint64_t batch_size = 50;
        std::vector<std::vector<std::string>> id_groups;
        std::vector<std::string> temp_group;
        for (auto& id : partition_id_array) {
            temp_group.push_back(id);
            if (temp_group.size() >= batch_size) {
                id_groups.emplace_back(temp_group);
                temp_group.clear();
            }
        }

        if (!temp_group.empty()) {
            id_groups.emplace_back(temp_group);
        }

        // perform query batch by batch
        int64_t files_count = 0;
        Status ret;
        for (auto group : id_groups) {
            std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, "
                                    "date, engine_type, created_on, updated_time FROM " + Quote(META_TABLEFILES)
                                    + " WHERE table_id in (";
            for (size_t i = 0; i < group.size(); i++) {
                statement += Quote(group[i]);
                if (i != group.size() - 1) {
                    statement += ",";
                }
            }
            statement += ")";
            if (is_all_search_file) {
                statement += (" AND (file_type = " + std::to_string(SegmentSchema::RAW));
                statement += (" OR file_type = " + std::to_string(SegmentSchema::TO_INDEX));
                statement += (" OR file_type = " + std::to_string(SegmentSchema::INDEX) + ");");
            } else {
                statement += (" AND file_type = " + std::to_string(SegmentSchema::INDEX) + ";");
            }
            LOG_ENGINE_DEBUG_ << "FilesToSearchEx: " << statement;

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(operation_mutex_);

            AttrsMapList res;
            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }

            for (auto& resRow : res) {
                SegmentSchema collection_file;
                collection_file.id_ = std::stoul(resRow["id"]);
                collection_file.collection_id_ = resRow["table_id"];
                collection_file.segment_id_ = resRow["segment_id"];
                collection_file.file_id_ = resRow["file_id"];
                collection_file.file_type_ = std::stoi(resRow["file_type"]);
                collection_file.file_size_ = std::stoul(resRow["file_size"]);
                collection_file.row_count_ = std::stoul(resRow["row_count"]);
                collection_file.date_ = std::stoi(resRow["date"]);
                collection_file.engine_type_ = std::stoi(resRow["engine_type"]);
                collection_file.created_on_ = std::stol(resRow["created_on"]);
                collection_file.updated_time_ = std::stol(resRow["updated_time"]);

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
        return HandleException("Encounter exception when iterate index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::FilesToMerge(const std::string& collection_id, FilesHolder& files_holder) {
    try {
        fiu_do_on("SqliteMetaImpl.FilesToMerge.throw_exception", throw std::exception());

        server::MetricCollector metric;

        // check collection existence
        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        auto status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        // get files to merge
        std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, "
                                "date, engine_type, created_on, updated_time FROM " + Quote(META_TABLEFILES)
                                + " WHERE table_id = " + Quote(collection_id)
                                + " AND file_type = " + std::to_string(SegmentSchema::RAW)
                                + " ORDER BY row_count DESC;";
        LOG_ENGINE_DEBUG_ << "FilesToMerge: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        Status ret;
        SegmentsSchema files;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.file_size_ = std::stoul(resRow["file_size"]);
            if ((int64_t)(collection_file.file_size_) >= collection_schema.index_file_size_) {
                continue;  // skip large file
            }

            collection_file.id_ = std::stoul(resRow["id"]);
            collection_file.collection_id_ = resRow["table_id"];
            collection_file.segment_id_ = resRow["segment_id"];
            collection_file.file_id_ = resRow["file_id"];
            collection_file.file_type_ = std::stoi(resRow["file_type"]);
            collection_file.file_size_ = std::stoul(resRow["file_size"]);
            collection_file.row_count_ = std::stoul(resRow["row_count"]);
            collection_file.date_ = std::stoi(resRow["date"]);
            collection_file.engine_type_ = std::stoi(resRow["engine_type"]);
            collection_file.created_on_ = std::stol(resRow["created_on"]);
            collection_file.updated_time_ = std::stol(resRow["updated_time"]);

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
        return HandleException("Encounter exception when iterate merge files", e.what());
    }
}

Status
SqliteMetaImpl::FilesToIndex(FilesHolder& files_holder) {
    try {
        fiu_do_on("SqliteMetaImpl.FilesToIndex.throw_exception", throw std::exception());
        server::MetricCollector metric;

        std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, "
                                "date, engine_type, created_on, updated_time FROM " + Quote(META_TABLEFILES)
                                + " WHERE file_type = " + std::to_string(SegmentSchema::TO_INDEX) + ";";
        // LOG_ENGINE_DEBUG_ << "FilesToIndex: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        Status ret;
        int64_t files_count = 0;
        std::map<std::string, CollectionSchema> groups;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.id_ = std::stoul(resRow["id"]);
            collection_file.collection_id_ = resRow["table_id"];
            collection_file.segment_id_ = resRow["segment_id"];
            collection_file.file_id_ = resRow["file_id"];
            collection_file.file_type_ = std::stoi(resRow["file_type"]);
            collection_file.file_size_ = std::stoul(resRow["file_size"]);
            collection_file.row_count_ = std::stol(resRow["row_count"]);
            collection_file.date_ = std::stoi(resRow["date"]);
            collection_file.engine_type_ = std::stoi(resRow["engine_type"]);
            collection_file.created_on_ = std::stol(resRow["created_on"]);
            collection_file.updated_time_ = std::stol(resRow["updated_time"]);

            auto groupItr = groups.find(collection_file.collection_id_);
            if (groupItr == groups.end()) {
                CollectionSchema collection_schema;
                collection_schema.collection_id_ = collection_file.collection_id_;
                auto status = DescribeCollection(collection_schema);
                fiu_do_on("SqliteMetaImpl_FilesToIndex_CollectionNotFound",
                          status = Status(DB_NOT_FOUND, "collection not found"));
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
        return HandleException("Encounter exception when iterate raw files", e.what());
    }
}

Status
SqliteMetaImpl::FilesByType(const std::string& collection_id, const std::vector<int>& file_types,
                            FilesHolder& files_holder) {
    if (file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    Status ret = Status::OK();

    try {
        fiu_do_on("SqliteMetaImpl.FilesByType.throw_exception", throw std::exception());

        std::string types;
        for (auto type : file_types) {
            if (!types.empty()) {
                types += ",";
            }
            types += std::to_string(type);
        }

        // since collection_id is a unique column we just need to check whether it exists or not
        std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, "
                                "date, engine_type, created_on, updated_time FROM " + Quote(META_TABLEFILES)
                                + " WHERE table_id = " + Quote(collection_id)
                                + " AND file_type in (" + types + ");";
        LOG_ENGINE_DEBUG_ << "FilesByType: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        CollectionSchema collection_schema;
        collection_schema.collection_id_ = collection_id;
        status = DescribeCollection(collection_schema);
        if (!status.ok()) {
            return status;
        }

        if (res.size() > 0) {
            int raw_count = 0, new_count = 0, new_merge_count = 0, new_index_count = 0;
            int to_index_count = 0, index_count = 0, backup_count = 0;
            for (auto& resRow : res) {
                SegmentSchema file_schema;
                file_schema.id_ = std::stoul(resRow["id"]);
                file_schema.collection_id_ = collection_id;
                file_schema.segment_id_ = resRow["segment_id"];
                file_schema.file_id_ = resRow["file_id"];
                file_schema.file_type_ = std::stoi(resRow["file_type"]);
                file_schema.file_size_ = std::stoul(resRow["file_size"]);
                file_schema.row_count_ = std::stoul(resRow["row_count"]);
                file_schema.date_ = std::stoi(resRow["date"]);
                file_schema.engine_type_ = std::stoi(resRow["engine_type"]);
                file_schema.created_on_ = std::stol(resRow["created_on"]);
                file_schema.updated_time_ = std::stol(resRow["updated_time"]);

                file_schema.index_file_size_ = collection_schema.index_file_size_;
                file_schema.index_params_ = collection_schema.index_params_;
                file_schema.metric_type_ = collection_schema.metric_type_;
                file_schema.dimension_ = collection_schema.dimension_;

                auto status = utils::GetCollectionFilePath(options_, file_schema);
                if (!status.ok()) {
                    ret = status;
                }

                files_holder.MarkFile(file_schema);

                int32_t file_type = file_schema.file_type_;
                switch (file_type) {
                    case (int)SegmentSchema::RAW:++raw_count;
                        break;
                    case (int)SegmentSchema::NEW:++new_count;
                        break;
                    case (int)SegmentSchema::NEW_MERGE:++new_merge_count;
                        break;
                    case (int)SegmentSchema::NEW_INDEX:++new_index_count;
                        break;
                    case (int)SegmentSchema::TO_INDEX:++to_index_count;
                        break;
                    case (int)SegmentSchema::INDEX:++index_count;
                        break;
                    case (int)SegmentSchema::BACKUP:++backup_count;
                        break;
                    default:break;
                }
            }

            std::string msg = "Get collection files by type.";
            for (int file_type : file_types) {
                switch (file_type) {
                    case (int)SegmentSchema::RAW:msg = msg + " raw files:" + std::to_string(raw_count);
                        break;
                    case (int)SegmentSchema::NEW:msg = msg + " new files:" + std::to_string(new_count);
                        break;
                    case (int)SegmentSchema::NEW_MERGE:
                        msg = msg + " new_merge files:" + std::to_string(new_merge_count);
                        break;
                    case (int)SegmentSchema::NEW_INDEX:
                        msg = msg + " new_index files:" + std::to_string(new_index_count);
                        break;
                    case (int)SegmentSchema::TO_INDEX:msg = msg + " to_index files:" + std::to_string(to_index_count);
                        break;
                    case (int)SegmentSchema::INDEX:msg = msg + " index files:" + std::to_string(index_count);
                        break;
                    case (int)SegmentSchema::BACKUP:msg = msg + " backup files:" + std::to_string(backup_count);
                        break;
                    default:break;
                }
            }
            LOG_ENGINE_DEBUG_ << msg;
        }

        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files by type", e.what());
    }
}

Status
SqliteMetaImpl::FilesByTypeEx(const std::vector<meta::CollectionSchema>& collections,
                              const std::vector<int>& file_types, FilesHolder& files_holder) {
    if (file_types.empty()) {
        return Status(DB_ERROR, "file types array is empty");
    }

    Status ret = Status::OK();

    try {
        fiu_do_on("SqliteMetaImpl.FilesByTypeEx.throw_exception", throw std::exception());

        // distribute id array to batches
        const uint64_t batch_size = 50;
        std::vector<std::vector<std::string>> id_groups;
        std::vector<std::string> temp_group;
        std::unordered_map<std::string, meta::CollectionSchema> map_collections;
        for (auto& collection : collections) {
            map_collections.insert(std::make_pair(collection.collection_id_, collection));
            temp_group.push_back(collection.collection_id_);
            if (temp_group.size() >= batch_size) {
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
            std::string types;
            for (auto type : file_types) {
                if (!types.empty()) {
                    types += ",";
                }
                types += std::to_string(type);
            }

            // since collection_id is a unique column we just need to check whether it exists or not
            std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, "
                                    "date, engine_type, created_on, updated_time FROM " + Quote(META_TABLEFILES)
                                    + " WHERE table_id in (";
            for (size_t i = 0; i < group.size(); i++) {
                statement += Quote(group[i]);
                if (i != group.size() - 1) {
                    statement += ",";
                }
            }
            statement += (") AND file_type in (" + types + ");");
            LOG_ENGINE_DEBUG_ << "FilesByTypeEx: " << statement;

            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(operation_mutex_);

            AttrsMapList res;
            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }

            for (auto& resRow : res) {
                SegmentSchema file_schema;
                file_schema.id_ = std::stoul(resRow["id"]);
                file_schema.collection_id_ = resRow["table_id"];
                file_schema.segment_id_ = resRow["segment_id"];
                file_schema.file_id_ = resRow["file_id"];
                file_schema.file_type_ = std::stoi(resRow["file_type"]);
                file_schema.file_size_ = std::stoul(resRow["file_size"]);
                file_schema.row_count_ = std::stoul(resRow["row_count"]);
                file_schema.date_ = std::stoi(resRow["date"]);
                file_schema.engine_type_ = std::stoi(resRow["engine_type"]);
                file_schema.created_on_ = std::stol(resRow["created_on"]);
                file_schema.updated_time_ = std::stol(resRow["updated_time"]);

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

                int32_t file_type = file_schema.file_type_;
                switch (file_type) {
                    case (int)SegmentSchema::RAW:++raw_count;
                        break;
                    case (int)SegmentSchema::NEW:++new_count;
                        break;
                    case (int)SegmentSchema::NEW_MERGE:++new_merge_count;
                        break;
                    case (int)SegmentSchema::NEW_INDEX:++new_index_count;
                        break;
                    case (int)SegmentSchema::TO_INDEX:++to_index_count;
                        break;
                    case (int)SegmentSchema::INDEX:++index_count;
                        break;
                    case (int)SegmentSchema::BACKUP:++backup_count;
                        break;
                    default:break;
                }
            }
        }

        std::string msg = "Get collection files by type.";
        for (int file_type : file_types) {
            switch (file_type) {
                case (int)SegmentSchema::RAW:msg = msg + " raw files:" + std::to_string(raw_count);
                    break;
                case (int)SegmentSchema::NEW:msg = msg + " new files:" + std::to_string(new_count);
                    break;
                case (int)SegmentSchema::NEW_MERGE:msg = msg + " new_merge files:" + std::to_string(new_merge_count);
                    break;
                case (int)SegmentSchema::NEW_INDEX:msg = msg + " new_index files:" + std::to_string(new_index_count);
                    break;
                case (int)SegmentSchema::TO_INDEX:msg = msg + " to_index files:" + std::to_string(to_index_count);
                    break;
                case (int)SegmentSchema::INDEX:msg = msg + " index files:" + std::to_string(index_count);
                    break;
                case (int)SegmentSchema::BACKUP:msg = msg + " backup files:" + std::to_string(backup_count);
                    break;
                default:break;
            }
        }

        LOG_ENGINE_DEBUG_ << msg;
        return ret;
    } catch (std::exception& e) {
        return HandleException("Failed to get files by type", e.what());
    }
}

Status
SqliteMetaImpl::FilesByID(const std::vector<size_t>& ids, FilesHolder& files_holder) {
    if (ids.empty()) {
        return Status::OK();
    }

    try {
        server::MetricCollector metric;
        fiu_do_on("SqliteMetaImpl.FilesByID.throw_exception", throw std::exception());

        std::string statement = "SELECT id, table_id, segment_id, file_id, file_type, file_size, row_count, "
                                "date, engine_type, created_on, updated_time FROM " + Quote(META_TABLEFILES);

        std::stringstream idSS;
        for (auto& id : ids) {
            idSS << "id = " << std::to_string(id) << " OR ";
        }
        std::string idStr = idSS.str();
        idStr = idStr.substr(0, idStr.size() - 4);  // remove the last " OR "

        statement += (" WHERE (" + idStr + ")");
        LOG_ENGINE_DEBUG_ << "FilesByID: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        std::map<std::string, meta::CollectionSchema> collections;
        Status ret;
        int64_t files_count = 0;
        for (auto& resRow : res) {
            SegmentSchema collection_file;
            collection_file.id_ = std::stoul(resRow["id"]);
            collection_file.collection_id_ = resRow["table_id"];
            collection_file.segment_id_ = resRow["segment_id"];
            collection_file.file_id_ = resRow["file_id"];
            collection_file.file_type_ = std::stoi(resRow["file_type"]);
            collection_file.file_size_ = std::stoul(resRow["file_size"]);
            collection_file.row_count_ = std::stoul(resRow["row_count"]);
            collection_file.date_ = std::stoi(resRow["date"]);
            collection_file.engine_type_ = std::stoi(resRow["engine_type"]);
            collection_file.created_on_ = std::stol(resRow["created_on"]);
            collection_file.updated_time_ = std::stol(resRow["updated_time"]);

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
        return HandleException("Encounter exception when iterate index files", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Archive() {
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
                fiu_do_on("SqliteMetaImpl.Archive.throw_exception", throw std::exception());

                std::string statement = "UPDATE " + std::string(META_TABLEFILES)
                          + " SET file_type = " + std::to_string(SegmentSchema::TO_DELETE)
                          + " WHERE created_on < " + std::to_string(now - usecs)
                          + " AND file_type <> " + std::to_string(SegmentSchema::TO_DELETE) + ";";

                auto status = SqlTransaction({statement});
                if (!status.ok()) {
                    return HandleException("Failed to archive", status.message().c_str());
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
SqliteMetaImpl::Size(uint64_t& result) {
    result = 0;
    try {
        fiu_do_on("SqliteMetaImpl.Size.throw_exception", throw std::exception());

        std::string statement = "SELECT IFNULL(SUM(file_size),0) AS sum FROM " + Quote(META_TABLEFILES)
                                + " WHERE file_type <> " + std::to_string(SegmentSchema::TO_DELETE) + ";";

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.empty()) {
            result = 0;
        } else {
            result = std::stoul(res[0]["sum"]);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when calculate db size", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpShadowFiles() {
    try {
        server::MetricCollector metric;

        std::string statement = "DELETE FROM " + std::string(META_TABLEFILES) + " WHERE file_type IN ("
                                + std::to_string(SegmentSchema::NEW) + ","
                                + std::to_string(SegmentSchema::NEW_MERGE) + ","
                                + std::to_string(SegmentSchema::NEW_INDEX) + ");";
        LOG_ENGINE_DEBUG_ << "CleanUpShadowFiles: " << statement;

        auto status = SqlTransaction({statement});
        fiu_do_on("SqliteMetaImpl.CleanUpShadowFiles.fail_commited", status = Status(DB_ERROR, ""));
        fiu_do_on("SqliteMetaImpl.CleanUpShadowFiles.throw_exception", throw std::exception());
        if (!status.ok()) {
            return HandleException("CleanUp error: sqlite transaction failed");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection file", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::CleanUpFilesWithTTL(uint64_t seconds /*, CleanUpFilter* filter*/) {
    auto now = utils::GetMicroSecTimeStamp();
    std::set<std::string> collection_ids;
    std::map<std::string, SegmentSchema> segment_ids;

    // remove to_delete files
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_ThrowException", throw std::exception());

        server::MetricCollector metric;

        std::vector<int> file_types = {
            (int)SegmentSchema::TO_DELETE,
            (int)SegmentSchema::BACKUP,
        };

        // collect files to be deleted
        std::string statement = "SELECT id, table_id, segment_id, engine_type, file_id, file_type, date"
                                " FROM " + std::string(META_TABLEFILES) + " WHERE file_type IN ("
                                + std::to_string(SegmentSchema::TO_DELETE) + ","
                                + std::to_string(SegmentSchema::BACKUP) + ")"
                                + " AND updated_time < " + std::to_string(now - seconds * US_PS) + ";";


        AttrsMapList res;
        {
            // to ensure UpdateCollectionFiles to be a atomic operation
            std::lock_guard<std::mutex> meta_lock(operation_mutex_);

            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }
        }

        SegmentSchema collection_file;
        std::vector<std::string> delete_ids;

        int64_t clean_files = 0;
        for (auto& resRow : res) {
            collection_file.id_ = std::stoul(resRow["id"]);
            collection_file.collection_id_ = resRow["table_id"];
            collection_file.segment_id_ = resRow["segment_id"];
            collection_file.engine_type_ = std::stoi(resRow["engine_type"]);
            collection_file.file_id_ = resRow["file_id"];
            collection_file.date_ = std::stoi(resRow["date"]);
            collection_file.file_type_ = std::stoi(resRow["file_type"]);

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

        if (clean_files > 0) {
            LOG_ENGINE_DEBUG_ << "Clean " << clean_files << " files expired in " << seconds << " seconds";
        }

        // delete file from meta
        std::vector<std::string> statements;
        if (!delete_ids.empty()) {
            // distribute id array to batches
            // sqlite could not parse long sql statement
            std::vector<std::vector<std::string>> id_groups;
            DistributeBatch(delete_ids, id_groups);

            for (auto& group : id_groups) {
                std::stringstream idsToDeleteSS;
                for (auto& id : group) {
                    idsToDeleteSS << "id = " << id << " OR ";
                }

                std::string idsToDeleteStr = idsToDeleteSS.str();
                idsToDeleteStr = idsToDeleteStr.substr(0, idsToDeleteStr.size() - 4);  // remove the last " OR "
                statement = "DELETE FROM " + std::string(META_TABLEFILES) + " WHERE " + idsToDeleteStr + ";";
                statements.emplace_back(statement);
                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement;
            }
        }

        auto status = SqlTransaction(statements);
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveFile_FailCommited", status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return HandleException("CleanUpFilesWithTTL error: sqlite transaction failed");
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection files", e.what());
    }

    // remove to_delete collections
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_ThrowException", throw std::exception());
        server::MetricCollector metric;

        std::string statement = "SELECT id, table_id FROM " + std::string(META_TABLES)
                                + " WHERE state = " + std::to_string(CollectionSchema::TO_DELETE) + ";";

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        int64_t remove_collections = 0;
        if (!res.empty()) {
            for (auto& resRow : res) {
                std::string collection_id;
                collection_id = resRow["table_id"];

                utils::DeleteCollectionPath(options_, collection_id, false);  // only delete empty folder
                ++remove_collections;

                statement = "DELETE FROM " + std::string(META_TABLES) + " WHERE id = " + resRow["id"] + ";";
                LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement;
                status = SqlTransaction({statement});
                if (!status.ok()) {
                    return HandleException("Failed to clean up with ttl", status.message().c_str());
                }
            }
        }

        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollection_Failcommited", throw std::exception());

        if (remove_collections > 0) {
            LOG_ENGINE_DEBUG_ << "Remove " << remove_collections << " collections from meta";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when clean collection files", e.what());
    }

    // remove deleted collection folder
    // don't remove collection folder until all its files has been deleted
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveCollectionFolder_ThrowException", throw std::exception());
        server::MetricCollector metric;

        int64_t remove_collections = 0;
        for (auto& collection_id : collection_ids) {
            std::string statement = "SELECT file_id FROM " + std::string(META_TABLEFILES)
                + " WHERE table_id = " + Quote(collection_id) + ";";

            AttrsMapList res;
            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }

            if (res.empty()) {
                utils::DeleteCollectionPath(options_, collection_id);
                ++remove_collections;
            }
        }

        if (remove_collections) {
            LOG_ENGINE_DEBUG_ << "Remove " << remove_collections << " collections folder";
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    // remove deleted segment folder
    // don't remove segment folder until all its files has been deleted
    try {
        fiu_do_on("SqliteMetaImpl.CleanUpFilesWithTTL.RemoveSegmentFolder_ThrowException", throw std::exception());
        server::MetricCollector metric;

        int64_t remove_segments = 0;
        for (auto& segment_id : segment_ids) {
            std::string statement = "SELECT id FROM " + std::string(META_TABLEFILES)
                + " WHERE segment_id = " + Quote(segment_id.first) + ";";
            LOG_ENGINE_DEBUG_ << "CleanUpFilesWithTTL: " << statement;

            AttrsMapList res;
            auto status = SqlQuery(statement, &res);
            if (!status.ok()) {
                return status;
            }

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
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::Count(const std::string& collection_id, uint64_t& result) {
    try {
        fiu_do_on("SqliteMetaImpl.Count.throw_exception", throw std::exception());
        server::MetricCollector metric;

        std::string statement = "SELECT row_count FROM " + std::string(META_TABLEFILES)
            + " WHERE table_id = " + Quote(collection_id)
            + " AND (file_type = " + std::to_string(SegmentSchema::RAW)
            + " OR file_type = " + std::to_string(SegmentSchema::TO_INDEX)
            + " OR file_type = " + std::to_string(SegmentSchema::INDEX) + ");";
        LOG_ENGINE_DEBUG_ << "Count: " << statement;

        // to ensure UpdateCollectionFiles to be a atomic operation
        std::lock_guard<std::mutex> meta_lock(operation_mutex_);

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        result = 0;
        for (auto& resRow : res) {
            size_t size = std::stoul(resRow["row_count"]);
            result += size;
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when calculate collection file size", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DropAll() {
    LOG_ENGINE_DEBUG_ << "Drop all sqlite meta";

    try {
        std::string statement = "DROP TABLE IF EXISTS ";
        std::vector<std::string> statements = {
            statement + TABLES_SCHEMA.name() + ";",
            statement + TABLEFILES_SCHEMA.name() + ";",
            statement + ENVIRONMENT_SCHEMA.name() + ";",
            statement + FIELDS_SCHEMA.name() + ";",
        };

        for (auto& sql : statements) {
            LOG_ENGINE_DEBUG_ << "DropAll: " << sql;
        }

        auto status = SqlTransaction(statements);
        if (!status.ok()) {
            return HandleException("Failed to drop all", status.message().c_str());
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when drop all meta", e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::DiscardFiles(int64_t to_discard_size) {
    if (to_discard_size <= 0) {
        return Status::OK();
    }

    LOG_ENGINE_DEBUG_ << "About to discard size=" << to_discard_size;

    try {
        fiu_do_on("SqliteMetaImpl.DiscardFiles.throw_exception", throw std::exception());
        server::MetricCollector metric;

        std::string statement = "SELECT id, file_size FROM " + std::string(META_TABLEFILES)
            + " WHERE file_type <> " + std::to_string(SegmentSchema::TO_DELETE)
            + " ORDER BY id ASC LIMIT 10;";
        LOG_ENGINE_DEBUG_ << "DiscardFiles: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.empty()) {
            return Status::OK();
        }

        SegmentSchema collection_file;
        std::stringstream idsToDiscardSS;
        for (auto& resRow : res) {
            if (to_discard_size <= 0) {
                break;
            }
            collection_file.id_ = std::stoul(resRow["id"]);
            collection_file.file_size_ = std::stoul(resRow["file_size"]);
            idsToDiscardSS << "id = " << std::to_string(collection_file.id_) << " OR ";
            LOG_ENGINE_DEBUG_ << "Discard file id=" << collection_file.file_id_
                              << " file size=" << collection_file.file_size_;
            to_discard_size -= collection_file.file_size_;
        }

        std::string idsToDiscardStr = idsToDiscardSS.str();
        idsToDiscardStr = idsToDiscardStr.substr(0, idsToDiscardStr.size() - 4);  // remove the last " OR "

        statement = "UPDATE " + std::string(META_TABLEFILES)
            + " SET file_type = " + std::to_string(SegmentSchema::TO_DELETE)
            + " ,updated_time = " + std::to_string(utils::GetMicroSecTimeStamp())
            + " WHERE " + idsToDiscardStr + ";";
        LOG_ENGINE_DEBUG_ << "DiscardFiles: " << statement;

        status = SqlTransaction({statement});
        fiu_do_on("SqliteMetaImpl.DiscardFiles.fail_commited", status = Status(DB_ERROR, ""));
        if (!status.ok()) {
            return HandleException("DiscardFiles error: sqlite transaction failed", status.message().c_str());
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when discard collection file", e.what());
    }

    return DiscardFiles(to_discard_size);
}

Status
SqliteMetaImpl::SetGlobalLastLSN(uint64_t lsn) {
    try {
        server::MetricCollector metric;

        bool first_create = false;
        uint64_t last_lsn = 0;
        std::string statement = "SELECT global_lsn FROM " + std::string(META_ENVIRONMENT) + ";";

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.size() == 0) {
            first_create = true;
        } else {
            last_lsn = std::stoul(res[0]["global_lsn"]);
        }

        if (first_create) {  // first time to get global lsn
            statement = "INSERT INTO " + std::string(META_ENVIRONMENT) + " VALUES(" + std::to_string(lsn) + ");";
            LOG_ENGINE_DEBUG_ << "SetGlobalLastLSN: " << statement;

            status = SqlTransaction({statement});
            if (!status.ok()) {
                return HandleException("QUERY ERROR WHEN SET GLOBAL LSN", status.message().c_str());
            }
        } else if (lsn > last_lsn) {
            statement = "UPDATE " + std::string(META_ENVIRONMENT) + " SET global_lsn = " + std::to_string(lsn) + ";";
            LOG_ENGINE_DEBUG_ << "SetGlobalLastLSN: " << statement;

            status = SqlTransaction({statement});
            if (!status.ok()) {
                return HandleException("Failed to set global lsn", status.message().c_str());
            }
        }

    } catch (std::exception& e) {
        std::string msg = "Exception update global lsn = " + lsn;
        return HandleException(msg, e.what());
    }

    return Status::OK();
}

Status
SqliteMetaImpl::GetGlobalLastLSN(uint64_t& lsn) {
    try {
        server::MetricCollector metric;

        std::string statement = "SELECT global_lsn FROM " + std::string(META_ENVIRONMENT) + ";";
        LOG_ENGINE_DEBUG_ << "GetGlobalLastLSN: " << statement;

        AttrsMapList res;
        auto status = SqlQuery(statement, &res);
        if (!status.ok()) {
            return status;
        }

        if (res.empty()) {
            lsn = 0;
        } else {
            lsn = std::stoul(res[0]["global_lsn"]);
        }
    } catch (std::exception& e) {
        return HandleException("Encounter exception when delete collection folder", e.what());
    }

    return Status::OK();
}

}  // namespace meta
}  // namespace engine
}  // namespace milvus
