/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "MegasearchTask.h"
#include "ServerConfig.h"
#include "VecIdMapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ThreadPool.h"
#include "db/DB.h"
#include "db/Env.h"
#include "db/Meta.h"


namespace zilliz {
namespace vecwise {
namespace server {

static const std::string DQL_TASK_GROUP = "dql";
static const std::string DDL_DML_TASK_GROUP = "ddl_dml";
static const std::string PING_TASK_GROUP = "ping";

static const std::string VECTOR_UID = "uid";
static const uint64_t USE_MT = 5000;

using DB_META = zilliz::vecwise::engine::meta::Meta;
using DB_DATE = zilliz::vecwise::engine::meta::DateT;

namespace {
    class DBWrapper {
    public:
        DBWrapper() {
            zilliz::vecwise::engine::Options opt;
            ConfigNode& config = ServerConfig::GetInstance().GetConfig(CONFIG_DB);
            opt.meta.backend_uri = config.GetValue(CONFIG_DB_URL);
            std::string db_path = config.GetValue(CONFIG_DB_PATH);
            opt.memory_sync_interval = (uint16_t)config.GetInt32Value(CONFIG_DB_FLUSH_INTERVAL, 10);
            opt.meta.path = db_path + "/db";

            CommonUtil::CreateDirectory(opt.meta.path);

            zilliz::vecwise::engine::DB::Open(opt, &db_);
            if(db_ == nullptr) {
                SERVER_LOG_ERROR << "Failed to open db";
                throw ServerException(SERVER_NULL_POINTER, "Failed to open db");
            }
        }

        ~DBWrapper() {
            delete db_;
        }

        zilliz::vecwise::engine::DB* DB() { return db_; }

    private:
        zilliz::vecwise::engine::DB* db_ = nullptr;
    };

    zilliz::vecwise::engine::DB* DB() {
        static DBWrapper db_wrapper;
        return db_wrapper.DB();
    }

    ThreadPool& GetThreadPool() {
        static ThreadPool pool(6);
        return pool;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CreateTableTask::CreateTableTask(const thrift::TableSchema& schema)
: BaseTask(DDL_DML_TASK_GROUP),
  schema_(schema) {

}

BaseTaskPtr CreateTableTask::Create(const thrift::TableSchema& schema) {
    return std::shared_ptr<BaseTask>(new CreateTableTask(schema));
}

ServerError CreateTableTask::OnExecute() {
    TimeRecorder rc("CreateTableTask");
    
    try {
        if(schema_.vector_column_array.empty()) {
            return SERVER_INVALID_ARGUMENT;
        }

        IVecIdMapper::GetInstance()->AddGroup(schema_.table_name);
        engine::meta::TableSchema table_info;
        table_info.dimension = (uint16_t)schema_.vector_column_array[0].dimension;
        table_info.table_id = schema_.table_name;
        engine::Status stat = DB()->CreateTable(table_info);
        if(!stat.ok()) {//could exist
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return SERVER_SUCCESS;
        }

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << error_msg_;
        return SERVER_UNEXPECTED_ERROR;
    }

    rc.Record("done");

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DescribeTableTask::DescribeTableTask(const std::string &table_name, thrift::TableSchema &schema)
    : BaseTask(PING_TASK_GROUP),
      table_name_(table_name),
      schema_(schema) {
    schema_.table_name = table_name_;
}

BaseTaskPtr DescribeTableTask::Create(const std::string& table_name, thrift::TableSchema& schema) {
    return std::shared_ptr<BaseTask>(new DescribeTableTask(table_name, schema));
}

ServerError DescribeTableTask::OnExecute() {
    TimeRecorder rc("DescribeTableTask");

    try {
        engine::meta::TableSchema table_info;
        table_info.table_id = table_name_;
        engine::Status stat = DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_GROUP_NOT_EXIST;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        } else {

        }

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << error_msg_;
        return SERVER_UNEXPECTED_ERROR;
    }

    rc.Record("done");

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DeleteTableTask::DeleteTableTask(const std::string& table_name)
    : BaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name) {

}

BaseTaskPtr DeleteTableTask::Create(const std::string& group_id) {
    return std::shared_ptr<BaseTask>(new DeleteTableTask(group_id));
}

ServerError DeleteTableTask::OnExecute() {
    error_code_ = SERVER_NOT_IMPLEMENT;
    error_msg_ = "delete table not implemented";
    SERVER_LOG_ERROR << error_msg_;

    IVecIdMapper::GetInstance()->DeleteGroup(table_name_);

    return SERVER_NOT_IMPLEMENT;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CreateTablePartitionTask::CreateTablePartitionTask(const thrift::CreateTablePartitionParam &param)
    : BaseTask(DDL_DML_TASK_GROUP),
      param_(param) {

}

BaseTaskPtr CreateTablePartitionTask::Create(const thrift::CreateTablePartitionParam &param) {
    return std::shared_ptr<BaseTask>(new CreateTablePartitionTask(param));
}

ServerError CreateTablePartitionTask::OnExecute() {
    error_code_ = SERVER_NOT_IMPLEMENT;
    error_msg_ = "create table partition not implemented";
    SERVER_LOG_ERROR << error_msg_;

    return SERVER_NOT_IMPLEMENT;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DeleteTablePartitionTask::DeleteTablePartitionTask(const thrift::DeleteTablePartitionParam &param)
    : BaseTask(DDL_DML_TASK_GROUP),
      param_(param) {

}

BaseTaskPtr DeleteTablePartitionTask::Create(const thrift::DeleteTablePartitionParam &param) {
    return std::shared_ptr<BaseTask>(new DeleteTablePartitionTask(param));
}

ServerError DeleteTablePartitionTask::OnExecute() {
    error_code_ = SERVER_NOT_IMPLEMENT;
    error_msg_ = "delete table partition not implemented";
    SERVER_LOG_ERROR << error_msg_;

    return SERVER_NOT_IMPLEMENT;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ShowTablesTask::ShowTablesTask(std::vector<std::string>& tables)
    : BaseTask(PING_TASK_GROUP),
      tables_(tables) {

}

BaseTaskPtr ShowTablesTask::Create(std::vector<std::string>& tables) {
    return std::shared_ptr<BaseTask>(new ShowTablesTask(tables));
}

ServerError ShowTablesTask::OnExecute() {
    IVecIdMapper::GetInstance()->AllGroups(tables_);

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
AddVectorTask::AddVectorTask(const std::string& table_name,
                                       const std::vector<thrift::RowRecord>& record_array,
                                       std::vector<int64_t>& record_ids)
    : BaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name),
      record_array_(record_array),
      record_ids_(record_ids) {
    record_ids_.clear();
}

BaseTaskPtr AddVectorTask::Create(const std::string& table_name,
                                       const std::vector<thrift::RowRecord>& record_array,
                                       std::vector<int64_t>& record_ids) {
    return std::shared_ptr<BaseTask>(new AddVectorTask(table_name, record_array, record_ids));
}

ServerError AddVectorTask::OnExecute() {
    try {
        TimeRecorder rc("AddVectorTask");

        if(record_array_.empty()) {
            return SERVER_SUCCESS;
        }

        engine::meta::TableSchema table_info;
        table_info.table_id = table_name_;
        engine::Status stat = DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_GROUP_NOT_EXIST;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        rc.Record("get group info");

        uint64_t vec_count = (uint64_t)record_array_.size();
        uint64_t group_dim = table_info.dimension;
        std::vector<float> vec_f;
        vec_f.resize(vec_count*group_dim);//allocate enough memory
        for(uint64_t i = 0; i < vec_count; i++) {
            const auto& record = record_array_[i];
            if(record.vector_map.empty()) {
                error_code_ = SERVER_INVALID_ARGUMENT;
                error_msg_ = "No vector provided in record";
                SERVER_LOG_ERROR << error_msg_;
                return error_code_;
            }
            uint64_t vec_dim = record.vector_map.begin()->second.size()/sizeof(double);//how many double value?
            if(vec_dim != group_dim) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                                 << " vs. group dimension:" << group_dim;
                error_code_ = SERVER_INVALID_VECTOR_DIMENSION;
                error_msg_ = "Engine failed: " + stat.ToString();
                return error_code_;
            }

            //convert double array to float array(thrift has no float type)
            const double* d_p = reinterpret_cast<const double*>(record.vector_map.begin()->second.data());
            for(uint64_t d = 0; d < vec_dim; d++) {
                vec_f[i*vec_dim + d] = (float)(d_p[d]);
            }
        }

        rc.Record("prepare vectors data");

        stat = DB()->InsertVectors(table_name_, vec_count, vec_f.data(), record_ids_);
        rc.Record("add vectors to engine");
        if(!stat.ok()) {
            error_code_ = SERVER_UNEXPECTED_ERROR;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        if(record_ids_.size() != vec_count) {
            SERVER_LOG_ERROR << "Vector ID not returned";
            return SERVER_UNEXPECTED_ERROR;
        }

        //persist attributes
        for(uint64_t i = 0; i < vec_count; i++) {
            const auto &record = record_array_[i];

            //any attributes?
            if(record.attribute_map.empty()) {
                continue;
            }

            std::string nid = std::to_string(record_ids_[i]);
            std::string attrib_str;
            AttributeSerializer::Encode(record.attribute_map, attrib_str);
            IVecIdMapper::GetInstance()->Put(nid, attrib_str, table_name_);
        }

        rc.Record("persist vector attributes");

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << error_msg_;
        return error_code_;
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SearchVectorTask::SearchVectorTask(const std::string& table_name,
                                   const int64_t top_k,
                                   const std::vector<thrift::QueryRecord>& record_array,
                                   std::vector<thrift::TopKQueryResult>& result_array)
    : BaseTask(DQL_TASK_GROUP),
      table_name_(table_name),
      top_k_(top_k),
      record_array_(record_array),
      result_array_(result_array) {

}

BaseTaskPtr SearchVectorTask::Create(const std::string& table_name,
                                     const std::vector<thrift::QueryRecord>& record_array,
                                     const int64_t top_k,
                                     std::vector<thrift::TopKQueryResult>& result_array) {
    return std::shared_ptr<BaseTask>(new SearchVectorTask(table_name, top_k, record_array, result_array));
}

ServerError SearchVectorTask::OnExecute() {
    try {
        TimeRecorder rc("SearchVectorTask");

        if(top_k_ <= 0 || record_array_.empty()) {
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "Invalid topk value, or query record array is empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        engine::meta::TableSchema table_info;
        table_info.table_id = table_name_;
        engine::Status stat = DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_GROUP_NOT_EXIST;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        std::vector<float> vec_f;
        uint64_t record_count = (uint64_t)record_array_.size();
        vec_f.resize(record_count*table_info.dimension);

        for(uint64_t i = 0; i < record_array_.size(); i++) {
            const auto& record = record_array_[i];
            if (record.vector_map.empty()) {
                error_code_ = SERVER_INVALID_ARGUMENT;
                error_msg_ = "Query record has no vector";
                SERVER_LOG_ERROR << error_msg_;
                return error_code_;
            }

            uint64_t vec_dim = record.vector_map.begin()->second.size() / sizeof(double);//how many double value?
            if (vec_dim != table_info.dimension) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                                 << " vs. group dimension:" << table_info.dimension;
                error_code_ = SERVER_INVALID_VECTOR_DIMENSION;
                error_msg_ = "Engine failed: " + stat.ToString();
                return error_code_;
            }

            //convert double array to float array(thrift has no float type)
            const double* d_p = reinterpret_cast<const double*>(record.vector_map.begin()->second.data());
            for(uint64_t d = 0; d < vec_dim; d++) {
                vec_f[i*vec_dim + d] = (float)(d_p[d]);
            }
        }

        rc.Record("prepare vector data");

        std::vector<DB_DATE> dates;
        engine::QueryResults results;
        stat = DB()->Query(table_name_, (size_t)top_k_, record_count, vec_f.data(), dates, results);
        rc.Record("search vectors from engine");
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        }

        if(results.size() != record_count) {
            SERVER_LOG_ERROR << "Search result not returned";
            return SERVER_UNEXPECTED_ERROR;
        }

        //construct result array
        for(uint64_t i = 0; i < record_count; i++) {
            auto& result = results[i];
            const auto& record = record_array_[i];

            thrift::TopKQueryResult thrift_topk_result;
            for(auto id : result) {
                thrift::QueryResult thrift_result;
                thrift_result.__set_id(id);

                //need get attributes?
                if(record.selected_column_array.empty()) {
                    thrift_topk_result.query_result_arrays.emplace_back(thrift_result);
                    continue;
                }

                std::string nid = std::to_string(id);
                std::string attrib_str;
                IVecIdMapper::GetInstance()->Get(nid, attrib_str, table_name_);

                AttribMap attrib_map;
                AttributeSerializer::Decode(attrib_str, attrib_map);

                for(auto& attribute : record.selected_column_array) {
                    thrift_result.column_map[attribute] = attrib_map[attribute];
                }

                thrift_topk_result.query_result_arrays.emplace_back(thrift_result);
            }

            result_array_.emplace_back(thrift_topk_result);
        }
        rc.Record("construct result");

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << error_msg_;
        return error_code_;
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
PingTask::PingTask(const std::string& cmd, std::string& result)
    : BaseTask(PING_TASK_GROUP),
      cmd_(cmd),
      result_(result) {

}

BaseTaskPtr PingTask::Create(const std::string& cmd, std::string& result) {
    return std::shared_ptr<BaseTask>(new PingTask(cmd, result));
}

ServerError PingTask::OnExecute() {
    if(cmd_ == "version") {
        result_ = "v1.2.0";//currently hardcode
    }

    return SERVER_SUCCESS;
}

}
}
}
