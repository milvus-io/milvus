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
        engine::meta::GroupSchema group_info;
        group_info.dimension = (uint16_t)schema_.vector_column_array[0].dimension;
        group_info.group_id = schema_.table_name;
        engine::Status stat = DB()->add_group(group_info);
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
    : BaseTask(DDL_DML_TASK_GROUP),
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
        engine::meta::GroupSchema group_info;
        group_info.group_id = table_name_;
        engine::Status stat = DB()->get_group(group_info);
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

    //IVecIdMapper::GetInstance()->DeleteGroup(table_name_);

    return SERVER_NOT_IMPLEMENT;
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
    record_ids_.resize(record_array.size());
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

        engine::meta::GroupSchema group_info;
        group_info.group_id = table_name_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            error_code_ = SERVER_GROUP_NOT_EXIST;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        rc.Record("get group info");

        uint64_t vec_count = (uint64_t)record_array_.size();
        uint64_t group_dim = group_info.dimension;
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

            const double* d_p = reinterpret_cast<const double*>(record.vector_map.begin()->second.data());
            for(uint64_t d = 0; d < vec_dim; d++) {
                vec_f[i*vec_dim + d] = (float)(d_p[d]);
            }
        }

        rc.Record("prepare vectors data");

        stat = DB()->add_vectors(table_name_, vec_count, vec_f.data(), record_ids_);
        rc.Record("add vectors to engine");
        if(!stat.ok()) {
            error_code_ = SERVER_UNEXPECTED_ERROR;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        if(record_ids_.size() < vec_count) {
            SERVER_LOG_ERROR << "Vector ID not returned";
            return SERVER_UNEXPECTED_ERROR;
        }

        rc.Record("done");

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

        engine::meta::GroupSchema group_info;
        group_info.group_id = table_name_;
        engine::Status stat = DB()->get_group(group_info);
        if(!stat.ok()) {
            error_code_ = SERVER_GROUP_NOT_EXIST;
            error_msg_ = "Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        std::vector<float> vec_f;
        uint64_t record_count = (uint64_t)record_array_.size();
        vec_f.resize(record_count*group_info.dimension);

        for(uint64_t i = 0; i < record_array_.size(); i++) {
            const auto& record = record_array_[i];
            if (record.vector_map.empty()) {
                error_code_ = SERVER_INVALID_ARGUMENT;
                error_msg_ = "Query record has no vector";
                SERVER_LOG_ERROR << error_msg_;
                return error_code_;
            }

            uint64_t vec_dim = record.vector_map.begin()->second.size() / sizeof(double);//how many double value?
            if (vec_dim != group_info.dimension) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                                 << " vs. group dimension:" << group_info.dimension;
                error_code_ = SERVER_INVALID_VECTOR_DIMENSION;
                error_msg_ = "Engine failed: " + stat.ToString();
                return error_code_;
            }

            const double* d_p = reinterpret_cast<const double*>(record.vector_map.begin()->second.data());
            for(uint64_t d = 0; d < vec_dim; d++) {
                vec_f[i*vec_dim + d] = (float)(d_p[d]);
            }
        }

        rc.Record("prepare vector data");

        std::vector<DB_DATE> dates;
        engine::QueryResults results;
        stat = DB()->search(table_name_, (size_t)top_k_, record_count, vec_f.data(), dates, results);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        } else {
            rc.Record("do searching");
            for(engine::QueryResult& result : results){
                thrift::TopKQueryResult thrift_topk_result;
                for(auto id : result) {
                    thrift::QueryResult thrift_result;
                    thrift_result.__set_id(id);
                    thrift_topk_result.query_result_arrays.emplace_back(thrift_result);
                }

                result_array_.push_back(thrift_topk_result);
            }
            rc.Record("construct result");
        }

        rc.Record("done");

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << error_msg_;
        return error_code_;
    }

    return SERVER_SUCCESS;
}

}
}
}
