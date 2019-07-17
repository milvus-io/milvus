/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "RequestTask.h"
#include "ServerConfig.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "DBWrapper.h"
#include "version.h"

#ifdef MILVUS_ENABLE_PROFILING
#include "gperftools/profiler.h"
#endif

namespace zilliz {
namespace milvus {
namespace server {

using namespace ::milvus;

static const std::string DQL_TASK_GROUP = "dql";
static const std::string DDL_DML_TASK_GROUP = "ddl_dml";
static const std::string PING_TASK_GROUP = "ping";

using DB_META = zilliz::milvus::engine::meta::Meta;
using DB_DATE = zilliz::milvus::engine::meta::DateT;

namespace {
    engine::EngineType EngineType(int type) {
        static std::map<int, engine::EngineType> map_type = {
                {0, engine::EngineType::INVALID},
                {1, engine::EngineType::FAISS_IDMAP},
                {2, engine::EngineType::FAISS_IVFFLAT},
                {3, engine::EngineType::FAISS_IVFSQ8},
        };

        if(map_type.find(type) == map_type.end()) {
            return engine::EngineType::INVALID;
        }

        return map_type[type];
    }

    int IndexType(engine::EngineType type) {
        static std::map<engine::EngineType, int> map_type = {
                {engine::EngineType::INVALID, 0},
                {engine::EngineType::FAISS_IDMAP, 1},
                {engine::EngineType::FAISS_IVFFLAT, 2},
                {engine::EngineType::FAISS_IVFSQ8, 3},
        };

        if(map_type.find(type) == map_type.end()) {
            return 0;
        }

        return map_type[type];
    }

    void
    ConvertRowRecordToFloatArray(const std::vector<thrift::RowRecord>& record_array,
                                 uint64_t dimension,
                                 std::vector<float>& float_array,
                                 ServerError& error_code,
                                 std::string& error_msg) {
        uint64_t vec_count = record_array.size();
        float_array.resize(vec_count*dimension);//allocate enough memory
        for(uint64_t i = 0; i < vec_count; i++) {
            const auto& record = record_array[i];
            if(record.vector_data.empty()) {
                error_code = SERVER_INVALID_ROWRECORD;
                error_msg = "Rowrecord float array is empty";
                return;
            }
            uint64_t vec_dim = record.vector_data.size()/sizeof(double);//how many double value?
            if(vec_dim != dimension) {
                error_code = SERVER_INVALID_VECTOR_DIMENSION;
                error_msg = "Invalid rowrecord dimension: " + std::to_string(vec_dim)
                                 + " vs. table dimension:" + std::to_string(dimension);
                return;
            }

            //convert double array to float array(thrift has no float type)
            const double* d_p = reinterpret_cast<const double*>(record.vector_data.data());
            for(uint64_t d = 0; d < vec_dim; d++) {
                float_array[i*vec_dim + d] = (float)(d_p[d]);
            }
        }
    }

    static constexpr long DAY_SECONDS = 86400;

    void
    ConvertTimeRangeToDBDates(const std::vector<thrift::Range> &range_array,
                              std::vector<DB_DATE>& dates,
                              ServerError& error_code,
                              std::string& error_msg) {
        dates.clear();
        for(auto& range : range_array) {
            time_t tt_start, tt_end;
            tm tm_start, tm_end;
            if(!CommonUtil::TimeStrToTime(range.start_value, tt_start, tm_start)){
                error_code = SERVER_INVALID_TIME_RANGE;
                error_msg = "Invalid time range: " + range.start_value;
                return;
            }

            if(!CommonUtil::TimeStrToTime(range.end_value, tt_end, tm_end)){
                error_code = SERVER_INVALID_TIME_RANGE;
                error_msg = "Invalid time range: " + range.start_value;
                return;
            }

            long days = (tt_end > tt_start) ? (tt_end - tt_start)/DAY_SECONDS : (tt_start - tt_end)/DAY_SECONDS;
            for(long i = 0; i <= days; i++) {
                time_t tt_day = tt_start + DAY_SECONDS*i;
                tm tm_day;
                CommonUtil::ConvertTime(tt_day, tm_day);

                long date = tm_day.tm_year*10000 + tm_day.tm_mon*100 + tm_day.tm_mday;//according to db logic
                dates.push_back(date);
            }
        }
    }

    std::string
    GetCurrTimeStr() {
        char tm_buf[20] = {0};
        time_t tt;
        time(&tt);
        tt = tt + 8 * 60 * 60;
        tm* t = gmtime(&tt);
        sprintf(tm_buf, "%4d%02d%02d_%02d%02d%02d", (t->tm_year+1900), (t->tm_mon+1), (t->tm_mday),
                                                    (t->tm_hour), (t->tm_min), (t->tm_sec));
        return tm_buf;
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
        //step 1: check arguments
        if(schema_.table_name.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }
        if(schema_.dimension <= 0) {
            return SetError(SERVER_INVALID_TABLE_DIMENSION, "Invalid table dimension: " + std::to_string(schema_.dimension));
        }

        engine::EngineType engine_type = EngineType(schema_.index_type);
        if(engine_type == engine::EngineType::INVALID) {
            return SetError(SERVER_INVALID_INDEX_TYPE, "Invalid index type: " + std::to_string(schema_.index_type));
        }

        //step 2: construct table schema
        engine::meta::TableSchema table_info;
        table_info.dimension_ = (uint16_t)schema_.dimension;
        table_info.table_id_ = schema_.table_name;
        table_info.engine_type_ = (int)EngineType(schema_.index_type);
        table_info.store_raw_data_ = schema_.store_raw_vector;

        //step 3: create table
        engine::Status stat = DBWrapper::DB()->CreateTable(table_info);
        if(!stat.ok()) {//table could exist
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
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
        //step 1: check arguments
        if(table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        //step 2: get table info
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        schema_.table_name = table_info.table_id_;
        schema_.index_type = IndexType((engine::EngineType)table_info.engine_type_);
        schema_.dimension = table_info.dimension_;
        schema_.store_raw_vector = table_info.store_raw_data_;

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.Record("done");

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
BuildIndexTask::BuildIndexTask(const std::string& table_name)
    : BaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name) {
}

BaseTaskPtr BuildIndexTask::Create(const std::string& table_name) {
    return std::shared_ptr<BaseTask>(new BuildIndexTask(table_name));
}

ServerError BuildIndexTask::OnExecute() {
    try {
        TimeRecorder rc("BuildIndexTask");

        //step 1: check arguments
        if(table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        //step 2: check table existence
        engine::Status stat = DBWrapper::DB()->BuildIndex(table_name_);
        if(!stat.ok()) {
            return SetError(SERVER_BUILD_INDEX_ERROR, "Engine failed: " + stat.ToString());
        }

        rc.Elapse("totally cost");
    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
HasTableTask::HasTableTask(const std::string& table_name, bool& has_table)
    : BaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name),
      has_table_(has_table) {

}

BaseTaskPtr HasTableTask::Create(const std::string& table_name, bool& has_table) {
    return std::shared_ptr<BaseTask>(new HasTableTask(table_name, has_table));
}

ServerError HasTableTask::OnExecute() {
    try {
        TimeRecorder rc("HasTableTask");

        //step 1: check arguments
        if(table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        //step 2: check table existence
        engine::Status stat = DBWrapper::DB()->HasTable(table_name_, has_table_);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        rc.Elapse("totally cost");
    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DeleteTableTask::DeleteTableTask(const std::string& table_name)
    : BaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name) {

}

BaseTaskPtr DeleteTableTask::Create(const std::string& table_name) {
    return std::shared_ptr<BaseTask>(new DeleteTableTask(table_name));
}

ServerError DeleteTableTask::OnExecute() {
    try {
        TimeRecorder rc("DeleteTableTask");

        //step 1: check arguments
        if (table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            if(stat.IsNotFound()) {
                return SetError(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
            } else {
                return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
            }
        }

        rc.Record("check validation");

        //step 3: delete table
        std::vector<DB_DATE> dates;
        stat = DBWrapper::DB()->DeleteTable(table_name_, dates);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        rc.Record("deleta table");
        rc.Elapse("totally cost");
    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ShowTablesTask::ShowTablesTask(std::vector<std::string>& tables)
    : BaseTask(DDL_DML_TASK_GROUP),
      tables_(tables) {

}

BaseTaskPtr ShowTablesTask::Create(std::vector<std::string>& tables) {
    return std::shared_ptr<BaseTask>(new ShowTablesTask(tables));
}

ServerError ShowTablesTask::OnExecute() {
    std::vector<engine::meta::TableSchema> schema_array;
    engine::Status stat = DBWrapper::DB()->AllTables(schema_array);
    if(!stat.ok()) {
        return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
    }

    tables_.clear();
    for(auto& schema : schema_array) {
        tables_.push_back(schema.table_id_);
    }

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

        //step 1: check arguments
        if (table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        if(record_array_.empty()) {
            return SetError(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array is empty");
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            if(stat.IsNotFound()) {
                return SetError(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
            } else {
                return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
            }
        }

        rc.Record("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/insert_" + std::to_string(this->record_array_.size()) + "_" +
                            GetCurrTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#else
        std::cout << "CYD - MILVUS_ENABLE_PROFILING is OFF!" << std::endl;
#endif

        //step 3: prepare float data
        std::vector<float> vec_f;
        ServerError error_code = SERVER_SUCCESS;
        std::string error_msg;
        ConvertRowRecordToFloatArray(record_array_, table_info.dimension_, vec_f, error_code, error_msg);
        if(error_code != SERVER_SUCCESS) {
            return SetError(error_code, error_msg);
        }

        rc.Record("prepare vectors data");

        //step 4: insert vectors
        uint64_t vec_count = (uint64_t)record_array_.size();
        stat = DBWrapper::DB()->InsertVectors(table_name_, vec_count, vec_f.data(), record_ids_);
        rc.Record("add vectors to engine");
        if(!stat.ok()) {
            return SetError(SERVER_CACHE_ERROR, "Cache error: " + stat.ToString());
        }

        if(record_ids_.size() != vec_count) {
            std::string msg = "Add " + std::to_string(vec_count) + " vectors but only return "
                    + std::to_string(record_ids_.size()) + " id";
            return SetError(SERVER_ILLEGAL_VECTOR_ID, msg);
        }

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.Record("do insert");
        rc.Elapse("totally cost");

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SearchVectorTask::SearchVectorTask(const std::string &table_name,
                                   const std::vector<std::string>& file_id_array,
                                   const std::vector<thrift::RowRecord> &query_record_array,
                                   const std::vector<thrift::Range> &query_range_array,
                                   const int64_t top_k,
                                   std::vector<thrift::TopKQueryResult> &result_array)
    : BaseTask(DQL_TASK_GROUP),
      table_name_(table_name),
      file_id_array_(file_id_array),
      record_array_(query_record_array),
      range_array_(query_range_array),
      top_k_(top_k),
      result_array_(result_array) {

}

BaseTaskPtr SearchVectorTask::Create(const std::string& table_name,
                                     const std::vector<std::string>& file_id_array,
                                     const std::vector<thrift::RowRecord> & query_record_array,
                                     const std::vector<thrift::Range> & query_range_array,
                                     const int64_t top_k,
                                     std::vector<thrift::TopKQueryResult>& result_array) {
    return std::shared_ptr<BaseTask>(new SearchVectorTask(table_name, file_id_array,
            query_record_array, query_range_array, top_k, result_array));
}

ServerError SearchVectorTask::OnExecute() {
    try {
        TimeRecorder rc("SearchVectorTask");

        //step 1: check arguments
        if (table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        if(top_k_ <= 0) {
            return SetError(SERVER_INVALID_TOPK, "Invalid topk: " + std::to_string(top_k_));
        }
        if(record_array_.empty()) {
            return SetError(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array is empty");
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            if(stat.IsNotFound()) {
                return SetError(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
            } else {
                return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
            }
        }

        //step 3: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        ServerError error_code = SERVER_SUCCESS;
        std::string error_msg;
        ConvertTimeRangeToDBDates(range_array_, dates, error_code, error_msg);
        if(error_code != SERVER_SUCCESS) {
            return SetError(error_code, error_msg);
        }

        rc.Record("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + std::to_string(this->record_array_.size()) + "_" +
                            "top_" + std::to_string(this->top_k_) + "_" +
                            GetCurrTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        //step 3: prepare float data
        std::vector<float> vec_f;
        ConvertRowRecordToFloatArray(record_array_, table_info.dimension_, vec_f, error_code, error_msg);
        if(error_code != SERVER_SUCCESS) {
            return SetError(error_code, error_msg);
        }

        rc.Record("prepare vector data");

        //step 4: search vectors
        engine::QueryResults results;
        uint64_t record_count = (uint64_t)record_array_.size();

        if(file_id_array_.empty()) {
            stat = DBWrapper::DB()->Query(table_name_, (size_t) top_k_, record_count, vec_f.data(), dates, results);
        } else {
            stat = DBWrapper::DB()->Query(table_name_, file_id_array_, (size_t) top_k_, record_count, vec_f.data(), dates, results);
        }

        rc.Record("search vectors from engine");
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        if(results.empty()) {
            return SERVER_SUCCESS; //empty table
        }

        if(results.size() != record_count) {
            std::string msg = "Search " + std::to_string(record_count) + " vectors but only return "
                              + std::to_string(results.size()) + " results";
            return SetError(SERVER_ILLEGAL_SEARCH_RESULT, msg);
        }

        rc.Record("do search");

        //step 5: construct result array
        for(uint64_t i = 0; i < record_count; i++) {
            auto& result = results[i];
            const auto& record = record_array_[i];

            thrift::TopKQueryResult thrift_topk_result;
            for(auto& pair : result) {
                thrift::QueryResult thrift_result;
                thrift_result.__set_id(pair.first);
                thrift_result.__set_distance(pair.second);

                thrift_topk_result.query_result_arrays.emplace_back(thrift_result);
            }

            result_array_.emplace_back(thrift_topk_result);
        }

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.Record("construct result");
        rc.Elapse("totally cost");

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
GetTableRowCountTask::GetTableRowCountTask(const std::string& table_name, int64_t& row_count)
: BaseTask(DDL_DML_TASK_GROUP),
  table_name_(table_name),
  row_count_(row_count) {

}

BaseTaskPtr GetTableRowCountTask::Create(const std::string& table_name, int64_t& row_count) {
    return std::shared_ptr<BaseTask>(new GetTableRowCountTask(table_name, row_count));
}

ServerError GetTableRowCountTask::OnExecute() {
    try {
        TimeRecorder rc("GetTableRowCountTask");

        //step 1: check arguments
        if (table_name_.empty()) {
            return SetError(SERVER_INVALID_TABLE_NAME, "Empty table name");
        }

        //step 2: get row count
        uint64_t row_count = 0;
        engine::Status stat = DBWrapper::DB()->GetTableRowCount(table_name_, row_count);
        if (!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        row_count_ = (int64_t) row_count;

        rc.Elapse("totally cost");

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
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
        result_ = MILVUS_VERSION;
    }

    return SERVER_SUCCESS;
}

}
}
}
