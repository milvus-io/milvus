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
        };

        if(map_type.find(type) == map_type.end()) {
            return 0;
        }

        return map_type[type];
    }

    ServerError
    ConvertRowRecordToFloatArray(const std::vector<thrift::RowRecord>& record_array,
                                 uint64_t dimension,
                                 std::vector<float>& float_array) {
        ServerError error_code;
        uint64_t vec_count = record_array.size();
        float_array.resize(vec_count*dimension);//allocate enough memory
        for(uint64_t i = 0; i < vec_count; i++) {
            const auto& record = record_array[i];
            if(record.vector_data.empty()) {
                error_code = SERVER_INVALID_ARGUMENT;
                SERVER_LOG_ERROR << "No vector provided in record";
                return error_code;
            }
            uint64_t vec_dim = record.vector_data.size()/sizeof(double);//how many double value?
            if(vec_dim != dimension) {
                SERVER_LOG_ERROR << "Invalid vector dimension: " << vec_dim
                                 << " vs. table dimension:" << dimension;
                error_code = SERVER_INVALID_VECTOR_DIMENSION;
                return error_code;
            }

            //convert double array to float array(thrift has no float type)
            const double* d_p = reinterpret_cast<const double*>(record.vector_data.data());
            for(uint64_t d = 0; d < vec_dim; d++) {
                float_array[i*vec_dim + d] = (float)(d_p[d]);
            }
        }

        return SERVER_SUCCESS;
    }

    static constexpr long DAY_SECONDS = 86400;

    ServerError
    ConvertTimeRangeToDBDates(const std::vector<thrift::Range> &range_array,
                              std::vector<DB_DATE>& dates) {
        dates.clear();
        ServerError error_code;
        for(auto& range : range_array) {
            time_t tt_start, tt_end;
            tm tm_start, tm_end;
            if(!CommonUtil::TimeStrToTime(range.start_value, tt_start, tm_start)){
                error_code = SERVER_INVALID_TIME_RANGE;
                SERVER_LOG_ERROR << "Invalid time range: " << range.start_value;
                return error_code;
            }

            if(!CommonUtil::TimeStrToTime(range.end_value, tt_end, tm_end)){
                error_code = SERVER_INVALID_TIME_RANGE;
                SERVER_LOG_ERROR << "Invalid time range: " << range.end_value;
                return error_code;
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

        return SERVER_SUCCESS;
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
        if(schema_.table_name.empty() || schema_.dimension <= 0) {
            error_code_ = SERVER_INVALID_ARGUMENT;
//            error_msg_ = schema_.table_name.empty() ?
            error_msg_ = "CreateTableTask: Invalid table name or dimension. table name = " + schema_.table_name
                        + "dimension = " + std::to_string(schema_.dimension);
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        engine::EngineType engine_type = EngineType(schema_.index_type);
        if(engine_type == engine::EngineType::INVALID) {
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "CreateTableTask: Invalid index type. type = " + std::to_string(schema_.index_type);
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
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
            error_code_ = SERVER_UNEXPECTED_ERROR;
            error_msg_ = "CreateTableTask: Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << "CreateTableTask: " << error_msg_;
        return error_code_;
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
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "DescribeTableTask: Table name cannot be empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 2: get table info
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_TABLE_NOT_EXIST;
            error_msg_ = "DescribeTableTask: Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        schema_.table_name = table_info.table_id_;
        schema_.index_type = IndexType((engine::EngineType)table_info.engine_type_);
        schema_.dimension = table_info.dimension_;
        schema_.store_raw_vector = table_info.store_raw_data_;

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << "DescribeTableTask: " << error_msg_;
        return SERVER_UNEXPECTED_ERROR;
    }

    rc.Record("done");

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
        if (table_name_.empty()) {
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "Table name cannot be empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 2: check table existence
        engine::Status stat = DBWrapper::DB()->HasTable(table_name_, has_table_);

        rc.Elapse("totally cost");
    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << error_msg_;
        return error_code_;
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
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "DeleteTableTask: Table name cannot be empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_TABLE_NOT_EXIST;
            error_msg_ = "DeleteTableTask: Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        rc.Record("check validation");

        //step 3: delete table
        std::vector<DB_DATE> dates;
        stat = DBWrapper::DB()->DeleteTable(table_name_, dates);
        if(!stat.ok()) {
            SERVER_LOG_ERROR << "DeleteTableTask: Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        }

        rc.Record("deleta table");
        rc.Elapse("total cost");
    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << "DeleteTableTask: " << error_msg_;
        return error_code_;
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
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = "ShowTablesTask: Engine failed: " + stat.ToString();
        SERVER_LOG_ERROR << error_msg_;
        return error_code_;
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
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "AddVectorTask: Table name cannot be empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        if(record_array_.empty()) {
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "AddVectorTask: Row record array is empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_TABLE_NOT_EXIST;
            error_msg_ = "AddVectorTask: Engine failed when DescribeTable: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        rc.Record("check validation");

        //step 3: prepare float data
        std::vector<float> vec_f;
        error_code_ = ConvertRowRecordToFloatArray(record_array_, table_info.dimension_, vec_f);
        if(error_code_ != SERVER_SUCCESS) {
            error_msg_ = "AddVectorTask when ConvertRowRecordToFloatArray: Invalid row record data";
            return error_code_;
        }

        rc.Record("prepare vectors data");

        //step 4: insert vectors
        uint64_t vec_count = (uint64_t)record_array_.size();
        stat = DBWrapper::DB()->InsertVectors(table_name_, vec_count, vec_f.data(), record_ids_);
        rc.Record("add vectors to engine");
        if(!stat.ok()) {
            error_code_ = SERVER_UNEXPECTED_ERROR;
            error_msg_ = "AddVectorTask: Engine failed when InsertVectors: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        if(record_ids_.size() != vec_count) {
            SERVER_LOG_ERROR << "AddVectorTask: Vector ID not returned";
            return SERVER_UNEXPECTED_ERROR;
        }

        rc.Record("do insert");
        rc.Elapse("total cost");

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << "AddVectorTask: " << error_msg_;
        return error_code_;
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
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "SearchVectorTask: Table name cannot be empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        if(top_k_ <= 0 || record_array_.empty()) {
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "SearchVectorTask: Invalid topk value, or query record array is empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            error_code_ = SERVER_TABLE_NOT_EXIST;
            error_msg_ = "SearchVectorTask: Engine failed when DescribeTable: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 3: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        error_code_ = ConvertTimeRangeToDBDates(range_array_, dates);
        if(error_code_ != SERVER_SUCCESS) {
            error_msg_ = "SearchVectorTask: Invalid query range when ConvertTimeRangeToDBDates";
            return error_code_;
        }

        rc.Record("check validation");

        //step 3: prepare float data
        std::vector<float> vec_f;
        error_code_ = ConvertRowRecordToFloatArray(record_array_, table_info.dimension_, vec_f);
        if(error_code_ != SERVER_SUCCESS) {
            error_msg_ = "Invalid row record data when ConvertRowRecordToFloatArray";
            return error_code_;
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
            SERVER_LOG_ERROR << "SearchVectorTask: Engine failed: " << stat.ToString();
            return SERVER_UNEXPECTED_ERROR;
        }

        if(results.size() != record_count) {
            SERVER_LOG_ERROR << "SearchVectorTask: Search result not returned";
            return SERVER_UNEXPECTED_ERROR;
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
                thrift_result.__set_score(pair.second);

                thrift_topk_result.query_result_arrays.emplace_back(thrift_result);
            }

            result_array_.emplace_back(thrift_topk_result);
        }
        rc.Record("construct result");
        rc.Elapse("total cost");

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << "SearchVectorTask: " << error_msg_;
        return error_code_;
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
            error_code_ = SERVER_INVALID_ARGUMENT;
            error_msg_ = "GetTableRowCountTask: Table name cannot be empty";
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        //step 2: get row count
        uint64_t row_count = 0;
        engine::Status stat = DBWrapper::DB()->GetTableRowCount(table_name_, row_count);
        if (!stat.ok()) {
            error_code_ = SERVER_UNEXPECTED_ERROR;
            error_msg_ = "GetTableRowCountTask: Engine failed: " + stat.ToString();
            SERVER_LOG_ERROR << error_msg_;
            return error_code_;
        }

        row_count_ = (int64_t) row_count;

        rc.Elapse("total cost");

    } catch (std::exception& ex) {
        error_code_ = SERVER_UNEXPECTED_ERROR;
        error_msg_ = ex.what();
        SERVER_LOG_ERROR << "GetTableRowCountTask: " << error_msg_;
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
        result_ = MILVUS_VERSION;
    }

    return SERVER_SUCCESS;
}

}
}
}
