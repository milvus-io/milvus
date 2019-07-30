/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "RequestTask.h"
#include "../ServerConfig.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"
#include "../DBWrapper.h"
#include "version.h"
#include "MilvusServer.h"

#include "src/server/Server.h"

namespace zilliz {
namespace milvus {
namespace server {
static const char* DQL_TASK_GROUP = "dql";
static const char* DDL_DML_TASK_GROUP = "ddl_dml";
static const char* PING_TASK_GROUP = "ping";

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

    constexpr long DAY_SECONDS = 24 * 60 * 60;

    void
    ConvertTimeRangeToDBDates(const std::vector<::milvus::grpc::Range> &range_array,
                              std::vector<DB_DATE>& dates,
                              ServerError& error_code,
                              std::string& error_msg) {
        dates.clear();
        for(auto& range : range_array) {
            time_t tt_start, tt_end;
            tm tm_start, tm_end;
            if(!CommonUtil::TimeStrToTime(range.start_value(), tt_start, tm_start)){
                error_code = SERVER_INVALID_TIME_RANGE;
                error_msg = "Invalid time range: " + range.start_value();
                return;
            }

            if(!CommonUtil::TimeStrToTime(range.end_value(), tt_end, tm_end)){
                error_code = SERVER_INVALID_TIME_RANGE;
                error_msg = "Invalid time range: " + range.start_value();
                return;
            }

            long days = (tt_end > tt_start) ? (tt_end - tt_start)/DAY_SECONDS : (tt_start - tt_end)/DAY_SECONDS;
            if(days == 0) {
                error_code = SERVER_INVALID_TIME_RANGE;
                error_msg = "Invalid time range: " + range.start_value() + " to " + range.end_value();
                return ;
            }

            for(long i = 0; i < days; i++) {
                time_t tt_day = tt_start + DAY_SECONDS*i;
                tm tm_day;
                CommonUtil::ConvertTime(tt_day, tm_day);

                long date = tm_day.tm_year*10000 + tm_day.tm_mon*100 + tm_day.tm_mday;//according to db logic
                dates.push_back(date);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CreateTableTask::CreateTableTask(const ::milvus::grpc::TableSchema& schema)
        : BaseTask(DDL_DML_TASK_GROUP),
          schema_(schema) {

}

BaseTaskPtr CreateTableTask::Create(const ::milvus::grpc::TableSchema& schema) {
    return std::shared_ptr<BaseTask>(new CreateTableTask(schema));
}

ServerError CreateTableTask::OnExecute() {
    TimeRecorder rc("CreateTableTask");

    try {
        //step 1: check arguments
        ServerError res = ValidationUtil::ValidateTableName(schema_.table_name().table_name());
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + schema_.table_name().table_name());
        }

        res = ValidationUtil::ValidateTableDimension(schema_.dimension());
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table dimension: " + std::to_string(schema_.dimension()));
        }

        res = ValidationUtil::ValidateTableIndexType(schema_.index_type());
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid index type: " + std::to_string(schema_.index_type()));
        }

        //step 2: construct table schema
        engine::meta::TableSchema table_info;
        table_info.dimension_ = (uint16_t)schema_.dimension();
        table_info.table_id_ = schema_.table_name().table_name();
        table_info.engine_type_ = (int)EngineType(schema_.index_type());
        table_info.store_raw_data_ = schema_.store_raw_vector();

        //step 3: create table
        engine::Status stat = DBWrapper::DB()->CreateTable(table_info);
        if(!stat.ok()) {
            //table could exist
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DescribeTableTask::DescribeTableTask(const std::string &table_name, ::milvus::grpc::TableSchema *schema)
        : BaseTask(DDL_DML_TASK_GROUP),
          table_name_(table_name),
          schema_(schema) {
}

BaseTaskPtr DescribeTableTask::Create(const std::string& table_name, ::milvus::grpc::TableSchema *schema) {
    return std::shared_ptr<BaseTask>(new DescribeTableTask(table_name, schema));
}

ServerError DescribeTableTask::OnExecute() {
    TimeRecorder rc("DescribeTableTask");

    try {
        //step 1: check arguments
        ServerError res = ValidationUtil::ValidateTableName(table_name_);
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + table_name_);
        }

        //step 2: get table info
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        schema_->mutable_table_name()->set_table_name(table_info.table_id_);

        schema_->set_index_type(IndexType((engine::EngineType)table_info.engine_type_));
        schema_->set_dimension(table_info.dimension_);
        schema_->set_store_raw_vector(table_info.store_raw_data_);

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");

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
        ServerError res = ValidationUtil::ValidateTableName(table_name_);
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + table_name_);
        }

        bool has_table = false;
        engine::Status stat = DBWrapper::DB()->HasTable(table_name_, has_table);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        if(!has_table) {
            return SetError(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
        }

        //step 2: check table existence
        stat = DBWrapper::DB()->BuildIndex(table_name_);
        if(!stat.ok()) {
            return SetError(SERVER_BUILD_INDEX_ERROR, "Engine failed: " + stat.ToString());
        }

        rc.ElapseFromBegin("totally cost");
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
        ServerError res = ValidationUtil::ValidateTableName(table_name_);
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + table_name_);
        }

        //step 2: check table existence
        engine::Status stat = DBWrapper::DB()->HasTable(table_name_, has_table_);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        rc.ElapseFromBegin("totally cost");
    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DropTableTask::DropTableTask(const std::string& table_name)
        : BaseTask(DDL_DML_TASK_GROUP),
          table_name_(table_name) {

}

BaseTaskPtr DropTableTask::Create(const std::string& table_name) {
    return std::shared_ptr<BaseTask>(new DropTableTask(table_name));
}

ServerError DropTableTask::OnExecute() {
    try {
        TimeRecorder rc("DropTableTask");

        //step 1: check arguments
        ServerError res = ValidationUtil::ValidateTableName(table_name_);
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + table_name_);
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

        rc.ElapseFromBegin("check validation");

        //step 3: Drop table
        std::vector<DB_DATE> dates;
        stat = DBWrapper::DB()->DeleteTable(table_name_, dates);
        if(!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        rc.ElapseFromBegin("total cost");
    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ShowTablesTask::ShowTablesTask(::grpc::ServerWriter< ::milvus::grpc::TableName>& writer)
        : BaseTask(DDL_DML_TASK_GROUP),
          writer_(writer) {

}

BaseTaskPtr ShowTablesTask::Create(::grpc::ServerWriter< ::milvus::grpc::TableName>& writer) {
    return std::shared_ptr<BaseTask>(new ShowTablesTask(writer));
}

ServerError ShowTablesTask::OnExecute() {
    std::vector<engine::meta::TableSchema> schema_array;
    engine::Status stat = DBWrapper::DB()->AllTables(schema_array);
    if(!stat.ok()) {
        return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
    }

    for(auto& schema : schema_array) {
        ::milvus::grpc::TableName tableName;
        tableName.set_table_name(schema.table_id_);
        writer_.Write(tableName);
    }
    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
InsertVectorTask::InsertVectorTask(const ::milvus::grpc::InsertInfos& insert_infos,
                                   ::milvus::grpc::VectorIds& record_ids)
        : BaseTask(DDL_DML_TASK_GROUP),
          insert_infos_(insert_infos),
          record_ids_(record_ids) {
    record_ids_.Clear();
}

BaseTaskPtr InsertVectorTask::Create(const ::milvus::grpc::InsertInfos& insert_infos,
                                  ::milvus::grpc::VectorIds& record_ids) {
    return std::shared_ptr<BaseTask>(new InsertVectorTask(insert_infos, record_ids));
}

ServerError InsertVectorTask::OnExecute() {
    try {
        TimeRecorder rc("InsertVectorTask");

        //step 1: check arguments
        ServerError res = ValidationUtil::ValidateTableName(insert_infos_.table_name());
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + insert_infos_.table_name());
        }
        if(insert_infos_.row_record_array().empty()) {
            return SetError(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array is empty");
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = insert_infos_.table_name();
        engine::Status stat = DBWrapper::DB()->DescribeTable(table_info);
        if(!stat.ok()) {
            if(stat.IsNotFound()) {
                return SetError(SERVER_TABLE_NOT_EXIST, "Table " + insert_infos_.table_name() + " not exists");
            } else {
                return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
            }
        }

        rc.RecordSection("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/insert_" + std::to_string(this->record_array_.size()) +
                            "_" + GetCurrTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        //step 3: prepare float data
        std::vector<float> vec_f(insert_infos_.row_record_array_size() * table_info.dimension_, 0);

        // TODO: change to one dimension array in protobuf or use multiple-thread to copy the data
        for (size_t i = 0; i < insert_infos_.row_record_array_size(); i++) {
            for (size_t j = 0; j < table_info.dimension_; j++) {
                if (insert_infos_.row_record_array(i).vector_data().empty()) {
                    return SetError(SERVER_INVALID_ROWRECORD_ARRAY, "Row record float array is empty");
                }
                uint64_t vec_dim = insert_infos_.row_record_array(i).vector_data().size();
                if (vec_dim != table_info.dimension_) {
                    ServerError error_code = SERVER_INVALID_VECTOR_DIMENSION;
                    std::string error_msg = "Invalid rowrecord dimension: " + std::to_string(vec_dim)
                        + " vs. table dimension:" + std::to_string(table_info.dimension_);
                    return SetError(error_code, error_msg);
                }
                vec_f[i * table_info.dimension_ + j] = insert_infos_.row_record_array(i).vector_data(j);
            }
        }

        rc.ElapseFromBegin("prepare vectors data");

        //step 4: insert vectors
        auto vec_count = (uint64_t)insert_infos_.row_record_array_size();
        std::vector<int64_t> vec_ids(record_ids_.vector_id_array_size(), 0);

        stat = DBWrapper::DB()->InsertVectors(insert_infos_.table_name(), vec_count, vec_f.data(), vec_ids);
        rc.ElapseFromBegin("add vectors to engine");
        if(!stat.ok()) {
            return SetError(SERVER_CACHE_ERROR, "Cache error: " + stat.ToString());
        }
        for (int64_t id : vec_ids) {
            record_ids_.add_vector_id_array(id);
        }

        auto ids_size = record_ids_.vector_id_array_size();
        if(ids_size != vec_count) {
            std::string msg = "Add " + std::to_string(vec_count) + " vectors but only return "
                              + std::to_string(ids_size) + " id";
            return SetError(SERVER_ILLEGAL_VECTOR_ID, msg);
        }

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("add vectors to engine");
        rc.ElapseFromBegin("total cost");

    } catch (std::exception& ex) {
        return SetError(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return SERVER_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SearchVectorTask::SearchVectorTask(const ::milvus::grpc::SearchVectorInfos& search_vector_infos,
                                   const std::vector<std::string>& file_id_array,
                                   ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>& writer)
        : BaseTask(DQL_TASK_GROUP),
          search_vector_infos_(search_vector_infos),
          file_id_array_(file_id_array),
          writer_(writer) {

}

BaseTaskPtr SearchVectorTask::Create(const ::milvus::grpc::SearchVectorInfos& search_vector_infos,
                                     const std::vector<std::string>& file_id_array,
                                     ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>& writer) {
    return std::shared_ptr<BaseTask>(new SearchVectorTask(search_vector_infos, file_id_array,
                                                          writer));
}

ServerError SearchVectorTask::OnExecute() {
    try {
        TimeRecorder rc("SearchVectorTask");

        //step 1: check arguments
        std::string table_name_ = search_vector_infos_.table_name();
        ServerError res = ValidationUtil::ValidateTableName(table_name_);
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + table_name_);
        }

        int top_k_ = search_vector_infos_.topk();

        if(top_k_ <= 0 || top_k_ > 1024) {
            return SetError(SERVER_INVALID_TOPK, "Invalid topk: " + std::to_string(
                    top_k_));
        }
        if(search_vector_infos_.query_record_array().empty()) {
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

        std::vector<::milvus::grpc::Range> range_array;
        for (size_t i = 0; i < search_vector_infos_.query_range_array_size(); i++) {
            range_array.emplace_back(search_vector_infos_.query_range_array(i));
        }
        ConvertTimeRangeToDBDates(range_array, dates, error_code, error_msg);
        if(error_code != SERVER_SUCCESS) {
            return SetError(error_code, error_msg);
        }

        double span_check = rc.RecordSection("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + std::to_string(this->record_array_.size()) +
                            "_top_" + std::to_string(this->top_k_) + "_" +
                            GetCurrTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        //step 3: prepare float data
        auto record_array_size = search_vector_infos_.query_record_array_size();
        std::vector<float> vec_f(record_array_size * table_info.dimension_, 0);
        //TODO
        for (size_t i = 0; i < record_array_size; i++) {
            for (size_t j = 0; j < table_info.dimension_; j++) {
                if (search_vector_infos_.query_record_array(i).vector_data().empty()) {
                    return SetError(SERVER_INVALID_ROWRECORD_ARRAY, "Query record float array is empty");
                }
                uint64_t query_vec_dim = search_vector_infos_.query_record_array(i).vector_data().size();
                if (query_vec_dim != table_info.dimension_) {
                    ServerError error_code = SERVER_INVALID_VECTOR_DIMENSION;
                    std::string error_msg = "Invalid rowrecord dimension: " + std::to_string(query_vec_dim)
                        + " vs. table dimension:" + std::to_string(table_info.dimension_);
                    return SetError(error_code, error_msg);
                }
                vec_f[i * table_info.dimension_ + j] = search_vector_infos_.query_record_array(i).vector_data(j);
            }
        }
        rc.ElapseFromBegin("prepare vector data");

        //step 4: search vectors
        engine::QueryResults results;
        auto record_count = (uint64_t)search_vector_infos_.query_record_array().size();

        if(file_id_array_.empty()) {
            stat = DBWrapper::DB()->Query(table_name_, (size_t) top_k_, record_count, vec_f.data(), dates, results);
        } else {
            stat = DBWrapper::DB()->Query(table_name_, file_id_array_,
                    (size_t) top_k_, record_count, vec_f.data(), dates, results);
        }

        rc.ElapseFromBegin("search vectors from engine");
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

        rc.ElapseFromBegin("do search");

        //step 5: construct result array
        for(uint64_t i = 0; i < record_count; i++) {
            auto& result = results[i];
            const auto &record = search_vector_infos_.query_record_array(i);
            ::milvus::grpc::TopKQueryResult grpc_topk_result;
            for(auto& pair : result) {
                ::milvus::grpc::QueryResult *grpc_result = grpc_topk_result.add_query_result_arrays();
                grpc_result->set_id(pair.first);
                grpc_result->set_distance(pair.second);
            }
            writer_.Write(grpc_topk_result);
        }

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        double span_result = rc.RecordSection("construct result");
        rc.ElapseFromBegin("totally cost");

        //step 6: print time cost percent

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
        ServerError res = SERVER_SUCCESS;
        res = ValidationUtil::ValidateTableName(table_name_);
        if(res != SERVER_SUCCESS) {
            return SetError(res, "Invalid table name: " + table_name_);
        }

        //step 2: get row count
        uint64_t row_count = 0;
        engine::Status stat = DBWrapper::DB()->GetTableRowCount(table_name_, row_count);
        if (!stat.ok()) {
            return SetError(DB_META_TRANSACTION_FAILED, "Engine failed: " + stat.ToString());
        }

        row_count_ = (int64_t) row_count;

        rc.ElapseFromBegin("total cost");

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
    } else if (cmd_ == "disconnect") {
        //TODO stopservice
    } else {
        result_ = "OK";
    }

    return SERVER_SUCCESS;
}

}
}
}