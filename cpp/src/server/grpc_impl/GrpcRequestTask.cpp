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

#include "server/grpc_impl/GrpcRequestTask.h"

#include <string.h>
#include <map>
#include <vector>
#include <string>
//#include <gperftools/profiler.h>

#include "server/Server.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"
#include "GrpcServer.h"
#include "db/Utils.h"
#include "scheduler/SchedInst.h"
#include "../../../version.h"

namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

static const char *DQL_TASK_GROUP = "dql";
static const char *DDL_DML_TASK_GROUP = "ddl_dml";
static const char *PING_TASK_GROUP = "ping";

using DB_META = zilliz::milvus::engine::meta::Meta;
using DB_DATE = zilliz::milvus::engine::meta::DateT;

namespace {
engine::EngineType
EngineType(int type) {
    static std::map<int, engine::EngineType> map_type = {
        {0, engine::EngineType::INVALID},
        {1, engine::EngineType::FAISS_IDMAP},
        {2, engine::EngineType::FAISS_IVFFLAT},
        {3, engine::EngineType::FAISS_IVFSQ8},
    };

    if (map_type.find(type) == map_type.end()) {
        return engine::EngineType::INVALID;
    }

    return map_type[type];
}

int
IndexType(engine::EngineType type) {
    static std::map<engine::EngineType, int> map_type = {
        {engine::EngineType::INVALID, 0},
        {engine::EngineType::FAISS_IDMAP, 1},
        {engine::EngineType::FAISS_IVFFLAT, 2},
        {engine::EngineType::FAISS_IVFSQ8, 3},
    };

    if (map_type.find(type) == map_type.end()) {
        return 0;
    }

    return map_type[type];
}

constexpr int64_t DAY_SECONDS = 24 * 60 * 60;

Status
ConvertTimeRangeToDBDates(const std::vector<::milvus::grpc::Range> &range_array,
                          std::vector<DB_DATE> &dates) {
    dates.clear();
    for (auto &range : range_array) {
        time_t tt_start, tt_end;
        tm tm_start, tm_end;
        if (!CommonUtil::TimeStrToTime(range.start_value(), tt_start, tm_start)) {
            return Status(SERVER_INVALID_TIME_RANGE, "Invalid time range: " + range.start_value());
        }

        if (!CommonUtil::TimeStrToTime(range.end_value(), tt_end, tm_end)) {
            return Status(SERVER_INVALID_TIME_RANGE, "Invalid time range: " + range.start_value());
        }

        int64_t days = (tt_end > tt_start) ? (tt_end - tt_start) / DAY_SECONDS : (tt_start - tt_end) /
            DAY_SECONDS;
        if (days == 0) {
            return Status(SERVER_INVALID_TIME_RANGE,
                          "Invalid time range: " + range.start_value() + " to " + range.end_value());
        }

        //range: [start_day, end_day)
        for (int64_t i = 0; i < days; i++) {
            time_t tt_day = tt_start + DAY_SECONDS * i;
            tm tm_day;
            CommonUtil::ConvertTime(tt_day, tm_day);

            int64_t date = tm_day.tm_year * 10000 + tm_day.tm_mon * 100 +
                tm_day.tm_mday;//according to db logic
            dates.push_back(date);
        }
    }

    return Status::OK();
}
} // namespace

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CreateTableTask::CreateTableTask(const ::milvus::grpc::TableSchema *schema)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      schema_(schema) {
}

BaseTaskPtr
CreateTableTask::Create(const ::milvus::grpc::TableSchema *schema) {
    if (schema == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseTask>(new CreateTableTask(schema));
}

Status
CreateTableTask::OnExecute() {
    TimeRecorder rc("CreateTableTask");

    try {
        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(schema_->table_name());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableDimension(schema_->dimension());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexFileSize(schema_->index_file_size());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexMetricType(schema_->metric_type());
        if (!status.ok()) {
            return status;
        }

        //step 2: construct table schema
        engine::meta::TableSchema table_info;
        table_info.table_id_ = schema_->table_name();
        table_info.dimension_ = (uint16_t) schema_->dimension();
        table_info.index_file_size_ = schema_->index_file_size();
        table_info.metric_type_ = schema_->metric_type();

        //step 3: create table
        status = DBWrapper::DB()->CreateTable(table_info);
        if (!status.ok()) {
            //table could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_TABLE_NAME, status.message());
            }
            return status;
        }
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DescribeTableTask::DescribeTableTask(const std::string &table_name, ::milvus::grpc::TableSchema *schema)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name),
      schema_(schema) {
}

BaseTaskPtr
DescribeTableTask::Create(const std::string &table_name, ::milvus::grpc::TableSchema *schema) {
    return std::shared_ptr<GrpcBaseTask>(new DescribeTableTask(table_name, schema));
}

Status
DescribeTableTask::OnExecute() {
    TimeRecorder rc("DescribeTableTask");

    try {
        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: get table info
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            return status;
        }

        schema_->set_table_name(table_info.table_id_);
        schema_->set_dimension(table_info.dimension_);
        schema_->set_index_file_size(table_info.index_file_size_);
        schema_->set_metric_type(table_info.metric_type_);
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    rc.ElapseFromBegin("totally cost");

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CreateIndexTask::CreateIndexTask(const ::milvus::grpc::IndexParam *index_param)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      index_param_(index_param) {
}

BaseTaskPtr
CreateIndexTask::Create(const ::milvus::grpc::IndexParam *index_param) {
    if (index_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseTask>(new CreateIndexTask(index_param));
}

Status
CreateIndexTask::OnExecute() {
    try {
        TimeRecorder rc("CreateIndexTask");

        //step 1: check arguments
        std::string table_name_ = index_param_->table_name();
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        bool has_table = false;
        status = DBWrapper::DB()->HasTable(table_name_, has_table);
        if (!status.ok()) {
            return status;
        }

        if (!has_table) {
            return Status(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
        }

        auto &grpc_index = index_param_->index();
        status = ValidationUtil::ValidateTableIndexType(grpc_index.index_type());
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableIndexNlist(grpc_index.nlist());
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        engine::TableIndex index;
        index.engine_type_ = grpc_index.index_type();
        index.nlist_ = grpc_index.nlist();
        status = DBWrapper::DB()->CreateIndex(table_name_, index);
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("totally cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
HasTableTask::HasTableTask(const std::string &table_name, bool &has_table)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name),
      has_table_(has_table) {
}

BaseTaskPtr
HasTableTask::Create(const std::string &table_name, bool &has_table) {
    return std::shared_ptr<GrpcBaseTask>(new HasTableTask(table_name, has_table));
}

Status
HasTableTask::OnExecute() {
    try {
        TimeRecorder rc("HasTableTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        status = DBWrapper::DB()->HasTable(table_name_, has_table_);
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("totally cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DropTableTask::DropTableTask(const std::string &table_name)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name) {
}

BaseTaskPtr
DropTableTask::Create(const std::string &table_name) {
    return std::shared_ptr<GrpcBaseTask>(new DropTableTask(table_name));
}

Status
DropTableTask::OnExecute() {
    try {
        TimeRecorder rc("DropTableTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
            } else {
                return status;
            }
        }

        rc.ElapseFromBegin("check validation");

        //step 3: Drop table
        std::vector<DB_DATE> dates;
        status = DBWrapper::DB()->DeleteTable(table_name_, dates);
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("total cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ShowTablesTask::ShowTablesTask(::milvus::grpc::TableNameList *table_name_list)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_list_(table_name_list) {
}

BaseTaskPtr
ShowTablesTask::Create(::milvus::grpc::TableNameList *table_name_list) {
    return std::shared_ptr<GrpcBaseTask>(new ShowTablesTask(table_name_list));
}

Status
ShowTablesTask::OnExecute() {
    std::vector<engine::meta::TableSchema> schema_array;
    auto statuts = DBWrapper::DB()->AllTables(schema_array);
    if (!statuts.ok()) {
        return statuts;
    }

    for (auto &schema : schema_array) {
        table_name_list_->add_table_names(schema.table_id_);
    }
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
InsertTask::InsertTask(const ::milvus::grpc::InsertParam *insert_param,
                       ::milvus::grpc::VectorIds *record_ids)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      insert_param_(insert_param),
      record_ids_(record_ids) {
}

BaseTaskPtr
InsertTask::Create(const ::milvus::grpc::InsertParam *insert_param,
                   ::milvus::grpc::VectorIds *record_ids) {
    if (insert_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseTask>(new InsertTask(insert_param, record_ids));
}

Status
InsertTask::OnExecute() {
    try {
        TimeRecorder rc("InsertVectorTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(insert_param_->table_name());
        if (!status.ok()) {
            return status;
        }
        if (insert_param_->row_record_array().empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array is empty");
        }

        if (!record_ids_->vector_id_array().empty()) {
            if (record_ids_->vector_id_array().size() != insert_param_->row_record_array_size()) {
                return Status(SERVER_ILLEGAL_VECTOR_ID,
                              "Size of vector ids is not equal to row record array size");
            }
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = insert_param_->table_name();
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST,
                              "Table " + insert_param_->table_name() + " not exists");
            } else {
                return status;
            }
        }

        //step 3: check table flag
        //all user provide id, or all internal id
        bool user_provide_ids = !insert_param_->row_id_array().empty();
        //user already provided id before, all insert action require user id
        if ((table_info.flag_ & engine::meta::FLAG_MASK_HAS_USERID) && !user_provide_ids) {
            return Status(SERVER_ILLEGAL_VECTOR_ID,
                          "Table vector ids are user defined, please provide id for this batch");
        }

        //user didn't provided id before, no need to provide user id
        if ((table_info.flag_ & engine::meta::FLAG_MASK_NO_USERID) && user_provide_ids) {
            return Status(SERVER_ILLEGAL_VECTOR_ID,
                          "Table vector ids are auto generated, no need to provide id for this batch");
        }

        rc.RecordSection("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/insert_" + std::to_string(this->insert_param_->row_record_array_size())
            + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        //step 4: prepare float data
        std::vector<float> vec_f(insert_param_->row_record_array_size() * table_info.dimension_, 0);

        // TODO: change to one dimension array in protobuf or use multiple-thread to copy the data
        for (size_t i = 0; i < insert_param_->row_record_array_size(); i++) {
            if (insert_param_->row_record_array(i).vector_data().empty()) {
                return Status(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array data is empty");
            }
            uint64_t vec_dim = insert_param_->row_record_array(i).vector_data().size();
            if (vec_dim != table_info.dimension_) {
                ErrorCode error_code = SERVER_INVALID_VECTOR_DIMENSION;
                std::string error_msg = "Invalid row record dimension: " + std::to_string(vec_dim)
                    + " vs. table dimension:" +
                    std::to_string(table_info.dimension_);
                return Status(error_code, error_msg);
            }
            memcpy(&vec_f[i * table_info.dimension_],
                   insert_param_->row_record_array(i).vector_data().data(),
                   table_info.dimension_ * sizeof(float));
        }

        rc.ElapseFromBegin("prepare vectors data");

        //step 5: insert vectors
        auto vec_count = (uint64_t) insert_param_->row_record_array_size();
        std::vector<int64_t> vec_ids(insert_param_->row_id_array_size(), 0);
        if (!insert_param_->row_id_array().empty()) {
            const int64_t *src_data = insert_param_->row_id_array().data();
            int64_t *target_data = vec_ids.data();
            memcpy(target_data, src_data, (size_t) (sizeof(int64_t) * insert_param_->row_id_array_size()));
        }

        status = DBWrapper::DB()->InsertVectors(insert_param_->table_name(), vec_count, vec_f.data(), vec_ids);
        rc.ElapseFromBegin("add vectors to engine");
        if (!status.ok()) {
            return status;
        }
        for (int64_t id : vec_ids) {
            record_ids_->add_vector_id_array(id);
        }

        auto ids_size = record_ids_->vector_id_array_size();
        if (ids_size != vec_count) {
            std::string msg = "Add " + std::to_string(vec_count) + " vectors but only return "
                + std::to_string(ids_size) + " id";
            return Status(SERVER_ILLEGAL_VECTOR_ID, msg);
        }

        //step 6: update table flag
        user_provide_ids ? table_info.flag_ |= engine::meta::FLAG_MASK_HAS_USERID
                         : table_info.flag_ |= engine::meta::FLAG_MASK_NO_USERID;
        status = DBWrapper::DB()->UpdateTableFlag(insert_param_->table_name(), table_info.flag_);

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("add vectors to engine");
        rc.ElapseFromBegin("total cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SearchTask::SearchTask(const ::milvus::grpc::SearchParam *search_vector_infos,
                       const std::vector<std::string> &file_id_array,
                       ::milvus::grpc::TopKQueryResultList *response)
    : GrpcBaseTask(DQL_TASK_GROUP),
      search_param_(search_vector_infos),
      file_id_array_(file_id_array),
      topk_result_list(response) {
}

BaseTaskPtr
SearchTask::Create(const ::milvus::grpc::SearchParam *search_vector_infos,
                   const std::vector<std::string> &file_id_array,
                   ::milvus::grpc::TopKQueryResultList *response) {
    if (search_vector_infos == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }
    return std::shared_ptr<GrpcBaseTask>(new SearchTask(search_vector_infos, file_id_array, response));
}

Status
SearchTask::OnExecute() {
    try {
        int64_t top_k = search_param_->topk();
        int64_t nprobe = search_param_->nprobe();

        std::string hdr = "SearchTask(k=" + std::to_string(top_k) + ", nprob=" + std::to_string(nprobe) + ")";
        TimeRecorder rc(hdr);

        //step 1: check table name
        std::string table_name_ = search_param_->table_name();
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name_;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
            } else {
                return status;
            }
        }

        //step 3: check search parameter
        status = ValidationUtil::ValidateSearchTopk(top_k, table_info);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateSearchNprobe(nprobe, table_info);
        if (!status.ok()) {
            return status;
        }

        if (search_param_->query_record_array().empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array is empty");
        }

        //step 4: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        std::vector<::milvus::grpc::Range> range_array;
        for (size_t i = 0; i < search_param_->query_range_array_size(); i++) {
            range_array.emplace_back(search_param_->query_range_array(i));
        }

        status = ConvertTimeRangeToDBDates(range_array, dates);
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");


        //step 5: prepare float data
        auto record_array_size = search_param_->query_record_array_size();
        std::vector<float> vec_f(record_array_size * table_info.dimension_, 0);
        for (size_t i = 0; i < record_array_size; i++) {
            if (search_param_->query_record_array(i).vector_data().empty()) {
                return Status(SERVER_INVALID_ROWRECORD_ARRAY, "Row record array data is empty");
            }
            uint64_t query_vec_dim = search_param_->query_record_array(i).vector_data().size();
            if (query_vec_dim != table_info.dimension_) {
                ErrorCode error_code = SERVER_INVALID_VECTOR_DIMENSION;
                std::string error_msg = "Invalid row record dimension: " + std::to_string(query_vec_dim)
                    + " vs. table dimension:" + std::to_string(table_info.dimension_);
                return Status(error_code, error_msg);
            }

            memcpy(&vec_f[i * table_info.dimension_],
                   search_param_->query_record_array(i).vector_data().data(),
                   table_info.dimension_ * sizeof(float));
        }
        rc.RecordSection("prepare vector data");

        //step 6: search vectors
        engine::QueryResults results;
        auto record_count = (uint64_t) search_param_->query_record_array().size();

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + std::to_string(this->search_param_->query_record_array_size())
            + ".profiling";
        ProfilerStart(fname.c_str());
#endif

        if (file_id_array_.empty()) {
            status = DBWrapper::DB()->Query(table_name_, (size_t) top_k, record_count, nprobe,
                                            vec_f.data(), dates, results);
        } else {
            status = DBWrapper::DB()->Query(table_name_, file_id_array_, (size_t) top_k,
                                            record_count, nprobe, vec_f.data(), dates, results);
        }

#ifdef MILVUS_ENABLE_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("search vectors from engine");
        if (!status.ok()) {
            return status;
        }

        if (results.empty()) {
            return Status::OK(); //empty table
        }

        if (results.size() != record_count) {
            std::string msg = "Search " + std::to_string(record_count) + " vectors but only return "
                + std::to_string(results.size()) + " results";
            return Status(SERVER_ILLEGAL_SEARCH_RESULT, msg);
        }

        //step 7: construct result array
        for (auto &result : results) {
            ::milvus::grpc::TopKQueryResult *topk_query_result = topk_result_list->add_topk_query_result();
            for (auto &pair : result) {
                ::milvus::grpc::QueryResult *grpc_result = topk_query_result->add_query_result_arrays();
                grpc_result->set_id(pair.first);
                grpc_result->set_distance(pair.second);
            }
        }

        //step 8: print time cost percent
        rc.RecordSection("construct result and send");
        rc.ElapseFromBegin("totally cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CountTableTask::CountTableTask(const std::string &table_name, int64_t &row_count)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name),
      row_count_(row_count) {
}

BaseTaskPtr
CountTableTask::Create(const std::string &table_name, int64_t &row_count) {
    return std::shared_ptr<GrpcBaseTask>(new CountTableTask(table_name, row_count));
}

Status
CountTableTask::OnExecute() {
    try {
        TimeRecorder rc("GetTableRowCountTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: get row count
        uint64_t row_count = 0;
        status = DBWrapper::DB()->GetTableRowCount(table_name_, row_count);
        if (!status.ok()) {
            return status;
        }

        row_count_ = (int64_t) row_count;

        rc.ElapseFromBegin("total cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
CmdTask::CmdTask(const std::string &cmd, std::string &result)
    : GrpcBaseTask(PING_TASK_GROUP),
      cmd_(cmd),
      result_(result) {
}

BaseTaskPtr
CmdTask::Create(const std::string &cmd, std::string &result) {
    return std::shared_ptr<GrpcBaseTask>(new CmdTask(cmd, result));
}

Status
CmdTask::OnExecute() {
    if (cmd_ == "version") {
        result_ = MILVUS_VERSION;
    } else if (cmd_ == "tasktable") {
        result_ = scheduler::ResMgrInst::GetInstance()->DumpTaskTables();
    } else {
        result_ = "OK";
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DeleteByRangeTask::DeleteByRangeTask(const ::milvus::grpc::DeleteByRangeParam *delete_by_range_param)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      delete_by_range_param_(delete_by_range_param) {
}

BaseTaskPtr
DeleteByRangeTask::Create(const ::milvus::grpc::DeleteByRangeParam *delete_by_range_param) {
    if (delete_by_range_param == nullptr) {
        SERVER_LOG_ERROR << "grpc input is null!";
        return nullptr;
    }

    return std::shared_ptr<GrpcBaseTask>(new DeleteByRangeTask(delete_by_range_param));
}

Status
DeleteByRangeTask::OnExecute() {
    try {
        TimeRecorder rc("DeleteByRangeTask");

        //step 1: check arguments
        std::string table_name = delete_by_range_param_->table_name();
        auto status = ValidationUtil::ValidateTableName(table_name);
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        engine::meta::TableSchema table_info;
        table_info.table_id_ = table_name;
        status = DBWrapper::DB()->DescribeTable(table_info);
        if (!status.ok()) {
            if (status.code(), DB_NOT_FOUND) {
                return Status(SERVER_TABLE_NOT_EXIST, "Table " + table_name + " not exists");
            } else {
                return status;
            }
        }

        rc.ElapseFromBegin("check validation");

        //step 3: check date range, and convert to db dates
        std::vector<DB_DATE> dates;
        ErrorCode error_code = SERVER_SUCCESS;
        std::string error_msg;

        std::vector<::milvus::grpc::Range> range_array;
        range_array.emplace_back(delete_by_range_param_->range());
        status = ConvertTimeRangeToDBDates(range_array, dates);
        if (!status.ok()) {
            return status;
        }

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/search_nq_" + this->delete_by_range_param_->table_name() + ".profiling";
        ProfilerStart(fname.c_str());
#endif
        status = DBWrapper::DB()->DeleteTable(table_name, dates);
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
PreloadTableTask::PreloadTableTask(const std::string &table_name)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name) {
}

BaseTaskPtr
PreloadTableTask::Create(const std::string &table_name) {
    return std::shared_ptr<GrpcBaseTask>(new PreloadTableTask(table_name));
}

Status
PreloadTableTask::OnExecute() {
    try {
        TimeRecorder rc("PreloadTableTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        status = DBWrapper::DB()->PreloadTable(table_name_);
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("totally cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DescribeIndexTask::DescribeIndexTask(const std::string &table_name,
                                     ::milvus::grpc::IndexParam *index_param)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name),
      index_param_(index_param) {
}

BaseTaskPtr
DescribeIndexTask::Create(const std::string &table_name,
                          ::milvus::grpc::IndexParam *index_param) {
    return std::shared_ptr<GrpcBaseTask>(new DescribeIndexTask(table_name, index_param));
}

Status
DescribeIndexTask::OnExecute() {
    try {
        TimeRecorder rc("DescribeIndexTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        //step 2: check table existence
        engine::TableIndex index;
        status = DBWrapper::DB()->DescribeIndex(table_name_, index);
        if (!status.ok()) {
            return status;
        }

        index_param_->set_table_name(table_name_);
        index_param_->mutable_index()->set_index_type(index.engine_type_);
        index_param_->mutable_index()->set_nlist(index.nlist_);

        rc.ElapseFromBegin("totally cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DropIndexTask::DropIndexTask(const std::string &table_name)
    : GrpcBaseTask(DDL_DML_TASK_GROUP),
      table_name_(table_name) {
}

BaseTaskPtr
DropIndexTask::Create(const std::string &table_name) {
    return std::shared_ptr<GrpcBaseTask>(new DropIndexTask(table_name));
}

Status
DropIndexTask::OnExecute() {
    try {
        TimeRecorder rc("DropIndexTask");

        //step 1: check arguments
        auto status = ValidationUtil::ValidateTableName(table_name_);
        if (!status.ok()) {
            return status;
        }

        bool has_table = false;
        status = DBWrapper::DB()->HasTable(table_name_, has_table);
        if (!status.ok()) {
            return status;
        }

        if (!has_table) {
            return Status(SERVER_TABLE_NOT_EXIST, "Table " + table_name_ + " not exists");
        }

        //step 2: check table existence
        status = DBWrapper::DB()->DropIndex(table_name_);
        if (!status.ok()) {
            return status;
        }

        rc.ElapseFromBegin("totally cost");
    } catch (std::exception &ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

} // namespace grpc
} // namespace server
} // namespace milvus
} // namespace zilliz
