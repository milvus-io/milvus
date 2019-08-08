/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "RequestHandler.h"
#include "RequestTask.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace milvus {
namespace server {

using namespace ::milvus;

RequestHandler::RequestHandler() {

}

void
RequestHandler::CreateTable(const thrift::TableSchema &param) {
    BaseTaskPtr task_ptr = CreateTableTask::Create(param);
    RequestScheduler::ExecTask(task_ptr);
}

bool
RequestHandler::HasTable(const std::string &table_name) {
    bool has_table = false;
    BaseTaskPtr task_ptr = HasTableTask::Create(table_name, has_table);
    RequestScheduler::ExecTask(task_ptr);

    return has_table;
}

void
RequestHandler::DeleteTable(const std::string &table_name) {
    BaseTaskPtr task_ptr = DeleteTableTask::Create(table_name);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::BuildIndex(const std::string &table_name) {
    BaseTaskPtr task_ptr = BuildIndexTask::Create(table_name);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::AddVector(std::vector<int64_t> &_return,
                          const std::string &table_name,
                          const std::vector<thrift::RowRecord> &record_array) {
    BaseTaskPtr task_ptr = AddVectorTask::Create(table_name, record_array, _return);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::SearchVector(std::vector<thrift::TopKQueryResult> &_return,
                             const std::string &table_name,
                             const std::vector<thrift::RowRecord> &query_record_array,
                             const std::vector<thrift::Range> &query_range_array,
                             const int64_t topk) {
//    SERVER_LOG_DEBUG << "Entering RequestHandler::SearchVector";
    BaseTaskPtr task_ptr = SearchVectorTask1::Create(table_name, std::vector<std::string>(), query_record_array,
            query_range_array, topk, _return);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::SearchVector2(std::vector<thrift::TopKQueryBinResult> & _return,
        const std::string& table_name,
        const std::vector<thrift::RowRecord> & query_record_array,
        const std::vector<thrift::Range> & query_range_array,
        const int64_t topk) {
    BaseTaskPtr task_ptr = SearchVectorTask2::Create(table_name, std::vector<std::string>(), query_record_array,
                                                    query_range_array, topk, _return);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::SearchVectorInFiles(std::vector<::milvus::thrift::TopKQueryResult> &_return,
                                    const std::string& table_name,
                                    const std::vector<std::string> &file_id_array,
                                    const std::vector<::milvus::thrift::RowRecord> &query_record_array,
                                    const std::vector<::milvus::thrift::Range> &query_range_array,
                                    const int64_t topk) {
//    SERVER_LOG_DEBUG << "Entering RequestHandler::SearchVectorInFiles. file_id_array size = " << std::to_string(file_id_array.size());
    BaseTaskPtr task_ptr = SearchVectorTask1::Create(table_name, file_id_array, query_record_array,
            query_range_array, topk, _return);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::DescribeTable(thrift::TableSchema &_return, const std::string &table_name) {
    BaseTaskPtr task_ptr = DescribeTableTask::Create(table_name, _return);
    RequestScheduler::ExecTask(task_ptr);
}

int64_t
RequestHandler::GetTableRowCount(const std::string& table_name) {
    int64_t row_count = 0;
    {
        BaseTaskPtr task_ptr = GetTableRowCountTask::Create(table_name, row_count);
        RequestScheduler::ExecTask(task_ptr);
        task_ptr->WaitToFinish();
    }

    return row_count;
}

void
RequestHandler::ShowTables(std::vector<std::string> &_return) {
    BaseTaskPtr task_ptr = ShowTablesTask::Create(_return);
    RequestScheduler::ExecTask(task_ptr);
}

void
RequestHandler::Ping(std::string& _return, const std::string& cmd) {
    BaseTaskPtr task_ptr = PingTask::Create(cmd, _return);
    RequestScheduler::ExecTask(task_ptr);
}

}
}
}
