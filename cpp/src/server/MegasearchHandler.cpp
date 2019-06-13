/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "MegasearchHandler.h"
#include "MegasearchTask.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace milvus {
namespace server {

using namespace megasearch;

MegasearchServiceHandler::MegasearchServiceHandler() {

}

void
MegasearchServiceHandler::CreateTable(const thrift::TableSchema &param) {
    BaseTaskPtr task_ptr = CreateTableTask::Create(param);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::DeleteTable(const std::string &table_name) {
    BaseTaskPtr task_ptr = DeleteTableTask::Create(table_name);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::AddVector(std::vector<int64_t> &_return,
        const std::string &table_name,
        const std::vector<thrift::RowRecord> &record_array) {
    BaseTaskPtr task_ptr = AddVectorTask::Create(table_name, record_array, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::SearchVector(std::vector<megasearch::thrift::TopKQueryResult> & _return,
                                       const std::string& table_name,
                                       const std::vector<megasearch::thrift::RowRecord> & query_record_array,
                                       const std::vector<megasearch::thrift::Range> & query_range_array,
                                       const int64_t topk) {
    BaseTaskPtr task_ptr = SearchVectorTask::Create(table_name, query_record_array, query_range_array, topk, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::DescribeTable(thrift::TableSchema &_return, const std::string &table_name) {
    BaseTaskPtr task_ptr = DescribeTableTask::Create(table_name, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

int64_t
MegasearchServiceHandler::GetTableRowCount(const std::string& table_name) {
    int64_t row_count = 0;
    {
        BaseTaskPtr task_ptr = GetTableRowCountTask::Create(table_name, row_count);
        MegasearchScheduler::ExecTask(task_ptr);
        task_ptr->WaitToFinish();
    }

    return row_count;
}

void
MegasearchServiceHandler::ShowTables(std::vector<std::string> &_return) {
    BaseTaskPtr task_ptr = ShowTablesTask::Create(_return);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::Ping(std::string& _return, const std::string& cmd) {
    BaseTaskPtr task_ptr = PingTask::Create(cmd, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

}
}
}
