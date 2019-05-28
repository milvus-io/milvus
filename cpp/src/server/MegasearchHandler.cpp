/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "MegasearchHandler.h"
#include "MegasearchTask.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace vecwise {
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
MegasearchServiceHandler::CreateTablePartition(const thrift::CreateTablePartitionParam &param) {
    // Your implementation goes here
    printf("CreateTablePartition\n");
}

void
MegasearchServiceHandler::DeleteTablePartition(const thrift::DeleteTablePartitionParam &param) {
    // Your implementation goes here
    printf("DeleteTablePartition\n");
}

void
MegasearchServiceHandler::AddVector(std::vector<int64_t> &_return,
        const std::string &table_name,
        const std::vector<thrift::RowRecord> &record_array) {
    BaseTaskPtr task_ptr = AddVectorTask::Create(table_name, record_array, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::SearchVector(std::vector<thrift::TopKQueryResult> &_return,
        const std::string &table_name,
        const std::vector<thrift::QueryRecord> &query_record_array,
        const int64_t topk) {
    BaseTaskPtr task_ptr = SearchVectorTask::Create(table_name, query_record_array, topk, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::DescribeTable(thrift::TableSchema &_return, const std::string &table_name) {
    BaseTaskPtr task_ptr = DescribeTableTask::Create(table_name, _return);
    MegasearchScheduler::ExecTask(task_ptr);
}

void
MegasearchServiceHandler::ShowTables(std::vector<std::string> &_return) {
    // Your implementation goes here
    printf("ShowTables\n");
}

void
MegasearchServiceHandler::Ping(std::string& _return, const std::string& cmd) {
    // Your implementation goes here
    printf("Ping\n");
}

}
}
}
