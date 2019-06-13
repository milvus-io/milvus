/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MegasearchScheduler.h"
#include "utils/Error.h"
#include "utils/AttributeSerializer.h"
#include "db/Types.h"

#include "megasearch_types.h"

#include <condition_variable>
#include <memory>

namespace zilliz {
namespace milvus {
namespace server {

using namespace megasearch;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CreateTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const thrift::TableSchema& schema);

protected:
    CreateTableTask(const thrift::TableSchema& schema);

    ServerError OnExecute() override;

private:
    const thrift::TableSchema& schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DescribeTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name, thrift::TableSchema& schema);

protected:
    DescribeTableTask(const std::string& table_name, thrift::TableSchema& schema);

    ServerError OnExecute() override;


private:
    std::string table_name_;
    thrift::TableSchema& schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DeleteTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name);

protected:
    DeleteTableTask(const std::string& table_name);

    ServerError OnExecute() override;


private:
    std::string table_name_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class ShowTablesTask : public BaseTask {
public:
    static BaseTaskPtr Create(std::vector<std::string>& tables);

protected:
    ShowTablesTask(std::vector<std::string>& tables);

    ServerError OnExecute() override;

private:
    std::vector<std::string>& tables_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class AddVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name,
                              const std::vector<thrift::RowRecord>& record_array,
                              std::vector<int64_t>& record_ids_);

protected:
    AddVectorTask(const std::string& table_name,
                        const std::vector<thrift::RowRecord>& record_array,
                        std::vector<int64_t>& record_ids_);

    ServerError OnExecute() override;

private:
    std::string table_name_;
    const std::vector<thrift::RowRecord>& record_array_;
    std::vector<int64_t>& record_ids_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class SearchVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name,
                              const std::vector<thrift::RowRecord> & query_record_array,
                              const std::vector<megasearch::thrift::Range> & query_range_array,
                              const int64_t top_k,
                              std::vector<thrift::TopKQueryResult>& result_array);

protected:
    SearchVectorTask(const std::string& table_name,
                     const std::vector<thrift::RowRecord> & query_record_array,
                     const std::vector<megasearch::thrift::Range> & query_range_array,
                     const int64_t top_k,
                     std::vector<thrift::TopKQueryResult>& result_array);

    ServerError OnExecute() override;

private:
    std::string table_name_;
    int64_t top_k_;
    const std::vector<thrift::RowRecord>& record_array_;
    const std::vector<megasearch::thrift::Range>& range_array_;
    std::vector<thrift::TopKQueryResult>& result_array_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class GetTableRowCountTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name, int64_t& row_count);

protected:
    GetTableRowCountTask(const std::string& table_name, int64_t& row_count);

    ServerError OnExecute() override;

private:
    std::string table_name_;
    int64_t& row_count_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class PingTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& cmd, std::string& result);

protected:
    PingTask(const std::string& cmd, std::string& result);

    ServerError OnExecute() override;

private:
    std::string cmd_;
    std::string& result_;
};

}
}
}