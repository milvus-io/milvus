/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "RequestScheduler.h"
#include "utils/Error.h"
#include "db/Types.h"

#include "milvus_types.h"

#include <condition_variable>
#include <memory>

namespace zilliz {
namespace milvus {
namespace server {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CreateTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const ::milvus::thrift::TableSchema& schema);

protected:
    CreateTableTask(const ::milvus::thrift::TableSchema& schema);

    ServerError OnExecute() override;

private:
    const ::milvus::thrift::TableSchema& schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class HasTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name, bool& has_table);

protected:
    HasTableTask(const std::string& table_name, bool& has_table);

    ServerError OnExecute() override;


private:
    std::string table_name_;
    bool& has_table_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DescribeTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name, ::milvus::thrift::TableSchema& schema);

protected:
    DescribeTableTask(const std::string& table_name, ::milvus::thrift::TableSchema& schema);

    ServerError OnExecute() override;


private:
    std::string table_name_;
    ::milvus::thrift::TableSchema& schema_;
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
                              const std::vector<::milvus::thrift::RowRecord>& record_array,
                              std::vector<int64_t>& record_ids_);

protected:
    AddVectorTask(const std::string& table_name,
                        const std::vector<::milvus::thrift::RowRecord>& record_array,
                        std::vector<int64_t>& record_ids_);

    ServerError OnExecute() override;

private:
    std::string table_name_;
    const std::vector<::milvus::thrift::RowRecord>& record_array_;
    std::vector<int64_t>& record_ids_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class SearchVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name,
                              const std::vector<std::string>& file_id_array,
                              const std::vector<::milvus::thrift::RowRecord> & query_record_array,
                              const std::vector<::milvus::thrift::Range> & query_range_array,
                              const int64_t top_k,
                              std::vector<::milvus::thrift::TopKQueryResult>& result_array);

protected:
    SearchVectorTask(const std::string& table_name,
                     const std::vector<std::string>& file_id_array,
                     const std::vector<::milvus::thrift::RowRecord> & query_record_array,
                     const std::vector<::milvus::thrift::Range> & query_range_array,
                     const int64_t top_k,
                     std::vector<::milvus::thrift::TopKQueryResult>& result_array);

    ServerError OnExecute() override;

private:
    std::string table_name_;
    std::vector<std::string> file_id_array_;
    int64_t top_k_;
    const std::vector<::milvus::thrift::RowRecord>& record_array_;
    const std::vector<::milvus::thrift::Range>& range_array_;
    std::vector<::milvus::thrift::TopKQueryResult>& result_array_;
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