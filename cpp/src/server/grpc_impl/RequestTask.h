/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once
#include "RequestScheduler.h"
#include "utils/Error.h"
#include "db/Types.h"

#include "milvus.grpc.pb.h"
#include "status.pb.h"

#include <condition_variable>
#include <memory>

namespace zilliz {
namespace milvus {
namespace server {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CreateTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const ::milvus::grpc::TableSchema& schema);

protected:
    explicit CreateTableTask(const ::milvus::grpc::TableSchema& request);

    ServerError OnExecute() override;

private:
    const ::milvus::grpc::TableSchema schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class HasTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name, bool& has_table);

protected:
    HasTableTask(const std::string& request, bool& has_table);

    ServerError OnExecute() override;


private:
    std::string table_name_;
    bool& has_table_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DescribeTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name, ::milvus::grpc::TableSchema& schema);

protected:
    DescribeTableTask(const std::string& table_name, ::milvus::grpc::TableSchema& schema);

    ServerError OnExecute() override;


private:
    std::string table_name_;
    ::milvus::grpc::TableSchema& schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DropTableTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name);

protected:
    explicit  DropTableTask(const std::string& table_name);

    ServerError OnExecute() override;


private:
    std::string table_name_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class BuildIndexTask : public BaseTask {
public:
    static BaseTaskPtr Create(const std::string& table_name);

protected:
    explicit BuildIndexTask(const std::string& table_name);

    ServerError OnExecute() override;


private:
    std::string table_name_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class ShowTablesTask : public BaseTask {
public:
    static BaseTaskPtr Create(::grpc::ServerWriter< ::milvus::grpc::TableName>& writer);

protected:
    explicit ShowTablesTask(::grpc::ServerWriter< ::milvus::grpc::TableName>& writer);

    ServerError OnExecute() override;

private:
    ::grpc::ServerWriter< ::milvus::grpc::TableName> writer_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class InsertVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const ::milvus::grpc::InsertInfos& insert_infos,
                              ::milvus::grpc::VectorIds& record_ids_);

protected:
    InsertVectorTask(const ::milvus::grpc::InsertInfos& insert_infos,
                  ::milvus::grpc::VectorIds& record_ids_);

    ServerError OnExecute() override;

private:
    const ::milvus::grpc::InsertInfos insert_infos_;
    ::milvus::grpc::VectorIds& record_ids_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class SearchVectorTask : public BaseTask {
public:
    static BaseTaskPtr Create(const ::milvus::grpc::SearchVectorInfos& searchVectorInfos,
                              const std::vector<std::string>& file_id_array,
                              ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>& writer);

protected:
    SearchVectorTask(const ::milvus::grpc::SearchVectorInfos& searchVectorInfos,
                     const std::vector<std::string>& file_id_array,
                     ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>& writer);

    ServerError OnExecute() override;

private:
    const ::milvus::grpc::SearchVectorInfos search_vector_infos_;
    std::vector<std::string> file_id_array_;
    ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> writer_;
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