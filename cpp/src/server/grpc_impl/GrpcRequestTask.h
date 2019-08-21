/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#pragma once
#include "GrpcRequestScheduler.h"
#include "utils/Error.h"
#include "db/Types.h"

#include "milvus.grpc.pb.h"
#include "status.pb.h"

#include <condition_variable>
#include <memory>

namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CreateTableTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const ::milvus::grpc::TableSchema &schema);

protected:
    explicit
    CreateTableTask(const ::milvus::grpc::TableSchema &request);

    ServerError
    OnExecute() override;

private:
    const ::milvus::grpc::TableSchema schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class HasTableTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name, bool &has_table);

protected:
    HasTableTask(const std::string &request, bool &has_table);

    ServerError
    OnExecute() override;


private:
    std::string table_name_;
    bool &has_table_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DescribeTableTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name, ::milvus::grpc::TableSchema &schema);

protected:
    DescribeTableTask(const std::string &table_name, ::milvus::grpc::TableSchema &schema);

    ServerError
    OnExecute() override;


private:
    std::string table_name_;
    ::milvus::grpc::TableSchema &schema_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DropTableTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name);

protected:
    explicit
    DropTableTask(const std::string &table_name);

    ServerError
    OnExecute() override;


private:
    std::string table_name_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CreateIndexTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const ::milvus::grpc::IndexParam &index_Param);

protected:
    explicit
    CreateIndexTask(const ::milvus::grpc::IndexParam &index_Param);

    ServerError
    OnExecute() override;


private:
    ::milvus::grpc::IndexParam index_param_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class ShowTablesTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(::grpc::ServerWriter<::milvus::grpc::TableName> &writer);

protected:
    explicit
    ShowTablesTask(::grpc::ServerWriter<::milvus::grpc::TableName> &writer);

    ServerError
    OnExecute() override;

private:
    ::grpc::ServerWriter<::milvus::grpc::TableName> writer_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class InsertTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const ::milvus::grpc::InsertParam &insert_Param,
           ::milvus::grpc::VectorIds &record_ids_);

protected:
    InsertTask(const ::milvus::grpc::InsertParam &insert_Param,
                     ::milvus::grpc::VectorIds &record_ids_);

    ServerError
    OnExecute() override;

private:
    const ::milvus::grpc::InsertParam insert_param_;
    ::milvus::grpc::VectorIds &record_ids_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class SearchTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const ::milvus::grpc::SearchParam &search_param,
           const std::vector<std::string> &file_id_array,
           ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> &writer);

protected:
    SearchTask(const ::milvus::grpc::SearchParam &search_param,
                     const std::vector<std::string> &file_id_array,
                     ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> &writer);

    ServerError
    OnExecute() override;

private:
    const ::milvus::grpc::SearchParam search_param_;
    std::vector<std::string> file_id_array_;
    ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> writer_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CountTableTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name, int64_t &row_count);

protected:
    CountTableTask(const std::string &table_name, int64_t &row_count);

    ServerError
    OnExecute() override;

private:
    std::string table_name_;
    int64_t &row_count_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class CmdTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &cmd, std::string &result);

protected:
    CmdTask(const std::string &cmd, std::string &result);

    ServerError
    OnExecute() override;

private:
    std::string cmd_;
    std::string &result_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DeleteByRangeTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const ::milvus::grpc::DeleteByRangeParam &delete_by_range_param);

protected:
    DeleteByRangeTask(const ::milvus::grpc::DeleteByRangeParam &delete_by_range_param);

    ServerError
    OnExecute() override;

private:
    ::milvus::grpc::DeleteByRangeParam delete_by_range_param_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class PreloadTableTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name);

protected:
    PreloadTableTask(const std::string &table_name);

    ServerError
    OnExecute() override;

private:
    std::string table_name_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DescribeIndexTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name,
            ::milvus::grpc::IndexParam &index_param);

protected:
    DescribeIndexTask(const std::string &table_name,
            ::milvus::grpc::IndexParam &index_param);

    ServerError
    OnExecute() override;

private:
    std::string table_name_;
    ::milvus::grpc::IndexParam& index_param_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DropIndexTask : public GrpcBaseTask {
public:
    static BaseTaskPtr
    Create(const std::string &table_name);

protected:
    DropIndexTask(const std::string &table_name);

    ServerError
    OnExecute() override;

private:
    std::string table_name_;

};

}
}
}
}