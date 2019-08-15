/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#pragma once

#include "MilvusApi.h"
#ifdef MILVUS_ENABLE_THRIFT
#include "src/sdk/thrift/ClientProxy.h"
#else
#include "src/sdk/grpc/ClientProxy.h"
#endif

namespace milvus {

class ConnectionImpl : public Connection {
public:
    ConnectionImpl();

    // Implementations of the Connection interface
    virtual Status
    Connect(const ConnectParam &param) override;

    virtual Status
    Connect(const std::string &uri) override;

    virtual Status
    Connected() const override;

    virtual Status
    Disconnect() override;

    virtual Status
    CreateTable(const TableSchema &param) override;

    virtual
    bool HasTable(const std::string &table_name) override;

    virtual Status
    DropTable(const std::string &table_name) override;

    virtual Status
    CreateIndex(const IndexParam &index_param) override;

    virtual Status
    Insert(const std::string &table_name,
           const std::vector<RowRecord> &record_array,
           std::vector<int64_t> &id_array) override;

    virtual Status
    Search(const std::string &table_name,
                                const std::vector<RowRecord> &query_record_array,
                                const std::vector<Range> &query_range_array,
                                int64_t topk,
                                int64_t nprobe,
                                std::vector<TopKQueryResult> &topk_query_result_array) override;

    virtual Status
    DescribeTable(const std::string &table_name, TableSchema &table_schema) override;

    virtual Status
    CountTable(const std::string &table_name, int64_t &row_count) override;

    virtual Status
    ShowTables(std::vector<std::string> &table_array) override;

    virtual std::string
    ClientVersion() const override;

    virtual std::string
    ServerVersion() const override;

    virtual std::string
    ServerStatus() const override;

    virtual Status
    DeleteByRange(Range &range,
                  const std::string &table_name) override;

    virtual Status
    PreloadTable(const std::string &table_name) const override;

    virtual IndexParam
    DescribeIndex(const std::string &table_name) const override;

    virtual Status
    DropIndex(const std::string &table_name) const override;

private:
    std::shared_ptr<ClientProxy> client_proxy_;
};

}
