/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#pragma once

#include "../include/MilvusApi.h"
#include "src/sdk/grpc/ClientProxy.h"

namespace milvus {

class ConnectionImpl : public Connection {
public:
    ConnectionImpl();

    // Implementations of the Connection interface
    virtual Status Connect(const ConnectParam &param) override;

    virtual Status Connect(const std::string &uri) override;

    virtual Status Connected() const override;

    virtual Status Disconnect() override;

    virtual Status CreateTable(const TableSchema &param) override;

    virtual bool HasTable(const std::string &table_name) override;

    virtual Status DropTable(const std::string &table_name) override;

    virtual Status DeleteTable(const std::string &table_name) override;

    virtual Status BuildIndex(const std::string &table_name) override;

    virtual Status InsertVector(const std::string &table_name,
                             const std::vector<RowRecord> &record_array,
                             std::vector<int64_t> &id_array) override;

    virtual Status AddVector(const std::string &table_name,
                                const std::vector<RowRecord> &record_array,
                                std::vector<int64_t> &id_array) override;

    virtual Status SearchVector(const std::string &table_name,
                                const std::vector<RowRecord> &query_record_array,
                                const std::vector<Range> &query_range_array,
                                int64_t topk,
                                std::vector<TopKQueryResult> &topk_query_result_array) override;

    virtual Status DescribeTable(const std::string &table_name, TableSchema &table_schema) override;

    virtual Status GetTableRowCount(const std::string &table_name, int64_t &row_count) override;

    virtual Status ShowTables(std::vector<std::string> &table_array) override;

    virtual std::string ClientVersion() const override;

    virtual std::string ServerVersion() const override;

    virtual std::string ServerStatus() const override;

private:
    std::shared_ptr<ClientProxy> client_proxy_;
};

}
