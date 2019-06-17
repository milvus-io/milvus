/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ConnectionImpl.h"
#include "version.h"

namespace milvus {

std::shared_ptr<Connection>
Connection::Create() {
    return std::shared_ptr<Connection>(new ConnectionImpl());
}

Status
Connection::Destroy(std::shared_ptr<milvus::Connection> connection_ptr) {
    if(connection_ptr != nullptr) {
        return connection_ptr->Disconnect();
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////////
ConnectionImpl::ConnectionImpl() {
    client_proxy_ = std::make_shared<ClientProxy>();
}

Status
ConnectionImpl::Connect(const ConnectParam &param) {
    return client_proxy_->Connect(param);
}

Status
ConnectionImpl::Connect(const std::string &uri) {
    return client_proxy_->Connect(uri);
}

Status
ConnectionImpl::Connected() const {
    return client_proxy_->Connected();
}

Status
ConnectionImpl::Disconnect() {
    return client_proxy_->Disconnect();
}

std::string
ConnectionImpl::ClientVersion() const {
    return MILVUS_VERSION;
}

Status
ConnectionImpl::CreateTable(const TableSchema &param) {
    return client_proxy_->CreateTable(param);
}

Status
ConnectionImpl::DeleteTable(const std::string &table_name) {
    return client_proxy_->DeleteTable(table_name);
}

Status
ConnectionImpl::AddVector(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    return client_proxy_->AddVector(table_name, record_array, id_array);
}

Status
ConnectionImpl::SearchVector(const std::string &table_name,
                             const std::vector<RowRecord> &query_record_array,
                             const std::vector<Range> &query_range_array,
                             int64_t topk,
                             std::vector<TopKQueryResult> &topk_query_result_array) {
    return client_proxy_->SearchVector(table_name, query_record_array, query_range_array, topk, topk_query_result_array);
}

Status
ConnectionImpl::DescribeTable(const std::string &table_name, TableSchema &table_schema) {
    return client_proxy_->DescribeTable(table_name, table_schema);
}

Status
ConnectionImpl::GetTableRowCount(const std::string &table_name, int64_t &row_count) {
    return client_proxy_->GetTableRowCount(table_name, row_count);
}

Status
ConnectionImpl::ShowTables(std::vector<std::string> &table_array) {
    return client_proxy_->ShowTables(table_array);
}

std::string
ConnectionImpl::ServerVersion() const {
    return client_proxy_->ServerVersion();
}

std::string
ConnectionImpl::ServerStatus() const {
    return client_proxy_->ServerStatus();
}

}

