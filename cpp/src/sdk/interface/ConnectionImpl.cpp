/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#include "ConnectionImpl.h"

namespace milvus {

std::shared_ptr<Connection>
Connection::Create() {
    return std::shared_ptr<Connection>(new ConnectionImpl());
}

Status
Connection::Destroy(std::shared_ptr<milvus::Connection>& connection_ptr) {
    if (connection_ptr != nullptr) {
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
    return client_proxy_->ClientVersion();
}

Status
ConnectionImpl::CreateTable(const TableSchema &param) {
    return client_proxy_->CreateTable(param);
}

bool
ConnectionImpl::HasTable(const std::string &table_name) {
    return client_proxy_->HasTable(table_name);
}

Status
ConnectionImpl::DropTable(const std::string &table_name) {
    return client_proxy_->DropTable(table_name);
}

Status
ConnectionImpl::CreateIndex(const IndexParam &index_param) {
    return client_proxy_->CreateIndex(index_param);
}

Status
ConnectionImpl::Insert(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    return client_proxy_->Insert(table_name, record_array, id_array);
}


Status
ConnectionImpl::Search(const std::string &table_name,
                             const std::vector<RowRecord> &query_record_array,
                             const std::vector<Range> &query_range_array,
                             int64_t topk,
                             int64_t nprobe,
                             std::vector<TopKQueryResult> &topk_query_result_array) {
    return client_proxy_->Search(table_name, query_record_array, query_range_array, topk,
                                 nprobe, topk_query_result_array);
}

Status
ConnectionImpl::DescribeTable(const std::string &table_name, TableSchema &table_schema) {
    return client_proxy_->DescribeTable(table_name, table_schema);
}

Status
ConnectionImpl::CountTable(const std::string &table_name, int64_t &row_count) {
    return client_proxy_->CountTable(table_name, row_count);
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

Status
ConnectionImpl::DeleteByRange(Range &range,
              const std::string &table_name) {
    return client_proxy_->DeleteByRange(range, table_name);
}

Status
ConnectionImpl::PreloadTable(const std::string &table_name) const {
    return client_proxy_->PreloadTable(table_name);
}

IndexParam
ConnectionImpl::DescribeIndex(const std::string &table_name) const {

}

Status
ConnectionImpl::DropIndex(const std::string &table_name) const {

}

}
