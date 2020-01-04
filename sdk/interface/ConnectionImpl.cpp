// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "interface/ConnectionImpl.h"

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
ConnectionImpl::Connect(const ConnectParam& param) {
    return client_proxy_->Connect(param);
}

Status
ConnectionImpl::Connect(const std::string& uri) {
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
ConnectionImpl::CreateTable(const TableSchema& param) {
    return client_proxy_->CreateTable(param);
}

bool
ConnectionImpl::HasTable(const std::string& table_name) {
    return client_proxy_->HasTable(table_name);
}

Status
ConnectionImpl::DropTable(const std::string& table_name) {
    return client_proxy_->DropTable(table_name);
}

Status
ConnectionImpl::CreateIndex(const IndexParam& index_param) {
    return client_proxy_->CreateIndex(index_param);
}

Status
ConnectionImpl::Insert(const std::string& table_name, const std::string& partition_tag,
                       const std::vector<RowRecord>& record_array, std::vector<int64_t>& id_array) {
    return client_proxy_->Insert(table_name, partition_tag, record_array, id_array);
}

Status
ConnectionImpl::Search(const std::string& table_name, const std::vector<std::string>& partition_tags,
                       const std::vector<RowRecord>& query_record_array, const std::vector<Range>& query_range_array,
                       int64_t topk, int64_t nprobe, TopKQueryResult& topk_query_result) {
    return client_proxy_->Search(table_name, partition_tags, query_record_array, query_range_array, topk, nprobe,
                                 topk_query_result);
}

Status
ConnectionImpl::DescribeTable(const std::string& table_name, TableSchema& table_schema) {
    return client_proxy_->DescribeTable(table_name, table_schema);
}

Status
ConnectionImpl::CountTable(const std::string& table_name, int64_t& row_count) {
    return client_proxy_->CountTable(table_name, row_count);
}

Status
ConnectionImpl::ShowTables(std::vector<std::string>& table_array) {
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

std::string
ConnectionImpl::DumpTaskTables() const {
    return client_proxy_->DumpTaskTables();
}

Status
ConnectionImpl::DeleteByDate(const std::string& table_name, const Range& range) {
    return client_proxy_->DeleteByDate(table_name, range);
}

Status
ConnectionImpl::PreloadTable(const std::string& table_name) const {
    return client_proxy_->PreloadTable(table_name);
}

Status
ConnectionImpl::DescribeIndex(const std::string& table_name, IndexParam& index_param) const {
    return client_proxy_->DescribeIndex(table_name, index_param);
}

Status
ConnectionImpl::DropIndex(const std::string& table_name) const {
    return client_proxy_->DropIndex(table_name);
}

Status
ConnectionImpl::CreatePartition(const PartitionParam& param) {
    return client_proxy_->CreatePartition(param);
}

Status
ConnectionImpl::ShowPartitions(const std::string& table_name, PartitionList& partition_array) const {
    return client_proxy_->ShowPartitions(table_name, partition_array);
}

Status
ConnectionImpl::DropPartition(const PartitionParam& param) {
    return client_proxy_->DropPartition(param);
}

Status
ConnectionImpl::GetConfig(const std::string& node_name, std::string& value) const {
    return client_proxy_->GetConfig(node_name, value);
}

Status
ConnectionImpl::SetConfig(const std::string& node_name, const std::string& value) const {
    return client_proxy_->SetConfig(node_name, value);
}
}  // namespace milvus
