#include "MegaSearch.h"


namespace megasearch {
std::shared_ptr<Connection>
Create() {
    return nullptr;
}

Status
Destroy(std::shared_ptr<Connection> &connection_ptr) {
    return Status::OK();
}

/**
Status
Connection::Connect(const ConnectParam &param) {
    return Status::NotSupported("Connect interface is not supported.");
}

Status
Connection::Connect(const std::string &uri) {
    return Status::NotSupported("Connect interface is not supported.");
}

Status
Connection::Connected() const {
    return Status::NotSupported("Connected interface is not supported.");
}

Status
Connection::Disconnect() {
    return Status::NotSupported("Disconnect interface is not supported.");
}

std::string
Connection::ClientVersion() const {
    return std::string("Current Version");
}

Status
Connection::CreateTable(const TableSchema &param) {
    return Status::NotSupported("Create table interface interface is not supported.");
}

Status
Connection::CreateTablePartition(const CreateTablePartitionParam &param) {
    return Status::NotSupported("Create table partition interface is not supported.");
}

Status
Connection::DeleteTablePartition(const DeleteTablePartitionParam &param) {
    return Status::NotSupported("Delete table partition interface is not supported.");
}

Status
Connection::DeleteTable(const std::string &table_name) {
    return Status::NotSupported("Create table interface is not supported.");
}

Status
Connection::AddVector(const std::string &table_name,
                      const std::vector<RowRecord> &record_array,
                      std::vector<int64_t> &id_array) {
    return Status::NotSupported("Add vector array interface is not supported.");
}

Status
Connection::SearchVector(const std::string &table_name,
                         const std::vector<QueryRecord> &query_record_array,
                         std::vector<TopKQueryResult> &topk_query_result_array,
                         int64_t topk) {
    return Status::NotSupported("Query vector array interface is not supported.");
}

Status
Connection::DescribeTable(const std::string &table_name, TableSchema &table_schema) {
    return Status::NotSupported("Show table interface is not supported.");
}

Status
Connection::ShowTables(std::vector<std::string> &table_array) {
    return Status::NotSupported("List table array interface is not supported.");
}

std::string
Connection::ServerVersion() const {
    return std::string("Server version.");
}

std::string
Connection::ServerStatus() const {
    return std::string("Server status");
}
**/
}