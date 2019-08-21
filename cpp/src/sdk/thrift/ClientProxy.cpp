/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientProxy.h"

namespace milvus {

std::shared_ptr<ThriftClient>&
ClientProxy::ClientPtr() const {
    if(client_ptr == nullptr) {
        client_ptr = std::make_shared<ThriftClient>();
    }
    return client_ptr;
}

bool ClientProxy::IsConnected() const {
    return (client_ptr != nullptr && connected_);
}

Status
ClientProxy::Connect(const ConnectParam &param) {
    Disconnect();

    int32_t port = atoi(param.port.c_str());
    Status status = ClientPtr()->Connect(param.ip_address, port, THRIFT_PROTOCOL_BINARY);
    if(status.ok()) {
        connected_ = true;
    }

    return status;
}

Status
ClientProxy::Connect(const std::string &uri) {
    Disconnect();

    size_t index = uri.find_first_of(":", 0);
    if((index == std::string::npos)) {
        return Status::Invalid("Invalid uri");
    }

    ConnectParam param;
    param.ip_address = uri.substr(0, index);
    param.port = uri.substr(index + 1);

    return Connect(param);
}

Status
ClientProxy::Connected() const {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        std::string info;
        ClientPtr()->interface()->Ping(info, "");
    }  catch ( std::exception& ex) {
        return Status(StatusCode::NotConnected, "connection lost: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::Disconnect() {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    connected_ = false;
    return ClientPtr()->Disconnect();
}

std::string
ClientProxy::ClientVersion() const {
    return "";
}

Status
ClientProxy::CreateTable(const TableSchema &param) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {

        thrift::TableSchema schema;
        schema.__set_table_name(param.table_name);
        schema.__set_index_type((int)param.index_type);
        schema.__set_dimension(param.dimension);
        schema.__set_store_raw_vector(param.store_raw_vector);

        ClientPtr()->interface()->CreateTable(schema);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to create table: " + std::string(ex.what()));
    }

    return Status::OK();
}

bool
ClientProxy::HasTable(const std::string &table_name) {
    if(!IsConnected()) {
        return false;
    }

    return ClientPtr()->interface()->HasTable(table_name);
}

Status
ClientProxy::DropTable(const std::string &table_name) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        ClientPtr()->interface()->DeleteTable(table_name);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to delete table: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::CreateIndex(const IndexParam &index_param) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        ClientPtr()->interface()->BuildIndex(index_param.table_name);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to build index: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::Insert(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        std::vector<thrift::RowRecord> thrift_records;
        for(auto& record : record_array) {
            thrift::RowRecord thrift_record;

            thrift_record.vector_data.resize(record.data.size() * sizeof(double));
            double *dbl = (double *) (const_cast<char *>(thrift_record.vector_data.data()));
            for (size_t i = 0; i < record.data.size(); i++) {
                dbl[i] = (double) (record.data[i]);
            }

            thrift_records.emplace_back(thrift_record);
        }
        ClientPtr()->interface()->AddVector(id_array, table_name, thrift_records);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to add vector: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::Search(const std::string &table_name,
                          const std::vector<RowRecord> &query_record_array,
                          const std::vector<Range> &query_range_array,
                          int64_t topk,
                          std::vector<TopKQueryResult> &topk_query_result_array) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {

        //step 1: convert vectors data
        std::vector<thrift::RowRecord> thrift_records;
        for(auto& record : query_record_array) {
            thrift::RowRecord thrift_record;

            thrift_record.vector_data.resize(record.data.size() * sizeof(double));
            auto dbl = (double *) (const_cast<char *>(thrift_record.vector_data.data()));
            for (size_t i = 0; i < record.data.size(); i++) {
                dbl[i] = (double) (record.data[i]);
            }

            thrift_records.emplace_back(thrift_record);
        }

        //step 2: convert range array
        std::vector<thrift::Range> thrift_ranges;
        for(auto& range : query_range_array) {
            thrift::Range thrift_range;
            thrift_range.__set_start_value(range.start_value);
            thrift_range.__set_end_value(range.end_value);

            thrift_ranges.emplace_back(thrift_range);
        }

        //step 3: search vectors
        std::vector<thrift::TopKQueryBinResult> result_array;
        ClientPtr()->interface()->SearchVector2(result_array, table_name, thrift_records, thrift_ranges, topk);

        //step 4: convert result array
        for(auto& thrift_topk_result : result_array) {
            TopKQueryResult result;

            size_t id_count = thrift_topk_result.id_array.size()/sizeof(int64_t);
            size_t dist_count = thrift_topk_result.distance_array.size()/ sizeof(double);
            if(id_count != dist_count) {
                return Status(StatusCode::UnknownError, "illegal result");
            }

            auto id_ptr = (int64_t*)thrift_topk_result.id_array.data();
            auto dist_ptr = (double*)thrift_topk_result.distance_array.data();
            for(size_t i = 0; i < id_count; i++) {
                QueryResult query_result;
                query_result.id = id_ptr[i];
                query_result.distance = dist_ptr[i];
                result.query_result_arrays.emplace_back(query_result);
            }

            topk_query_result_array.emplace_back(result);
        }

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to search vectors: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::DescribeTable(const std::string &table_name, TableSchema &table_schema) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        thrift::TableSchema thrift_schema;
        ClientPtr()->interface()->DescribeTable(thrift_schema, table_name);

        table_schema.table_name = thrift_schema.table_name;
        table_schema.index_type = (IndexType)thrift_schema.index_type;
        table_schema.dimension = thrift_schema.dimension;
        table_schema.store_raw_vector = thrift_schema.store_raw_vector;

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to describe table: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::CountTable(const std::string &table_name, int64_t &row_count) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        row_count = ClientPtr()->interface()->GetTableRowCount(table_name);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to show tables: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::ShowTables(std::vector<std::string> &table_array) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        ClientPtr()->interface()->ShowTables(table_array);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to show tables: " + std::string(ex.what()));
    }

    return Status::OK();
}

std::string
ClientProxy::ServerVersion() const {
    if(!IsConnected()) {
        return "";
    }

    try {
        std::string version;
        ClientPtr()->interface()->Ping(version, "version");
        return version;
    }  catch ( std::exception& ex) {
        return "";
    }
}

std::string
ClientProxy::ServerStatus() const {
    if(!IsConnected()) {
        return "not connected to server";
    }

    try {
        std::string dummy;
        ClientPtr()->interface()->Ping(dummy, "");
        return "server alive";
    }  catch ( std::exception& ex) {
        return "connection lost";
    }
}

Status ClientProxy::DeleteByRange(Range &range, const std::string &table_name) {
    return Status::OK();
}

Status ClientProxy::PreloadTable(const std::string &table_name) const {
    return Status::OK();
}

Status ClientProxy::DescribeIndex(const std::string &table_name, IndexParam &index_param) const {
    index_param.table_name = table_name;
    return index_param;
}

Status ClientProxy::DropIndex(const std::string &table_name) const {
    return Status::OK();
}
    
}
