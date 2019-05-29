/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ClientProxy.h"
#include "util/ConvertUtil.h"

namespace megasearch {

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
    return std::string("v1.0");
}

Status
ClientProxy::CreateTable(const TableSchema &param) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        thrift::TableSchema schema;
        schema.__set_table_name(param.table_name);

        std::vector<thrift::VectorColumn>  vector_column_array;
        for(auto& column : param.vector_column_array) {
            thrift::VectorColumn col;
            col.__set_dimension(column.dimension);
            col.__set_index_type(ConvertUtil::IndexType2Str(column.index_type));
            col.__set_store_raw_vector(column.store_raw_vector);
            vector_column_array.emplace_back(col);
        }
        schema.__set_vector_column_array(vector_column_array);

        std::vector<thrift::Column>  attribute_column_array;
        for(auto& column : param.attribute_column_array) {
            thrift::Column col;
            col.__set_name(col.name);
            col.__set_type(col.type);
            attribute_column_array.emplace_back(col);
        }
        schema.__set_attribute_column_array(attribute_column_array);

        schema.__set_partition_column_name_array(param.partition_column_name_array);

        ClientPtr()->interface()->CreateTable(schema);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to create table: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::CreateTablePartition(const CreateTablePartitionParam &param) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        thrift::CreateTablePartitionParam partition_param;
        partition_param.__set_table_name(param.table_name);
        partition_param.__set_partition_name(param.partition_name);

        std::map<std::string, thrift::Range> range_map;
        for(auto& pair : param.range_map) {
            thrift::Range range;
            range.__set_start_value(pair.second.start_value);
            range.__set_end_value(pair.second.end_value);
            range_map.insert(std::make_pair(pair.first, range));
        }
        partition_param.__set_range_map(range_map);

        ClientPtr()->interface()->CreateTablePartition(partition_param);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to create table partition: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::DeleteTablePartition(const DeleteTablePartitionParam &param) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        thrift::DeleteTablePartitionParam partition_param;
        partition_param.__set_table_name(param.table_name);
        partition_param.__set_partition_name_array(param.partition_name_array);

        ClientPtr()->interface()->DeleteTablePartition(partition_param);

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to delete table partition: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::DeleteTable(const std::string &table_name) {
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
ClientProxy::AddVector(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        std::vector<thrift::RowRecord> thrift_records;
        for(auto& record : record_array) {
            thrift::RowRecord thrift_record;
            thrift_record.__set_attribute_map(record.attribute_map);

            for(auto& pair : record.vector_map) {
                size_t dim = pair.second.size();
                std::string& thrift_vector = thrift_record.vector_map[pair.first];
                thrift_vector.resize(dim * sizeof(double));
                double *dbl = (double *) (const_cast<char *>(thrift_vector.data()));
                for (size_t i = 0; i < dim; i++) {
                    dbl[i] = (double) (pair.second[i]);
                }
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
ClientProxy::SearchVector(const std::string &table_name,
                             const std::vector<QueryRecord> &query_record_array,
                             std::vector<TopKQueryResult> &topk_query_result_array,
                             int64_t topk) {
    if(!IsConnected()) {
        return Status(StatusCode::NotConnected, "not connected to server");
    }

    try {
        std::vector<thrift::QueryRecord> thrift_records;
        for(auto& record : query_record_array) {
            thrift::QueryRecord thrift_record;
            thrift_record.__set_selected_column_array(record.selected_column_array);

            for(auto& pair : record.vector_map) {
                size_t dim = pair.second.size();
                std::string& thrift_vector = thrift_record.vector_map[pair.first];
                thrift_vector.resize(dim * sizeof(double));
                double *dbl = (double *) (const_cast<char *>(thrift_vector.data()));
                for (size_t i = 0; i < dim; i++) {
                    dbl[i] = (double) (pair.second[i]);
                }
            }
            thrift_records.emplace_back(thrift_record);
        }

        std::vector<thrift::TopKQueryResult> result_array;
        ClientPtr()->interface()->SearchVector(result_array, table_name, thrift_records, topk);

        for(auto& thrift_topk_result : result_array) {
            TopKQueryResult result;

            for(auto& thrift_query_result : thrift_topk_result.query_result_arrays) {
                QueryResult query_result;
                query_result.id = thrift_query_result.id;
                query_result.column_map = thrift_query_result.column_map;
                query_result.score = thrift_query_result.score;
                result.query_result_arrays.emplace_back(query_result);
            }

            topk_query_result_array.emplace_back(result);
        }

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to create table partition: " + std::string(ex.what()));
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
        table_schema.partition_column_name_array = thrift_schema.partition_column_name_array;

        for(auto& thrift_col : thrift_schema.attribute_column_array) {
            Column col;
            col.name = col.name;
            col.type = col.type;
            table_schema.attribute_column_array.emplace_back(col);
        }

        for(auto& thrift_col : thrift_schema.vector_column_array) {
            VectorColumn col;
            col.store_raw_vector = thrift_col.store_raw_vector;
            col.index_type = ConvertUtil::Str2IndexType(thrift_col.index_type);
            col.dimension = thrift_col.dimension;
            col.name = thrift_col.base.name;
            col.type = (ColumnType)thrift_col.base.type;
            table_schema.vector_column_array.emplace_back(col);
        }

    }  catch ( std::exception& ex) {
        return Status(StatusCode::UnknownError, "failed to describe table: " + std::string(ex.what()));
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
    
}
