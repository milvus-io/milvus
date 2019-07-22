/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#include "ClientProxy.h"
#include "milvus.grpc.pb.h"

namespace zilliz {
namespace milvus {

Status
ClientProxy::Connect(const ConnectParam &param) {
//    Disconnect();

    std::string uri = param.ip_address + ":" + param.port;

    channel_ = ::grpc::CreateChannel(uri, ::grpc::InsecureChannelCredentials());
    client_ptr = new grpcClient(channel_);

    if (channel_ != nullptr) {
        connected_ = true;
    }
    return Status::OK();
}

Status
ClientProxy::Connect(const std::string &uri) {
//    Disconnect();

    size_t index = uri.find_first_of(":", 0);
    if ((index == std::string::npos)) {
        return Status::Invalid("Invalid uri");
    }

    ConnectParam param;
    param.ip_address = uri.substr(0, index);
    param.port = uri.substr(index + 1);

    return Connect(param);
}

Status
ClientProxy::Connected() const {
    try {
        std::string info;
        client_ptr->Ping(info, "");
    } catch (std::exception &ex) {
        return Status(StatusCode::NotConnected, "connection lost: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::Disconnect() {
    connected_ = false;
//    delete client_ptr;
    return Status::OK();
}

std::string
ClientProxy::ClientVersion() const {
    return "";
}

Status
ClientProxy::CreateTable(const TableSchema &param) {
    try {
        ::milvus::grpc::TableSchema schema;
//        ::milvus::grpc::TableName *grpc_tablename = new ::milvus::grpc::TableName;
//        grpc_tablename->set_table_name(param.table_name.table_name);
//        schema.set_allocated_table_name(grpc_tablename);

        schema.mutable_table_name()->set_table_name(param.table_name.table_name);
        schema.set_index_type((int) param.index_type);
        schema.set_dimension(param.dimension);
        schema.set_store_raw_vector(param.store_raw_vector);

        client_ptr->CreateTable(schema);//stub call
    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to create table: " + std::string(ex.what()));
    }
    return Status::OK();
}

bool
ClientProxy::HasTable(const std::string &table_name) {
    ::milvus::grpc::TableName grpc_table_name;
    grpc_table_name.set_table_name(table_name);
    return client_ptr->HasTable(grpc_table_name);
}

Status
ClientProxy::DropTable(const std::string &table_name) {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        client_ptr->DropTable(grpc_table_name);

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to drop table: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::BuildIndex(const std::string &table_name) {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        client_ptr->BuildIndex(grpc_table_name);

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to build index: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::InsertVector(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    try {
//        ::milvus::grpc::InsertInfos insert_infos;
//        insert_infos.set_table_name(table_name);
//
//        for (auto &record : record_array) {
//            ::milvus::grpc::RowRecord *grpc_record = insert_infos.add_row_record_array();
//            for (size_t i = 0; i < record.data.size(); i++) {
//                grpc_record->add_vector_data(record.data[i]);
//            }
//        }
//
//        ::milvus::grpc::VectorIds vector_ids;
//
//        //Single thread
//        client_ptr->InsertVector(vector_ids, insert_infos);
//        auto finish = std::chrono::high_resolution_clock::now();
//
//        for (size_t i = 0; i < vector_ids.vector_id_array_size(); i++) {
//            id_array.push_back(vector_ids.vector_id_array(i));
//        }

////////////////////////////////////////////////////////////////////////////
        //multithread
        std::vector<std::thread> threads;
//      int thread_count = std::thread::hardware_concurrency();
        int thread_count = 10;

        ::milvus::grpc::InsertInfos *insert_info_array = new ::milvus::grpc::InsertInfos[thread_count];
        ::milvus::grpc::VectorIds *vector_ids_array = new ::milvus::grpc::VectorIds[thread_count];
        int64_t record_count = record_array.size() / thread_count;

        for (size_t i = 0; i < thread_count; i++) {
            insert_info_array[i].set_table_name(table_name);
            for (size_t j = i * record_count; j < record_count * (i + 1); j++) {
                ::milvus::grpc::RowRecord *grpc_record = insert_info_array[i].add_row_record_array();
                for (size_t k = 0; k < record_array[j].data.size(); k++) {
                    grpc_record->add_vector_data(record_array[j].data[k]);
                }
            }
        }

        std::cout << "*****************************************************\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j = 0; j < thread_count; j++) {
            threads.push_back(
                    std::thread(&grpcClient::InsertVector, client_ptr, std::ref(vector_ids_array[j]), insert_info_array[j]));
        }
        std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
        auto finish = std::chrono::high_resolution_clock::now();
        std::cout << "InsertVector cost: " << std::chrono::duration_cast<std::chrono::duration<double>>(finish - start).count() << "s\n";
        std::cout << "*****************************************************\n";

        for (size_t i = 0; i < thread_count; i++) {
            for (size_t j = 0; j < vector_ids_array[i].vector_id_array_size(); j++) {
                id_array.push_back(vector_ids_array[i].vector_id_array(j));
            }
        }

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to add vector: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::SearchVector(const std::string &table_name,
                          const std::vector<RowRecord> &query_record_array,
                          const std::vector<Range> &query_range_array,
                          int64_t topk,
                          std::vector<TopKQueryResult> &topk_query_result_array) {
    try {
        //step 1: convert vectors data
        ::milvus::grpc::SearchVectorInfos search_vector_infos;
        search_vector_infos.set_table_name(table_name);
        search_vector_infos.set_topk(topk);
        for (auto &record : query_record_array) {
            ::milvus::grpc::RowRecord *row_record = search_vector_infos.add_query_record_array();
            for (size_t i = 0; i < record.data.size(); i++) {
                row_record->add_vector_data(record.data[i]);
            }
        }

        //step 2: convert range array
        for (auto &range : query_range_array) {
            ::milvus::grpc::Range *grpc_range = search_vector_infos.add_query_range_array();
            grpc_range->set_start_value(range.start_value);
            grpc_range->set_end_value(range.end_value);
        }

        //step 3: search vectors
        std::vector<::milvus::grpc::TopKQueryResult> result_array;
        client_ptr->SearchVector(result_array, search_vector_infos);

        //step 4: convert result array
        for (auto &grpc_topk_result : result_array) {
            TopKQueryResult result;
            for (size_t i = 0; i < grpc_topk_result.query_result_arrays_size(); i++) {
                QueryResult query_result;
                query_result.id = grpc_topk_result.query_result_arrays(i).id();
                query_result.distance = grpc_topk_result.query_result_arrays(i).distance();
                result.query_result_arrays.emplace_back(query_result);
            }

            topk_query_result_array.emplace_back(result);
        }

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to search vectors: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::DescribeTable(const std::string &table_name, TableSchema &table_schema) {
    try {
        ::milvus::grpc::TableSchema grpc_schema;

        client_ptr->DescribeTable(grpc_schema, table_name);

        table_schema.table_name.table_name = grpc_schema.table_name().table_name();
        table_schema.index_type = (IndexType) grpc_schema.index_type();
        table_schema.dimension = grpc_schema.dimension();
        table_schema.store_raw_vector = grpc_schema.store_raw_vector();
    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to describe table: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::GetTableRowCount(const std::string &table_name, int64_t &row_count) {
    try {
        row_count = client_ptr->GetTableRowCount(table_name);

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to show tables: " + std::string(ex.what()));
    }

    return Status::OK();
}

Status
ClientProxy::ShowTables(std::vector<std::string> &table_array) {
    try {
        client_ptr->ShowTables(table_array);

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to show tables: " + std::string(ex.what()));
    }

    return Status::OK();
}

std::string
ClientProxy::ServerVersion() const {
    try {
        std::string version;
        client_ptr->Ping(version, "version");
        return version;
    } catch (std::exception &ex) {
        return "";
    }
}

std::string
ClientProxy::ServerStatus() const {
    if (connected_ == false) {
        return "not connected to server";
    }

    try {
        std::string dummy;
        client_ptr->Ping(dummy, "");
        return "server alive";
    } catch (std::exception &ex) {
        return "connection lost";
    }
}

}
}
