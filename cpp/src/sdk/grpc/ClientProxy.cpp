/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#include "ClientProxy.h"
#include "version.h"
#include "milvus.grpc.pb.h"
//#define GRPC_MULTIPLE_THREAD;

namespace milvus {

bool
UriCheck(const std::string &uri) {
     size_t index = uri.find_first_of(':', 0);
    if (index == std::string::npos) {
        return false;
    } else {
        return true;
    }
}

Status
ClientProxy::Connect(const ConnectParam &param) {
    std::string uri = param.ip_address + ":" + param.port;

    channel_ = ::grpc::CreateChannel(uri, ::grpc::InsecureChannelCredentials());
    if (channel_ != nullptr) {
        connected_ = true;
        client_ptr_ = std::make_shared<GrpcClient>(channel_);
        return Status::OK();
    } else {
        std::string reason = "connect failed!";
        connected_ = false;
        return Status(StatusCode::NotConnected, reason);
    }
}

Status
ClientProxy::Connect(const std::string &uri) {
    if (!UriCheck(uri)) {
        return Status::Invalid("Invalid uri");
    }
    size_t index = uri.find_first_of(':', 0);

    ConnectParam param;
    param.ip_address = uri.substr(0, index);
    param.port = uri.substr(index + 1);

    return Connect(param);
}

Status
ClientProxy::Connected() const {
    try {
        std::string info;
        return client_ptr_->Ping(info, "");
    } catch (std::exception &ex) {
        return Status(StatusCode::NotConnected, "connection lost: " + std::string(ex.what()));
    }
}

Status
ClientProxy::Disconnect() {
    try {
        Status status = client_ptr_->Disconnect();
        connected_ = false;
        channel_.reset();
        return status;
    }catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to disconnect: " + std::string(ex.what()));
    }
}

std::string
ClientProxy::ClientVersion() const {
    return MILVUS_VERSION;
}

Status
ClientProxy::CreateTable(const TableSchema &param) {
    try {
        ::milvus::grpc::TableSchema schema;
        schema.mutable_table_name()->set_table_name(param.table_name);
        schema.set_index_type((int) param.index_type);
        schema.set_dimension(param.dimension);
        schema.set_store_raw_vector(param.store_raw_vector);

        return client_ptr_->CreateTable(schema);
    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to create table: " + std::string(ex.what()));
    }
}

bool
ClientProxy::HasTable(const std::string &table_name) {
    Status status = Status::OK();
    ::milvus::grpc::TableName grpc_table_name;
    grpc_table_name.set_table_name(table_name);
    bool result = client_ptr_->HasTable(grpc_table_name, status);
    return result;
}

Status
ClientProxy::DeleteTable(const std::string &table_name) {
    return this->DropTable(table_name);
}

Status
ClientProxy::DropTable(const std::string &table_name) {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        return client_ptr_->DropTable(grpc_table_name);
    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to drop table: " + std::string(ex.what()));
    }
}

Status
ClientProxy::BuildIndex(const std::string &table_name) {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        return client_ptr_->BuildIndex(grpc_table_name);

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "failed to build index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::AddVector(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    return InsertVector(table_name, record_array, id_array);
}

Status
ClientProxy::InsertVector(const std::string &table_name,
                          const std::vector<RowRecord> &record_array,
                          std::vector<int64_t> &id_array) {
    Status status = Status::OK();
    try {
////////////////////////////////////////////////////////////////////////////
#ifdef GRPC_MULTIPLE_THREAD
        std::vector<std::thread> threads;
        int thread_count = 10;

        std::shared_ptr<::milvus::grpc::InsertInfos> insert_info_array(
            new ::milvus::grpc::InsertInfos[thread_count],
            std::default_delete<::milvus::grpc::InsertInfos[]>() );

        std::shared_ptr<::milvus::grpc::VectorIds> vector_ids_array(
            new ::milvus::grpc::VectorIds[thread_count],
                std::default_delete<::milvus::grpc::VectorIds[]>() );

        int64_t record_count = record_array.size() / thread_count;

        for (size_t i = 0; i < thread_count; i++) {
            insert_info_array.get()[i].set_table_name(table_name);
            for (size_t j = i * record_count; j < record_count * (i + 1); j++) {
                ::milvus::grpc::RowRecord *grpc_record =
                    insert_info_array.get()[i].add_row_record_array();
                for (size_t k = 0; k < record_array[j].data.size(); k++) {
                    grpc_record->add_vector_data(record_array[j].data[k]);
                }
            }
        }

        std::cout << "*****************************************************\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j = 0; j < thread_count; j++) {
            threads.push_back(
                    std::thread(&GrpcClient::InsertVector, client_ptr_,
                        std::ref(vector_ids_array.get()[j]), std::ref(insert_info_array.get()[j]),
                        std::ref(status)));
        }
        std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
        auto finish = std::chrono::high_resolution_clock::now();
        std::cout << "InsertVector cost: " << std::chrono::duration_cast<std::chrono::duration<double>>(finish - start).count() << "s\n";
        std::cout << "*****************************************************\n";

        for (size_t i = 0; i < thread_count; i++) {
            for (size_t j = 0; j < vector_ids_array.get()[i].vector_id_array_size(); j++) {
                id_array.push_back(vector_ids_array.get()[i].vector_id_array(j));
            }
        }
#else
        ::milvus::grpc::InsertInfos insert_infos;
        insert_infos.set_table_name(table_name);

        for (auto &record : record_array) {
            ::milvus::grpc::RowRecord *grpc_record = insert_infos.add_row_record_array();
            for (size_t i = 0; i < record.data.size(); i++) {
                grpc_record->add_vector_data(record.data[i]);
            }
        }

        ::milvus::grpc::VectorIds vector_ids;

        //Single thread
        client_ptr_->InsertVector(vector_ids, insert_infos, status);
        auto finish = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < vector_ids.vector_id_array_size(); i++) {
            id_array.push_back(vector_ids.vector_id_array(i));
        }
#endif

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "fail to add vector: " + std::string(ex.what()));
    }

    return status;
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
            for (auto &rec : record.data) {
                row_record->add_vector_data(rec);
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
        Status status = client_ptr_->SearchVector(result_array, search_vector_infos);

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
        return status;

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "fail to search vectors: " + std::string(ex.what()));
    }

}

Status
ClientProxy::DescribeTable(const std::string &table_name, TableSchema &table_schema) {
    try {
        ::milvus::grpc::TableSchema grpc_schema;

        Status status = client_ptr_->DescribeTable(grpc_schema, table_name);

        table_schema.table_name = grpc_schema.table_name().table_name();
        table_schema.index_type = (IndexType) grpc_schema.index_type();
        table_schema.dimension = grpc_schema.dimension();
        table_schema.store_raw_vector = grpc_schema.store_raw_vector();

        return status;
    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "fail to describe table: " + std::string(ex.what()));
    }

}

Status
ClientProxy::GetTableRowCount(const std::string &table_name, int64_t &row_count) {
    try {
        Status status;
        row_count = client_ptr_->GetTableRowCount(table_name, status);
        return status;
    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "fail to show tables: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowTables(std::vector<std::string> &table_array) {
    try {
        return client_ptr_->ShowTables(table_array);

    } catch (std::exception &ex) {
        return Status(StatusCode::UnknownError, "fail to show tables: " + std::string(ex.what()));
    }
}

std::string
ClientProxy::ServerVersion() const {
    Status status = Status::OK();
    try {
        std::string version;
        Status status = client_ptr_->Ping(version, "version");
        return version;
    } catch (std::exception &ex) {
        return "";
    }
}

std::string
ClientProxy::ServerStatus() const {
    if (channel_ == nullptr) {
        return "not connected to server";
    }

    try {
        std::string dummy;
        Status status = client_ptr_->Ping(dummy, "");
        return "server alive";
    } catch (std::exception &ex) {
        return "connection lost";
    }
}

}
