// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "grpc/ClientProxy.h"

#include <memory>
#include <string>
#include <vector>

#include "grpc-gen/gen-milvus/milvus.grpc.pb.h"

//#define GRPC_MULTIPLE_THREAD;
#define MILVUS_SDK_VERSION "0.7.0";

namespace milvus {
bool
UriCheck(const std::string& uri) {
    size_t index = uri.find_first_of(':', 0);
    return (index != std::string::npos);
}

void
CopyRowRecord(::milvus::grpc::RowRecord* target, const RowRecord& src) {
    if (!src.float_data.empty()) {
        auto vector_data = target->mutable_float_data();
        vector_data->Resize(static_cast<int>(src.float_data.size()), 0.0);
        memcpy(vector_data->mutable_data(), src.float_data.data(), src.float_data.size() * sizeof(float));
    }

    if (!src.binary_data.empty()) {
        target->set_binary_data(src.binary_data.data(), src.binary_data.size());
    }
}

void ConstructPartitionStat(const ::milvus::grpc::PartitionStat& grpc_partition_stat, PartitionStat& partition_stat) {
    partition_stat.tag = grpc_partition_stat.tag();
    partition_stat.row_count = grpc_partition_stat.total_row_count();
    for (int i = 0; i < grpc_partition_stat.segments_stat_size(); i++) {
        auto& grpc_seg_stat = grpc_partition_stat.segments_stat(i);
        SegmentStat seg_stat;
        seg_stat.row_count = grpc_seg_stat.row_count();
        seg_stat.segment_name = grpc_seg_stat.segment_name();
        seg_stat.index_name = grpc_seg_stat.index_name();
        seg_stat.data_size = grpc_seg_stat.data_size();
        partition_stat.segments_stat.emplace_back(seg_stat);
    }
}

Status
ClientProxy::Connect(const ConnectParam& param) {
    std::string uri = param.ip_address + ":" + param.port;

    channel_ = ::grpc::CreateChannel(uri, ::grpc::InsecureChannelCredentials());
    if (channel_ != nullptr) {
        connected_ = true;
        client_ptr_ = std::make_shared<GrpcClient>(channel_);
        return Status::OK();
    }

    std::string reason = "connect Failed!";
    connected_ = false;
    return Status(StatusCode::NotConnected, reason);
}

Status
ClientProxy::Connect(const std::string& uri) {
    if (!UriCheck(uri)) {
        return Status(StatusCode::InvalidAgument, "Invalid uri");
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
        return client_ptr_->Cmd("", info);
    } catch (std::exception& ex) {
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
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to disconnect: " + std::string(ex.what()));
    }
}

std::string
ClientProxy::ClientVersion() const {
    return MILVUS_SDK_VERSION;
}

Status
ClientProxy::CreateTable(const TableSchema& param) {
    try {
        ::milvus::grpc::TableSchema schema;
        schema.set_table_name(param.table_name);
        schema.set_dimension(param.dimension);
        schema.set_index_file_size(param.index_file_size);
        schema.set_metric_type(static_cast<int32_t>(param.metric_type));

        return client_ptr_->CreateTable(schema);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to create table: " + std::string(ex.what()));
    }
}

bool
ClientProxy::HasTable(const std::string& table_name) {
    Status status = Status::OK();
    ::milvus::grpc::TableName grpc_table_name;
    grpc_table_name.set_table_name(table_name);
    bool result = client_ptr_->HasTable(grpc_table_name, status);
    return result;
}

Status
ClientProxy::DropTable(const std::string& table_name) {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        return client_ptr_->DropTable(grpc_table_name);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to drop table: " + std::string(ex.what()));
    }
}

Status
ClientProxy::CreateIndex(const IndexParam& index_param) {
    try {
        ::milvus::grpc::IndexParam grpc_index_param;
        grpc_index_param.set_table_name(index_param.table_name);
        grpc_index_param.mutable_index()->set_index_type(static_cast<int32_t>(index_param.index_type));
        grpc_index_param.mutable_index()->set_nlist(index_param.nlist);
        return client_ptr_->CreateIndex(grpc_index_param);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to build index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::Insert(const std::string& table_name, const std::string& partition_tag,
                    const std::vector<RowRecord>& record_array, std::vector<int64_t>& id_array) {
    Status status = Status::OK();
    try {
////////////////////////////////////////////////////////////////////////////
#ifdef GRPC_MULTIPLE_THREAD
        std::vector<std::thread> threads;
        int thread_count = 10;

        std::shared_ptr<::milvus::grpc::InsertInfos> insert_info_array(
            new ::milvus::grpc::InsertInfos[thread_count], std::default_delete<::milvus::grpc::InsertInfos[]>());

        std::shared_ptr<::milvus::grpc::VectorIds> vector_ids_array(new ::milvus::grpc::VectorIds[thread_count],
                                                                    std::default_delete<::milvus::grpc::VectorIds[]>());

        int64_t record_count = record_array.size() / thread_count;

        for (size_t i = 0; i < thread_count; i++) {
            insert_info_array.get()[i].set_table_name(table_name);
            for (size_t j = i * record_count; j < record_count * (i + 1); j++) {
                ::milvus::grpc::RowRecord* grpc_record = insert_info_array.get()[i].add_row_record_array();
                for (size_t k = 0; k < record_array[j].data.size(); k++) {
                    grpc_record->add_vector_data(record_array[j].data[k]);
                }
            }
        }

        std::cout << "*****************************************************\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t j = 0; j < thread_count; j++) {
            threads.push_back(std::thread(&GrpcClient::InsertVector, client_ptr_, std::ref(vector_ids_array.get()[j]),
                                          std::ref(insert_info_array.get()[j]), std::ref(status)));
        }
        std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
        auto finish = std::chrono::high_resolution_clock::now();
        std::cout << "InsertVector cost: "
                  << std::chrono::duration_cast<std::chrono::duration<double>>(finish - start).count() << "s\n";
        std::cout << "*****************************************************\n";

        for (size_t i = 0; i < thread_count; i++) {
            for (size_t j = 0; j < vector_ids_array.get()[i].vector_id_array_size(); j++) {
                id_array.push_back(vector_ids_array.get()[i].vector_id_array(j));
            }
        }
#else
        ::milvus::grpc::InsertParam insert_param;
        insert_param.set_table_name(table_name);
        insert_param.set_partition_tag(partition_tag);

        for (auto& record : record_array) {
            ::milvus::grpc::RowRecord* grpc_record = insert_param.add_row_record_array();
            CopyRowRecord(grpc_record, record);
        }

        // Single thread
        ::milvus::grpc::VectorIds vector_ids;
        if (!id_array.empty()) {
            /* set user's ids */
            auto row_ids = insert_param.mutable_row_id_array();
            row_ids->Resize(static_cast<int>(id_array.size()), -1);
            memcpy(row_ids->mutable_data(), id_array.data(), id_array.size() * sizeof(int64_t));
            status = client_ptr_->Insert(vector_ids, insert_param);
        } else {
            status = client_ptr_->Insert(vector_ids, insert_param);
            /* return Milvus generated ids back to user */
            id_array.insert(id_array.end(), vector_ids.vector_id_array().begin(), vector_ids.vector_id_array().end());
        }
#endif
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to add vector: " + std::string(ex.what()));
    }

    return status;
}

Status
ClientProxy::GetVectorByID(const std::string& table_name, int64_t vector_id, RowRecord& vector_data) {
    try {
        ::milvus::grpc::VectorIdentity vector_identity;
        vector_identity.set_table_name(table_name);
        vector_identity.set_id(vector_id);

        ::milvus::grpc::VectorData grpc_data;
        Status status = client_ptr_->GetVectorByID(vector_identity, grpc_data);
        if (!status.ok()) {
            return status;
        }

        int float_size = grpc_data.vector_data().float_data_size();
        if (float_size > 0) {
            vector_data.float_data.resize(float_size);
            memcpy(vector_data.float_data.data(), grpc_data.vector_data().float_data().data(),
                   float_size * sizeof(float));
        }

        auto byte_size = grpc_data.vector_data().binary_data().length();
        if (byte_size > 0) {
            vector_data.binary_data.resize(byte_size);
            memcpy(vector_data.binary_data.data(), grpc_data.vector_data().binary_data().data(), byte_size);
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to get vector by id: " + std::string(ex.what()));
    }
}

Status
ClientProxy::Search(const std::string& table_name, const std::vector<std::string>& partition_tags,
                    const std::vector<RowRecord>& query_record_array, int64_t topk, int64_t nprobe,
                    TopKQueryResult& topk_query_result) {
    try {
        // step 1: convert vectors data
        ::milvus::grpc::SearchParam search_param;
        search_param.set_table_name(table_name);
        search_param.set_topk(topk);
        search_param.set_nprobe(nprobe);
        for (auto& tag : partition_tags) {
            search_param.add_partition_tag_array(tag);
        }
        for (auto& record : query_record_array) {
            ::milvus::grpc::RowRecord* row_record = search_param.add_query_record_array();
            CopyRowRecord(row_record, record);
        }

        // step 2: search vectors
        ::milvus::grpc::TopKQueryResult result;
        Status status = client_ptr_->Search(search_param, result);
        if (result.row_num() == 0) {
            return status;
        }

        // step 3: convert result array
        topk_query_result.reserve(result.row_num());
        int64_t nq = result.row_num();
        int64_t topk = result.ids().size() / nq;
        for (int64_t i = 0; i < result.row_num(); i++) {
            milvus::QueryResult one_result;
            one_result.ids.resize(topk);
            one_result.distances.resize(topk);
            memcpy(one_result.ids.data(), result.ids().data() + topk * i, topk * sizeof(int64_t));
            memcpy(one_result.distances.data(), result.distances().data() + topk * i, topk * sizeof(float));
            topk_query_result.emplace_back(one_result);
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to search vectors: " + std::string(ex.what()));
    }
}

Status
ClientProxy::SearchByID(const std::string& table_name, const std::vector<std::string>& partition_tags, int64_t query_id,
                        int64_t topk, int64_t nprobe, TopKQueryResult& topk_query_result) {
    try {
        // step 1: convert vector id array
        ::milvus::grpc::SearchByIDParam search_param;
        search_param.set_table_name(table_name);
        search_param.set_topk(topk);
        search_param.set_nprobe(nprobe);
        for (auto& tag : partition_tags) {
            search_param.add_partition_tag_array(tag);
        }
        search_param.set_id(query_id);

        // step 2: search vectors
        ::milvus::grpc::TopKQueryResult result;
        Status status = client_ptr_->SearchByID(search_param, result);
        if (result.row_num() == 0) {
            return status;
        }

        // step 4: convert result array
        topk_query_result.reserve(result.row_num());
        int64_t nq = result.row_num();
        int64_t topk = result.ids().size() / nq;
        for (int64_t i = 0; i < result.row_num(); i++) {
            milvus::QueryResult one_result;
            one_result.ids.resize(topk);
            one_result.distances.resize(topk);
            memcpy(one_result.ids.data(), result.ids().data() + topk * i, topk * sizeof(int64_t));
            memcpy(one_result.distances.data(), result.distances().data() + topk * i, topk * sizeof(float));
            topk_query_result.emplace_back(one_result);
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to search vectors: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DescribeTable(const std::string& table_name, TableSchema& table_schema) {
    try {
        ::milvus::grpc::TableSchema grpc_schema;

        Status status = client_ptr_->DescribeTable(table_name, grpc_schema);

        table_schema.table_name = grpc_schema.table_name();
        table_schema.dimension = grpc_schema.dimension();
        table_schema.index_file_size = grpc_schema.index_file_size();
        table_schema.metric_type = static_cast<MetricType>(grpc_schema.metric_type());

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to describe table: " + std::string(ex.what()));
    }
}

Status
ClientProxy::CountTable(const std::string& table_name, int64_t& row_count) {
    try {
        Status status;
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        row_count = client_ptr_->CountTable(grpc_table_name, status);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to show tables: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowTables(std::vector<std::string>& table_array) {
    try {
        Status status;
        milvus::grpc::TableNameList table_name_list;
        status = client_ptr_->ShowTables(table_name_list);

        table_array.resize(table_name_list.table_names_size());
        for (uint64_t i = 0; i < table_name_list.table_names_size(); ++i) {
            table_array[i] = table_name_list.table_names(i);
        }
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to show tables: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowTableInfo(const std::string& table_name, TableInfo& table_info) {
    try {
        Status status;
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        milvus::grpc::TableInfo grpc_table_info;
        status = client_ptr_->ShowTableInfo(grpc_table_name, grpc_table_info);

        // get native info
        table_info.total_row_count = grpc_table_info.total_row_count();

        // get partitions info
        for (int i = 0; i < grpc_table_info.partitions_stat_size(); i++) {
            auto& grpc_partition_stat = grpc_table_info.partitions_stat(i);
            PartitionStat partition_stat;
            ConstructPartitionStat(grpc_partition_stat, partition_stat);
            table_info.partitions_stat.emplace_back(partition_stat);
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to show table info: " + std::string(ex.what()));
    }
}

std::string
ClientProxy::ServerVersion() const {
    Status status = Status::OK();
    try {
        std::string version;
        Status status = client_ptr_->Cmd("version", version);
        return version;
    } catch (std::exception& ex) {
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
        Status status = client_ptr_->Cmd("", dummy);
        return "server alive";
    } catch (std::exception& ex) {
        return "connection lost";
    }
}

std::string
ClientProxy::DumpTaskTables() const {
    if (channel_ == nullptr) {
        return "not connected to server";
    }

    try {
        std::string dummy;
        Status status = client_ptr_->Cmd("tasktable", dummy);
        return dummy;
    } catch (std::exception& ex) {
        return "connection lost";
    }
}

Status
ClientProxy::DeleteByID(const std::string& table_name, const std::vector<int64_t>& id_array) {
    try {
        ::milvus::grpc::DeleteByIDParam delete_by_id_param;
        delete_by_id_param.set_table_name(table_name);

        for (auto id : id_array) {
            delete_by_id_param.add_id_array(id);
        }

        return client_ptr_->DeleteByID(delete_by_id_param);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to delete by range: " + std::string(ex.what()));
    }
}

Status
ClientProxy::PreloadTable(const std::string& table_name) const {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        Status status = client_ptr_->PreloadTable(grpc_table_name);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to preload tables: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DescribeIndex(const std::string& table_name, IndexParam& index_param) const {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        ::milvus::grpc::IndexParam grpc_index_param;
        Status status = client_ptr_->DescribeIndex(grpc_table_name, grpc_index_param);
        index_param.index_type = static_cast<IndexType>(grpc_index_param.mutable_index()->index_type());
        index_param.nlist = grpc_index_param.mutable_index()->nlist();

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to describe index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DropIndex(const std::string& table_name) const {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        Status status = client_ptr_->DropIndex(grpc_table_name);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to drop index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::CreatePartition(const PartitionParam& partition_param) {
    try {
        ::milvus::grpc::PartitionParam grpc_partition_param;
        grpc_partition_param.set_table_name(partition_param.table_name);
        grpc_partition_param.set_tag(partition_param.partition_tag);
        Status status = client_ptr_->CreatePartition(grpc_partition_param);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to create partition: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowPartitions(const std::string& table_name, PartitionTagList& partition_array) const {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        ::milvus::grpc::PartitionList grpc_partition_list;
        Status status = client_ptr_->ShowPartitions(grpc_table_name, grpc_partition_list);
        partition_array.resize(grpc_partition_list.partition_tag_array_size());
        for (uint64_t i = 0; i < grpc_partition_list.partition_tag_array_size(); ++i) {
            partition_array[i] = grpc_partition_list.partition_tag_array(i);
        }
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to show partitions: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DropPartition(const PartitionParam& partition_param) {
    try {
        ::milvus::grpc::PartitionParam grpc_partition_param;
        grpc_partition_param.set_table_name(partition_param.table_name);
        grpc_partition_param.set_tag(partition_param.partition_tag);
        Status status = client_ptr_->DropPartition(grpc_partition_param);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to drop partition: " + std::string(ex.what()));
    }
}

Status
ClientProxy::GetConfig(const std::string& node_name, std::string& value) const {
    try {
        return client_ptr_->Cmd("get_config " + node_name, value);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to get config: " + node_name);
    }
}

Status
ClientProxy::SetConfig(const std::string& node_name, const std::string& value) const {
    try {
        std::string dummy;
        return client_ptr_->Cmd("set_config " + node_name + " " + value, dummy);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to set config: " + node_name);
    }
}

Status
ClientProxy::FlushTable(const std::string& table_name) {
    try {
        std::string dummy;
        return client_ptr_->Flush(table_name);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to flush");
    }
}

Status
ClientProxy::Flush() {
    try {
        std::string dummy;
        return client_ptr_->Flush("");
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to flush");
    }
}

Status
ClientProxy::CompactTable(const std::string& table_name) {
    try {
        ::milvus::grpc::TableName grpc_table_name;
        grpc_table_name.set_table_name(table_name);
        Status status = client_ptr_->Compact(grpc_table_name);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to compact table: " + std::string(ex.what()));
    }
}

}  // namespace milvus
