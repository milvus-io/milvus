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

#define MILVUS_SDK_VERSION "0.8.0";

namespace milvus {

static const char* EXTRA_PARAM_KEY = "params";

bool
UriCheck(const std::string& uri) {
    size_t index = uri.find_first_of(':', 0);
    return (index != std::string::npos);
}

template<typename T>
void
ConstructSearchParam(const std::string& collection_name,
                     const std::vector<std::string>& partition_tag_array,
                     int64_t topk,
                     const std::string& extra_params,
                     T& search_param) {
    search_param.set_collection_name(collection_name);
    search_param.set_topk(topk);
    milvus::grpc::KeyValuePair* kv = search_param.add_extra_params();
    kv->set_key(EXTRA_PARAM_KEY);
    kv->set_value(extra_params);

    for (auto& tag : partition_tag_array) {
        search_param.add_partition_tag_array(tag);
    }
}

void
CopyRowRecord(::milvus::grpc::RowRecord* target, const Entity& src) {
    if (!src.float_data.empty()) {
        auto vector_data = target->mutable_float_data();
        vector_data->Resize(static_cast<int>(src.float_data.size()), 0.0);
        memcpy(vector_data->mutable_data(), src.float_data.data(), src.float_data.size() * sizeof(float));
    }

    if (!src.binary_data.empty()) {
        target->set_binary_data(src.binary_data.data(), src.binary_data.size());
    }
}

void
ConstructPartitionStat(const ::milvus::grpc::PartitionStat& grpc_partition_stat, PartitionStat& partition_stat) {
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

    ::grpc::ChannelArguments args;
    args.SetMaxSendMessageSize(-1);
    args.SetMaxReceiveMessageSize(-1);
    channel_ = ::grpc::CreateCustomChannel(uri, ::grpc::InsecureChannelCredentials(), args);
    if (channel_ != nullptr) {
        connected_ = true;
        client_ptr_ = std::make_shared<GrpcClient>(channel_);
        return Status::OK();
    }

    std::string reason = "Connect failed!";
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
        return Status(StatusCode::NotConnected, "Connection lost: " + std::string(ex.what()));
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
ClientProxy::CreateCollection(const CollectionParam& param) {
    try {
        ::milvus::grpc::CollectionSchema schema;
        schema.set_collection_name(param.collection_name);
        schema.set_dimension(param.dimension);
        schema.set_index_file_size(param.index_file_size);
        schema.set_metric_type(static_cast<int32_t>(param.metric_type));

        return client_ptr_->CreateCollection(schema);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to create collection: " + std::string(ex.what()));
    }
}

bool
ClientProxy::HasCollection(const std::string& collection_name) {
    Status status = Status::OK();
    ::milvus::grpc::CollectionName grpc_collection_name;
    grpc_collection_name.set_collection_name(collection_name);
    bool result = client_ptr_->HasCollection(grpc_collection_name, status);
    return result;
}

Status
ClientProxy::DropCollection(const std::string& collection_name) {
    try {
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        return client_ptr_->DropCollection(grpc_collection_name);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to drop collection: " + std::string(ex.what()));
    }
}

Status
ClientProxy::CreateIndex(const IndexParam& index_param) {
    try {
        ::milvus::grpc::IndexParam grpc_index_param;
        grpc_index_param.set_collection_name(index_param.collection_name);
        grpc_index_param.set_index_type(static_cast<int32_t>(index_param.index_type));
        milvus::grpc::KeyValuePair* kv = grpc_index_param.add_extra_params();
        kv->set_key(EXTRA_PARAM_KEY);
        kv->set_value(index_param.extra_params);
        return client_ptr_->CreateIndex(grpc_index_param);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to build index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::Insert(const std::string& collection_name, const std::string& partition_tag,
                    const std::vector<Entity>& entity_array, std::vector<int64_t>& id_array) {
    Status status = Status::OK();
    try {
        ::milvus::grpc::InsertParam insert_param;
        insert_param.set_collection_name(collection_name);
        insert_param.set_partition_tag(partition_tag);

        for (auto& entity : entity_array) {
            ::milvus::grpc::RowRecord* grpc_record = insert_param.add_row_record_array();
            CopyRowRecord(grpc_record, entity);
        }

        // Single thread
        ::milvus::grpc::VectorIds vector_ids;
        if (!id_array.empty()) {
            /* set user's ids */
            auto row_ids = insert_param.mutable_row_id_array();
            row_ids->Resize(static_cast<int>(id_array.size()), -1);
            memcpy(row_ids->mutable_data(), id_array.data(), id_array.size() * sizeof(int64_t));
            status = client_ptr_->Insert(insert_param, vector_ids);
        } else {
            status = client_ptr_->Insert(insert_param, vector_ids);
            /* return Milvus generated ids back to user */
            id_array.insert(id_array.end(), vector_ids.vector_id_array().begin(), vector_ids.vector_id_array().end());
        }
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to add entities: " + std::string(ex.what()));
    }

    return status;
}

Status
ClientProxy::GetEntityByID(const std::string& collection_name, int64_t entity_id, Entity& entity_data) {
    try {
        ::milvus::grpc::VectorIdentity vector_identity;
        vector_identity.set_collection_name(collection_name);
        vector_identity.set_id(entity_id);

        ::milvus::grpc::VectorData grpc_data;
        Status status = client_ptr_->GetVectorByID(vector_identity, grpc_data);
        if (!status.ok()) {
            return status;
        }

        int float_size = grpc_data.vector_data().float_data_size();
        if (float_size > 0) {
            entity_data.float_data.resize(float_size);
            memcpy(entity_data.float_data.data(), grpc_data.vector_data().float_data().data(),
                   float_size * sizeof(float));
        }

        auto byte_size = grpc_data.vector_data().binary_data().length();
        if (byte_size > 0) {
            entity_data.binary_data.resize(byte_size);
            memcpy(entity_data.binary_data.data(), grpc_data.vector_data().binary_data().data(), byte_size);
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to get entity by id: " + std::string(ex.what()));
    }
}

Status
ClientProxy::GetIDsInSegment(const std::string& collection_name, const std::string& segment_name,
                             std::vector<int64_t>& id_array) {
    try {
        ::milvus::grpc::GetVectorIDsParam param;
        param.set_collection_name(collection_name);
        param.set_segment_name(segment_name);

        ::milvus::grpc::VectorIds vector_ids;
        Status status = client_ptr_->GetIDsInSegment(param, vector_ids);
        if (!status.ok()) {
            return status;
        }

        id_array.insert(id_array.end(), vector_ids.vector_id_array().begin(), vector_ids.vector_id_array().end());

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to get ids from segment: " + std::string(ex.what()));
    }
}

Status
ClientProxy::Search(const std::string& collection_name, const std::vector<std::string>& partition_tag_array,
                    const std::vector<Entity>& entity_array, int64_t topk, const std::string& extra_params,
                    TopKQueryResult& topk_query_result) {
    try {
        // step 1: convert vectors data
        ::milvus::grpc::SearchParam search_param;
        ConstructSearchParam(collection_name,
                             partition_tag_array,
                             topk,
                             extra_params,
                             search_param);

        for (auto& entity : entity_array) {
            ::milvus::grpc::RowRecord* row_record = search_param.add_query_record_array();
            CopyRowRecord(row_record, entity);
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
        return Status(StatusCode::UnknownError, "Failed to search entities: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DescribeCollection(const std::string& collection_name, CollectionParam& collection_param) {
    try {
        ::milvus::grpc::CollectionSchema grpc_schema;

        Status status = client_ptr_->DescribeCollection(collection_name, grpc_schema);

        collection_param.collection_name = grpc_schema.collection_name();
        collection_param.dimension = grpc_schema.dimension();
        collection_param.index_file_size = grpc_schema.index_file_size();
        collection_param.metric_type = static_cast<MetricType>(grpc_schema.metric_type());

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to describe collection: " + std::string(ex.what()));
    }
}

Status
ClientProxy::CountCollection(const std::string& collection_name, int64_t& row_count) {
    try {
        Status status;
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        row_count = client_ptr_->CountCollection(grpc_collection_name, status);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to count collection: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowCollections(std::vector<std::string>& collection_array) {
    try {
        Status status;
        milvus::grpc::CollectionNameList collection_name_list;
        status = client_ptr_->ShowCollections(collection_name_list);

        collection_array.resize(collection_name_list.collection_names_size());
        for (uint64_t i = 0; i < collection_name_list.collection_names_size(); ++i) {
            collection_array[i] = collection_name_list.collection_names(i);
        }
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to show collections: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowCollectionInfo(const std::string& collection_name, CollectionInfo& collection_info) {
    try {
        Status status;
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        milvus::grpc::CollectionInfo grpc_collection_info;
        status = client_ptr_->ShowCollectionInfo(grpc_collection_name, grpc_collection_info);

        // get native info
        collection_info.total_row_count = grpc_collection_info.total_row_count();

        // get partitions info
        for (int i = 0; i < grpc_collection_info.partitions_stat_size(); i++) {
            auto& grpc_partition_stat = grpc_collection_info.partitions_stat(i);
            PartitionStat partition_stat;
            ConstructPartitionStat(grpc_partition_stat, partition_stat);
            collection_info.partitions_stat.emplace_back(partition_stat);
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to show collection info: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DeleteByID(const std::string& collection_name,  const std::vector<int64_t>& id_array) {
    try {
        ::milvus::grpc::DeleteByIDParam delete_by_id_param;
        delete_by_id_param.set_collection_name(collection_name);
        for (auto id : id_array) {
            delete_by_id_param.add_id_array(id);
        }

        return client_ptr_->DeleteByID(delete_by_id_param);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to delete entity id: " + std::string(ex.what()));
    }
}

Status
ClientProxy::PreloadCollection(const std::string& collection_name) const {
    try {
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        Status status = client_ptr_->PreloadCollection(grpc_collection_name);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to preload collection: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DescribeIndex(const std::string& collection_name, IndexParam& index_param) const {
    try {
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);

        ::milvus::grpc::IndexParam grpc_index_param;
        Status status = client_ptr_->DescribeIndex(grpc_collection_name, grpc_index_param);
        index_param.index_type = static_cast<IndexType>(grpc_index_param.index_type());

        for (int i = 0; i < grpc_index_param.extra_params_size(); i++) {
            const milvus::grpc::KeyValuePair& kv = grpc_index_param.extra_params(i);
            if (kv.key() == EXTRA_PARAM_KEY) {
                index_param.extra_params = kv.value();
            }
        }

        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to describe index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::DropIndex(const std::string& collection_name) const {
    try {
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        Status status = client_ptr_->DropIndex(grpc_collection_name);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to drop index: " + std::string(ex.what()));
    }
}

Status
ClientProxy::CreatePartition(const PartitionParam& partition_param) {
    try {
        ::milvus::grpc::PartitionParam grpc_partition_param;
        grpc_partition_param.set_collection_name(partition_param.collection_name);
        grpc_partition_param.set_tag(partition_param.partition_tag);
        Status status = client_ptr_->CreatePartition(grpc_partition_param);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to create partition: " + std::string(ex.what()));
    }
}

Status
ClientProxy::ShowPartitions(const std::string& collection_name, PartitionTagList& partition_tag_array) const {
    try {
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        ::milvus::grpc::PartitionList grpc_partition_list;
        Status status = client_ptr_->ShowPartitions(grpc_collection_name, grpc_partition_list);
        partition_tag_array.resize(grpc_partition_list.partition_tag_array_size());
        for (uint64_t i = 0; i < grpc_partition_list.partition_tag_array_size(); ++i) {
            partition_tag_array[i] = grpc_partition_list.partition_tag_array(i);
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
        grpc_partition_param.set_collection_name(partition_param.collection_name);
        grpc_partition_param.set_tag(partition_param.partition_tag);
        Status status = client_ptr_->DropPartition(grpc_partition_param);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to drop partition: " + std::string(ex.what()));
    }
}

Status
ClientProxy::FlushCollection(const std::string& collection_name) {
    try {
        std::string dummy;
        return client_ptr_->Flush(collection_name);
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to flush collection");
    }
}

Status
ClientProxy::Flush() {
    try {
        std::string dummy;
        return client_ptr_->Flush("");
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to flush collections");
    }
}

Status
ClientProxy::CompactCollection(const std::string& collection_name) {
    try {
        ::milvus::grpc::CollectionName grpc_collection_name;
        grpc_collection_name.set_collection_name(collection_name);
        Status status = client_ptr_->Compact(grpc_collection_name);
        return status;
    } catch (std::exception& ex) {
        return Status(StatusCode::UnknownError, "Failed to compact collection: " + std::string(ex.what()));
    }
}

/*******************************New Interface**********************************/

Status
ClientProxy::CreateHybridCollection(const HMapping& mapping) {
    try {
        ::milvus::grpc::Mapping grpc_mapping;
        grpc_mapping.set_collection_name(mapping.collection_name);
        for (auto field : mapping.numerica_fields) {
            ::milvus::grpc::FieldParam* field_param = grpc_mapping.add_fields();
            field_param->set_name(field->field_name);
            field_param->mutable_type()->set_data_type((::milvus::grpc::DataType)field->field_type);
            ::milvus::grpc::KeyValuePair* kv_pair = field_param->add_extra_params();
            kv_pair->set_key("params");
            kv_pair->set_value(field->extram_params);
        }
        for (auto field : mapping.vector_fields) {
            ::milvus::grpc::FieldParam* field_param = grpc_mapping.add_fields();
            field_param->set_name(field->field_name);
            field_param->mutable_type()->set_data_type((::milvus::grpc::DataType)field->field_type);
            field_param->mutable_type()->mutable_vector_param()->set_dimension(field->dimension);
            ::milvus::grpc::KeyValuePair* kv_pair = field_param->add_extra_params();
            kv_pair->set_key("params");
            kv_pair->set_value(field->extram_params);
        }
        return client_ptr_->CreateHybridCollection(grpc_mapping);
    } catch (std::exception& exception) {
        return Status(StatusCode::UnknownError, "Failed to create collection: " + std::string(exception.what()));
    }

}

void
CopyVectorField(::milvus::grpc::RowRecord* target, const Entity& src) {
    if (!src.float_data.empty()) {
        auto vector_data = target->mutable_float_data();
        vector_data->Resize(static_cast<int>(src.float_data.size()), 0.0);
        memcpy(vector_data->mutable_data(), src.float_data.data(), src.float_data.size() * sizeof(float));
    }

    if (!src.binary_data.empty()) {
        target->set_binary_data(src.binary_data.data(), src.binary_data.size());
    }
}

Status
ClientProxy::InsertEntity(const std::string& collection_name,
             const std::string& partition_tag,
             HEntity& entities,
             std::vector<uint64_t>& id_array) {
    Status status;
    try {
        ::milvus::grpc::HInsertParam grpc_param;
        grpc_param.set_collection_name(collection_name);
        grpc_param.set_partition_tag(partition_tag);

        auto numerica_it = entities.numerica_value.begin();
        for (; numerica_it != entities.numerica_value.end(); numerica_it++) {
            auto name = grpc_param.mutable_entities()->add_field_names();
            *name = numerica_it->first;
            auto records = grpc_param.mutable_entities()->add_attr_records();
            for (auto value : numerica_it->second) {
                auto attr = records->add_value();
                *attr = value;
            }
        }

        auto vector_it = entities.vector_value.begin();
        for (; vector_it != entities.vector_value.end(); vector_it++) {
            auto name = grpc_param.mutable_entities()->add_field_names();
            *name = vector_it->first;
            ::milvus::grpc::FieldValue* vector_field = grpc_param.mutable_entities()->add_result_values();
            for (auto entity : vector_it->second) {
                ::milvus::grpc::RowRecord* record = vector_field->mutable_vector_value()->add_value();
                CopyVectorField(record, entity);
            }
        }

        ::milvus::grpc::HEntityIDs entity_ids;
        if (!id_array.empty()) {
            auto row_ids = grpc_param.mutable_entity_id_array();
            row_ids->Resize(static_cast<int>(id_array.size()), -1);
            memcpy(row_ids->mutable_data(), id_array.data(), id_array.size() * sizeof(int64_t));
            status = client_ptr_->InsertEntities(grpc_param, entity_ids);
        } else {
            status = client_ptr_->InsertEntities(grpc_param, entity_ids);
            id_array.insert(id_array.end(), entity_ids.entity_id_array().begin(), entity_ids.entity_id_array().end());
        }
    } catch (std::exception& exception) {
        return Status(StatusCode::UnknownError, "Failed to create collection: " + std::string(exception.what()));
    }

    return status;
}

void
WriteQueryToProto(::milvus::grpc::GeneralQuery* general_query, BooleanQueryPtr boolean_query) {
    if (!boolean_query->GetBooleanQueries().empty()) {
        for (auto query : boolean_query->GetBooleanQueries()) {
            auto grpc_boolean_query = general_query->mutable_boolean_query();
            grpc_boolean_query->set_occur((::milvus::grpc::Occur)query->GetOccur());
            ::milvus::grpc::GeneralQuery* next_query = grpc_boolean_query->add_general_query();
            WriteQueryToProto(next_query, query);
        }
    } else {
        for (auto leaf_query : boolean_query->GetLeafQueries()) {
            ::milvus::grpc::GeneralQuery* grpc_query = general_query->mutable_boolean_query()->add_general_query();
            if (leaf_query->term_query_ptr != nullptr) {
                auto term_query = grpc_query->mutable_term_query();
                term_query->set_field_name(leaf_query->term_query_ptr->field_name);
                term_query->set_boost(leaf_query->query_boost);
                for (auto field_value : leaf_query->term_query_ptr->field_value) {
                    auto value = term_query->add_values();
                    *value = field_value;
                }
            }
            if (leaf_query->range_query_ptr != nullptr) {
                auto range_query = grpc_query->mutable_range_query();
                range_query->set_boost(leaf_query->query_boost);
                range_query->set_field_name(leaf_query->range_query_ptr->field_name);
                for (auto com_expr : leaf_query->range_query_ptr->compare_expr) {
                    auto grpc_com_expr = range_query->add_operand();
                    grpc_com_expr->set_operand(com_expr.operand);
                    grpc_com_expr->set_operator_((milvus::grpc::CompareOperator)com_expr.compare_operator);
                }
            }
            if (leaf_query->vector_query_ptr != nullptr) {
                auto vector_query = grpc_query->mutable_vector_query();
                vector_query->set_field_name(leaf_query->vector_query_ptr->field_name);
                vector_query->set_query_boost(leaf_query->query_boost);
                vector_query->set_topk(leaf_query->vector_query_ptr->topk);
                for (auto record : leaf_query->vector_query_ptr->query_vector) {
                    ::milvus::grpc::RowRecord* row_record = vector_query->add_records();
                    CopyRowRecord(row_record, record);
                }
                auto extra_param = vector_query->add_extra_params();
                extra_param->set_key(EXTRA_PARAM_KEY);
                extra_param->set_value(leaf_query->vector_query_ptr->extra_params);
            }
        }
    }
}

Status
ClientProxy::HybridSearch(const std::string& collection_name,
                          const std::vector<std::string>& partition_list,
                          BooleanQueryPtr& boolean_query,
                          const std::string& extra_params,
                          TopKQueryResult& topk_query_result) {
    try {
        // convert boolean_query to proto
        ::milvus::grpc::HSearchParam search_param;
        search_param.set_collection_name(collection_name);
        for (auto partition : partition_list) {
            auto value = search_param.add_partition_tag_array();
            *value = partition;
        }
        auto extra_param = search_param.add_extra_params();
        extra_param->set_key("params");
        extra_param->set_value(extra_params);
        WriteQueryToProto(search_param.mutable_general_query(), boolean_query);

        // step 2: search vectors
        ::milvus::grpc::TopKQueryResult result;
        Status status = client_ptr_->HybridSearch(search_param, result);

        // step 3: convert result array
        topk_query_result.reserve(result.row_num());
        int64_t nq = result.row_num();
        if (nq == 0) {
            return status;
        }
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
        return Status(StatusCode::UnknownError, "Failed to search entities: " + std::string(ex.what()));
    }
}

}  // namespace milvus
