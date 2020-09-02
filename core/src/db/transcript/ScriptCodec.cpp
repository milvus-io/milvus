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

#include "db/transcript/ScriptCodec.h"

#include <memory>
#include <utility>

namespace milvus {
namespace engine {

// action names
const char* ActionCreateCollection = "CreateCollection";
const char* ActionDropCollection = "DropCollection";
const char* ActionHasCollection = "HasCollection";
const char* ActionListCollections = "ListCollections";
const char* ActionGetCollectionInfo = "GetCollectionInfo";
const char* ActionGetCollectionStats = "GetCollectionStats";
const char* ActionCountEntities = "CountEntities";
const char* ActionCreatePartition = "CreatePartition";
const char* ActionDropPartition = "DropPartition";
const char* ActionHasPartition = "HasPartition";
const char* ActionListPartitions = "ListPartitions";
const char* ActionCreateIndex = "CreateIndex";
const char* ActionDropIndex = "DropIndex";
const char* ActionDescribeIndex = "DescribeIndex";
const char* ActionInsert = "Insert";
const char* ActionGetEntityByID = "GetEntityByID";
const char* ActionDeleteEntityByID = "DeleteEntityByID";
const char* ActionListIDInSegment = "ListIDInSegment";
const char* ActionQuery = "Query";
const char* ActionLoadCollection = "LoadCollection";
const char* ActionFlush = "Flush";
const char* ActionCompact = "Compact";

// json keys
const char* J_ACTION_TYPE = "action";
const char* J_ACTION_TS = "time";  // action timestamp
const char* J_COLLECTION_NAME = "col_name";
const char* J_PARTITION_NAME = "part_name";
const char* J_FIELD_NAME = "field_name";
const char* J_FIELD_NAMES = "field_names";
const char* J_FIELD_TYPE = "field_type";
const char* J_MAPPINGS = "mappings";
const char* J_PARAMS = "params";
const char* J_ID_ARRAY = "id_array";
const char* J_SEGMENT_ID = "segment_id";
const char* J_THRESHOLD = "threshold";
const char* J_FORCE = "force";
const char* J_INDEX_NAME = "index_name";
const char* J_INDEX_TYPE = "index_type";
const char* J_METRIC_TYPE = "metric_type";
const char* J_CHUNK_COUNT = "count";
const char* J_FIXED_FIELDS = "fixed_fields";
const char* J_VARIABLE_FIELDS = "variable_fields";
const char* J_CHUNK_DATA = "data";
const char* J_CHUNK_OFFSETS = "offsets";

// encode methods
Status
ScriptCodec::EncodeAction(milvus::json& json_obj, const std::string& action_type) {
    json_obj[J_ACTION_TYPE] = action_type;
    json_obj[J_ACTION_TS] = utils::GetMicroSecTimeStamp();
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const snapshot::CreateCollectionContext& context) {
    json_obj[J_COLLECTION_NAME] = context.collection->GetName();

    // params
    json_obj[J_PARAMS] = context.collection->GetParams();

    // mappings
    milvus::json json_fields;
    for (const auto& pair : context.fields_schema) {
        auto& field = pair.first;
        milvus::json json_field;
        json_field[J_FIELD_NAME] = field->GetName();
        json_field[J_FIELD_TYPE] = field->GetFtype();
        json_field[J_PARAMS] = field->GetParams();
        json_fields.push_back(json_field);
    }
    json_obj[J_MAPPINGS] = json_fields;

    return Status::OK();
}

Status
ScriptCodec::EncodeCollectionName(milvus::json& json_obj, const std::string& collection_name) {
    json_obj[J_COLLECTION_NAME] = collection_name;
    return Status::OK();
}

Status
ScriptCodec::EncodePartitionName(milvus::json& json_obj, const std::string& partition_name) {
    json_obj[J_PARTITION_NAME] = partition_name;
    return Status::OK();
}

Status
ScriptCodec::EncodeFieldName(milvus::json& json_obj, const std::string& field_name) {
    json_obj[J_FIELD_NAME] = field_name;
    return Status::OK();
}

Status
ScriptCodec::EncodeFieldNames(milvus::json& json_obj, const std::vector<std::string>& field_names) {
    json_obj[J_FIELD_NAMES] = field_names;
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const CollectionIndex& index) {
    json_obj[J_INDEX_NAME] = index.index_name_;
    json_obj[J_INDEX_TYPE] = index.index_type_;
    json_obj[J_METRIC_TYPE] = index.metric_name_;
    json_obj[J_PARAMS] = index.extra_params_;

    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const DataChunkPtr& data_chunk) {
    if (data_chunk == nullptr) {
        return Status::OK();
    }

    json_obj[J_CHUNK_COUNT] = data_chunk->count_;

    // fixed fields
    {
        milvus::json json_fields;
        for (const auto& pair : data_chunk->fixed_fields_) {
            auto& data = pair.second;
            if (data == nullptr) {
                continue;
            }

            milvus::json json_field;
            json_field[J_FIELD_NAME] = pair.first;
            json_field[J_CHUNK_DATA] = data->data_;
            json_fields.push_back(json_field);
        }
        json_obj[J_FIXED_FIELDS] = json_fields;
    }

    // variable fields
    {
        milvus::json json_fields;
        for (const auto& pair : data_chunk->variable_fields_) {
            auto& data = pair.second;
            if (data == nullptr) {
                continue;
            }

            milvus::json json_field;
            json_field[J_FIELD_NAME] = pair.first;
            json_field[J_CHUNK_DATA] = data->data_;
            json_field[J_CHUNK_OFFSETS] = data->offset_;

            json_fields.push_back(json_field);
        }
        json_obj[J_VARIABLE_FIELDS] = json_fields;
    }

    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const IDNumbers& id_array) {
    json_obj[J_ID_ARRAY] = id_array;
    return Status::OK();
}

Status
ScriptCodec::EncodeSegmentID(milvus::json& json_obj, int64_t segment_id) {
    json_obj[J_SEGMENT_ID] = segment_id;
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const query::QueryPtr& query_ptr) {
    if (query_ptr == nullptr) {
        return Status::OK();
    }

    return Status::OK();
}

Status
ScriptCodec::EncodeThreshold(milvus::json& json_obj, double threshold) {
    json_obj[J_THRESHOLD] = threshold;
    return Status::OK();
}

Status
ScriptCodec::EncodeForce(milvus::json& json_obj, bool force) {
    json_obj[J_FORCE] = force;
    return Status::OK();
}

// decode methods
Status
ScriptCodec::DecodeAction(milvus::json& json_obj, std::string& action_type, int64_t& action_ts) {
    action_type = "";
    action_ts = 0;
    if (json_obj.find(J_ACTION_TYPE) != json_obj.end()) {
        action_type = json_obj[J_ACTION_TYPE].get<std::string>();
    } else {
        return Status(DB_ERROR, "element doesn't exist");
    }

    if (json_obj.find(J_ACTION_TS) != json_obj.end()) {
        action_ts = json_obj[J_ACTION_TS].get<int64_t>();
    } else {
        return Status(DB_ERROR, "element doesn't exist");
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, snapshot::CreateCollectionContext& context) {
    std::string collection_name;
    ScriptCodec::DecodeCollectionName(json_obj, collection_name);

    milvus::json params;
    if (json_obj.find(J_PARAMS) != json_obj.end()) {
        params = json_obj[J_PARAMS];
    }
    context.collection = std::make_shared<snapshot::Collection>(collection_name, params);

    // mappings
    if (json_obj.find(J_MAPPINGS) != json_obj.end()) {
        milvus::json fields = json_obj[J_MAPPINGS];
        for (size_t i = 0; i < fields.size(); ++i) {
            auto field = fields[i];
            std::string field_name = field[J_FIELD_NAME].get<std::string>();
            DataType field_type = static_cast<DataType>(field[J_FIELD_TYPE].get<int32_t>());
            milvus::json field_params = field[J_PARAMS];
            auto field_ptr = std::make_shared<engine::snapshot::Field>(field_name, 0, field_type, field_params);
            context.fields_schema[field_ptr] = {};
        }
    }

    return Status::OK();
}

Status
ScriptCodec::DecodeCollectionName(milvus::json& json_obj, std::string& collection_name) {
    if (json_obj.find(J_COLLECTION_NAME) != json_obj.end()) {
        collection_name = json_obj[J_COLLECTION_NAME].get<std::string>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodePartitionName(milvus::json& json_obj, std::string& partition_name) {
    if (json_obj.find(J_PARTITION_NAME) != json_obj.end()) {
        partition_name = json_obj[J_PARTITION_NAME].get<std::string>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeFieldName(milvus::json& json_obj, std::string& field_name) {
    if (json_obj.find(J_FIELD_NAME) != json_obj.end()) {
        field_name = json_obj[J_FIELD_NAME].get<std::string>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeFieldNames(milvus::json& json_obj, std::vector<std::string>& field_names) {
    milvus::json names = json_obj[J_FIELD_NAMES];
    for (size_t i = 0; i < names.size(); ++i) {
        auto name = names[i].get<std::string>();
        field_names.push_back(name);
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, CollectionIndex& index) {
    if (json_obj.find(J_INDEX_NAME) != json_obj.end()) {
        index.index_name_ = json_obj[J_INDEX_NAME].get<std::string>();
    }
    if (json_obj.find(J_INDEX_TYPE) != json_obj.end()) {
        index.index_type_ = json_obj[J_INDEX_TYPE].get<std::string>();
    }
    if (json_obj.find(J_METRIC_TYPE) != json_obj.end()) {
        index.metric_name_ = json_obj[J_METRIC_TYPE].get<std::string>();
    }
    if (json_obj.find(J_PARAMS) != json_obj.end()) {
        index.extra_params_ = json_obj[J_PARAMS];
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, DataChunkPtr& data_chunk) {
    data_chunk = std::make_shared<DataChunk>();
    if (json_obj.find(J_CHUNK_COUNT) != json_obj.end()) {
        data_chunk->count_ = json_obj[J_CHUNK_COUNT].get<int64_t>();
    }

    // fixed fields
    if (json_obj.find(J_FIXED_FIELDS) != json_obj.end()) {
        auto fields = json_obj[J_FIXED_FIELDS];
        for (size_t i = 0; i < fields.size(); ++i) {
            auto field = fields[i];

            std::string name = field[J_FIELD_NAME];
            BinaryDataPtr bin = std::make_shared<BinaryData>();
            bin->data_ = field[J_CHUNK_DATA].get<std::vector<uint8_t>>();
            data_chunk->fixed_fields_.insert(std::make_pair(name, bin));
        }
    }

    // variable fields
    if (json_obj.find(J_VARIABLE_FIELDS) != json_obj.end()) {
        auto fields = json_obj[J_VARIABLE_FIELDS];
        for (size_t i = 0; i < fields.size(); ++i) {
            auto field = fields[i];

            std::string name = field[J_FIELD_NAME];
            VaribleDataPtr bin = std::make_shared<VaribleData>();
            bin->data_ = field[J_CHUNK_DATA].get<std::vector<uint8_t>>();
            bin->offset_ = field[J_CHUNK_OFFSETS].get<std::vector<int64_t>>();

            data_chunk->variable_fields_.insert(std::make_pair(name, bin));
        }
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, IDNumbers& id_array) {
    milvus::json ids = json_obj[J_ID_ARRAY];
    for (size_t i = 0; i < ids.size(); ++i) {
        auto id = ids[i].get<idx_t>();
        id_array.push_back(id);
    }

    return Status::OK();
}

Status
ScriptCodec::DecodeSegmentID(milvus::json& json_obj, int64_t& segment_id) {
    if (json_obj.find(J_SEGMENT_ID) != json_obj.end()) {
        segment_id = json_obj[J_SEGMENT_ID].get<int64_t>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::Decode(milvus::json& json_obj, query::QueryPtr& query_ptr) {
    return Status::OK();
}

Status
ScriptCodec::DecodeThreshold(milvus::json& json_obj, double& threshold) {
    if (json_obj.find(J_THRESHOLD) != json_obj.end()) {
        threshold = json_obj[J_THRESHOLD].get<double>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeForce(milvus::json& json_obj, bool& force) {
    if (json_obj.find(J_FORCE) != json_obj.end()) {
        force = json_obj[J_FORCE].get<bool>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

}  // namespace engine
}  // namespace milvus
