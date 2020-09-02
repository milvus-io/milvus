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
const char* J_ACTION_TS = "time"; // action timestamp
const char* J_COLLECTION_NAME = "collection_name";
const char* J_PARTITION_NAME = "partition_name";
const char* J_FIELD_NAME = "field_name";
const char* J_MAPPINGS = "mappings";
const char* J_PARAMS = "params";

const char* J_SEGMENT_ID = "segment_id";
const char* J_THRESHOLD = "threshold";
const char* J_FORCE = "force";


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
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const CollectionIndex& index) {
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const DataChunkPtr& data_chunk) {
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const IDNumbers& id_array) {
    return Status::OK();
}


Status
ScriptCodec::EncodeSegmentID(milvus::json& json_obj, int64_t segment_id) {
    json_obj[J_SEGMENT_ID] = segment_id;
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const query::QueryPtr& query_ptr) {
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
    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, CollectionIndex& index) {
    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, DataChunkPtr& data_chunk) {
    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, IDNumbers& id_array) {
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
