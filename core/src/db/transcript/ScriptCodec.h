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

#pragma once

#include "db/DB.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {
namespace engine {

// action names
extern const char* ActionCreateCollection;
extern const char* ActionDropCollection;
extern const char* ActionHasCollection;
extern const char* ActionListCollections;
extern const char* ActionGetCollectionInfo;
extern const char* ActionGetCollectionStats;
extern const char* ActionCountEntities;
extern const char* ActionCreatePartition;
extern const char* ActionDropPartition;
extern const char* ActionHasPartition;
extern const char* ActionListPartitions;
extern const char* ActionCreateIndex;
extern const char* ActionDropIndex;
extern const char* ActionDescribeIndex;
extern const char* ActionInsert;
extern const char* ActionGetEntityByID;
extern const char* ActionDeleteEntityByID;
extern const char* ActionListIDInSegment;
extern const char* ActionQuery;
extern const char* ActionLoadCollection;
extern const char* ActionFlush;
extern const char* ActionCompact;

// json keys
extern const char* J_ACTION_TYPE;
extern const char* J_ACTION_TS;  // action timestamp

class ScriptCodec {
 public:
    // encode methods
    static Status
    EncodeAction(milvus::json& json_obj, const std::string& action_type);

    static Status
    Encode(milvus::json& json_obj, const snapshot::CreateCollectionContext& context);

    static Status
    EncodeCollectionName(milvus::json& json_obj, const std::string& collection_name);

    static Status
    EncodePartitionName(milvus::json& json_obj, const std::string& partition_name);

    static Status
    EncodeFieldName(milvus::json& json_obj, const std::string& field_name);

    static Status
    EncodeFieldNames(milvus::json& json_obj, const std::vector<std::string>& field_names);

    static Status
    Encode(milvus::json& json_obj, const CollectionIndex& index);

    static Status
    Encode(milvus::json& json_obj, const DataChunkPtr& data_chunk);

    static Status
    Encode(milvus::json& json_obj, const IDNumbers& id_array);

    static Status
    EncodeSegmentID(milvus::json& json_obj, int64_t segment_id);

    static Status
    Encode(milvus::json& json_obj, const query::QueryPtr& query_ptr);

    static Status
    EncodeThreshold(milvus::json& json_obj, double threshold);

    static Status
    EncodeForce(milvus::json& json_obj, bool force);

    // decode methods
    static Status
    DecodeAction(milvus::json& json_obj, std::string& action_type, int64_t& action_ts);

    static Status
    Decode(milvus::json& json_obj, snapshot::CreateCollectionContext& context);

    static Status
    DecodeCollectionName(milvus::json& json_obj, std::string& collection_name);

    static Status
    DecodePartitionName(milvus::json& json_obj, std::string& partition_name);

    static Status
    DecodeFieldName(milvus::json& json_obj, std::string& field_name);

    static Status
    DecodeFieldNames(milvus::json& json_obj, std::vector<std::string>& field_names);

    static Status
    Decode(milvus::json& json_obj, CollectionIndex& index);

    static Status
    Decode(milvus::json& json_obj, DataChunkPtr& data_chunk);

    static Status
    Decode(milvus::json& json_obj, IDNumbers& id_array);

    static Status
    DecodeSegmentID(milvus::json& json_obj, int64_t& segment_id);

    static Status
    Decode(milvus::json& json_obj, query::QueryPtr& query_ptr);

    static Status
    DecodeThreshold(milvus::json& json_obj, double& threshold);

    static Status
    DecodeForce(milvus::json& json_obj, bool& force);

 private:
    static Status
    EncodeGeneralQuery(milvus::json& json_obj, query::GeneralQueryPtr& query);

    static Status
    DecodeGeneralQuery(milvus::json& json_obj, query::GeneralQueryPtr& query);

 private:
    ScriptCodec() = delete;
};

}  // namespace engine
}  // namespace milvus
