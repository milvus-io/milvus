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

#include "db/transcript/ScriptRecorder.h"
#include "db/Utils.h"
#include "db/transcript/ScriptCodec.h"
#include "utils/CommonUtil.h"
#include "utils/Json.h"

#include <experimental/filesystem>
#include <memory>

namespace milvus {
namespace engine {

ScriptRecorder::ScriptRecorder(const std::string& path) {
    std::experimental::filesystem::path script_path(path);

    std::string time_str;
    CommonUtil::GetCurrentTimeStr(time_str);
    script_path /= time_str;
    script_path_ = script_path.c_str();
    CommonUtil::CreateDirectory(script_path_);
}

ScriptRecorder::~ScriptRecorder() {
}

std::string
ScriptRecorder::GetScriptPath() const {
    return script_path_;
}

ScriptFilePtr
ScriptRecorder::GetFile() {
    if (file_ == nullptr || file_->ExceedMaxSize(0)) {
        file_ = std::make_shared<ScriptFile>();
        int64_t current_time = utils::GetMicroSecTimeStamp();

        std::experimental::filesystem::path file_path(script_path_);
        std::string file_name = std::to_string(current_time) + ".txt";
        file_path /= file_name;

        file_->OpenWrite(file_path.c_str());
    }

    return file_;
}

Status
ScriptRecorder::WriteJson(milvus::json& json_obj) {
    auto file = GetFile();
    std::string str = json_obj.dump();
    return file->WriteLine(str);
}

Status
ScriptRecorder::CreateCollection(const snapshot::CreateCollectionContext& context) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionCreateCollection);
    ScriptCodec::Encode(json_obj, context);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::DropCollection(const std::string& collection_name) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionDropCollection);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::HasCollection(const std::string& collection_name, bool& has_or_not) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionHasCollection);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::ListCollections(std::vector<std::string>& names) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionListCollections);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                                  snapshot::FieldElementMappings& fields_schema) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionGetCollectionInfo);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionGetCollectionStats);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::CountEntities(const std::string& collection_name, int64_t& row_count) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionCountEntities);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionCreatePartition);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodePartitionName(json_obj, partition_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionDropPartition);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodePartitionName(json_obj, partition_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::HasPartition(const std::string& collection_name, const std::string& partition_name, bool& exist) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionHasPartition);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodePartitionName(json_obj, partition_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionListPartitions);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::CreateIndex(const server::ContextPtr& context, const std::string& collection_name,
                            const std::string& field_name, const CollectionIndex& index) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionCreateIndex);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodeFieldName(json_obj, field_name);
    ScriptCodec::Encode(json_obj, index);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::DropIndex(const std::string& collection_name, const std::string& field_name) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionDropIndex);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodeFieldName(json_obj, field_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::DescribeIndex(const std::string& collection_name, const std::string& field_name,
                              CollectionIndex& index) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionDescribeIndex);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodeFieldName(json_obj, field_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
                       idx_t op_id) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionInsert);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodePartitionName(json_obj, partition_name);
    ScriptCodec::Encode(json_obj, data_chunk);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                              const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                              DataChunkPtr& data_chunk) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionGetEntityByID);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::Encode(json_obj, id_array);
    ScriptCodec::EncodeFieldNames(json_obj, field_names);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionDeleteEntityByID);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::Encode(json_obj, entity_ids);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionListIDInSegment);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodeSegmentID(json_obj, segment_id);
    ScriptCodec::Encode(json_obj, entity_ids);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr,
                      engine::QueryResultPtr& result) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionQuery);
    ScriptCodec::Encode(json_obj, query_ptr);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                               const std::vector<std::string>& field_names, bool force) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionLoadCollection);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodeFieldNames(json_obj, field_names);
    ScriptCodec::EncodeForce(json_obj, force);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::Flush(const std::string& collection_name) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionFlush);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::Flush() {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionFlush);

    return WriteJson(json_obj);
}

Status
ScriptRecorder::Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) {
    milvus::json json_obj;
    ScriptCodec::EncodeAction(json_obj, ActionCompact);
    ScriptCodec::EncodeCollectionName(json_obj, collection_name);
    ScriptCodec::EncodeThreshold(json_obj, threshold);

    return WriteJson(json_obj);
}

}  // namespace engine
}  // namespace milvus
