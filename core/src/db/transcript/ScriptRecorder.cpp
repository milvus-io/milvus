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
#include "db/transcript/ScriptCodec.h"
#include "db/Utils.h"
#include "utils/CommonUtil.h"

#include <experimental/filesystem>

namespace milvus {
namespace engine {

ScriptRecorder&
ScriptRecorder::GetInstance() {
    static ScriptRecorder s_recorder;
    return s_recorder;
}

void
ScriptRecorder::SetScriptPath(const std::string& path) {
    std::experimental::filesystem::path script_path(path);

    std::string time_str;
    CommonUtil::GetCurrentTimeStr(time_str);
    script_path /= time_str;
    script_path_ = script_path.c_str();
}

ScriptFilePtr
ScriptRecorder::GetFile() {
    if (file_ == nullptr || file_->ExceedMaxSize(0)) {
        file_ = std::make_shared<ScriptFile>();
        int64_t current_time = utils::GetMicroSecTimeStamp();

        std::experimental::filesystem::path file_path(script_path_);
        std::string file_name = std::to_string(current_time) + ".txt";
        file_path /= file_name;

    }

    return file_;
}

Status
ScriptRecorder::CreateCollection(const snapshot::CreateCollectionContext& context) {
    auto file = GetFile();


    return Status::OK();
}

Status
ScriptRecorder::DropCollection(const std::string& collection_name) {
    return Status::OK();
}

Status
ScriptRecorder::HasCollection(const std::string& collection_name, bool& has_or_not) {
    return Status::OK();
}

Status
ScriptRecorder::ListCollections(std::vector<std::string>& names) {
    return Status::OK();
}

Status
ScriptRecorder::GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                                  snapshot::FieldElementMappings& fields_schema) {
    return Status::OK();
}

Status
ScriptRecorder::GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) {
    return Status::OK();
}

Status
ScriptRecorder::CountEntities(const std::string& collection_name, int64_t& row_count) {
    return Status::OK();
}

Status
ScriptRecorder::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    return Status::OK();
}

Status
ScriptRecorder::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    return Status::OK();
}

Status
ScriptRecorder::HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) {
    return Status::OK();
}

Status
ScriptRecorder::ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    return Status::OK();
}

Status
ScriptRecorder::CreateIndex(const server::ContextPtr& context,
                            const std::string& collection_name,
                            const std::string& field_name,
                            const CollectionIndex& index) {
    return Status::OK();
}

Status
ScriptRecorder::DropIndex(const std::string& collection_name, const std::string& field_name) {
    return Status::OK();
}

Status
ScriptRecorder::DescribeIndex(const std::string& collection_name,
                              const std::string& field_name,
                              CollectionIndex& index) {
    return Status::OK();
}

Status
ScriptRecorder::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
                       idx_t op_id) {
    return Status::OK();
}

Status
ScriptRecorder::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                              const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                              DataChunkPtr& data_chunk) {
    return Status::OK();
}

Status
ScriptRecorder::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id) {
    return Status::OK();
}

Status
ScriptRecorder::ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) {
    return Status::OK();
}

Status
ScriptRecorder::Query(const server::ContextPtr& context,
                      const query::QueryPtr& query_ptr,
                      engine::QueryResultPtr& result) {
    return Status::OK();
}

Status
ScriptRecorder::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                               const std::vector<std::string>& field_names, bool force) {
    return Status::OK();
}

Status
ScriptRecorder::Flush(const std::string& collection_name) {
    return Status::OK();
}

Status
ScriptRecorder::Flush() {
    return Status::OK();
}

Status
ScriptRecorder::Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) {
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
