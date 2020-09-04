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

#include "db/transcript/ScriptReplay.h"
#include "db/transcript/ScriptCodec.h"
#include "db/transcript/ScriptFile.h"
#include "utils/Json.h"

#include <experimental/filesystem>
#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

Status
ScriptReplay::Replay(const DBPtr& db, const std::string& replay_script_path) {
    // get all script files under this folder, arrange them in ascending order
    std::map<int64_t, std::experimental::filesystem::path> files_map;

    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    DirectoryIterator iter(replay_script_path);
    DirectoryIterator end;
    for (; iter != end; ++iter) {
        auto path = (*iter).path();
        if (std::experimental::filesystem::is_directory(path)) {
            continue;
        }

        std::string file_name = path.filename().c_str();
        int64_t file_ts = atol(file_name.c_str());
        if (file_ts > 0) {
            files_map.insert(std::make_pair(file_ts, path));
        }
    }

    // replay the script files
    for (auto& pair : files_map) {
        ScriptFile file;
        file.OpenRead(pair.second.c_str());

        std::string str_line;
        while (file.ReadLine(str_line)) {
            STATUS_CHECK(PerformAction(db, str_line));
        }
    }

    return Status::OK();
}

Status
ScriptReplay::PerformAction(const DBPtr& db, const std::string& str_action) {
    try {
        milvus::json json_obj = milvus::json::parse(str_action);
        std::string action_type;
        int64_t action_ts = 0;
        ScriptCodec::DecodeAction(json_obj, action_type, action_ts);

        if (action_type == ActionCreateCollection) {
            snapshot::CreateCollectionContext context;
            ScriptCodec::Decode(json_obj, context);

            db->CreateCollection(context);
        } else if (action_type == ActionDropCollection) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            db->DropCollection(collection_name);
        } else if (action_type == ActionHasCollection) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            bool has = false;
            db->HasCollection(collection_name, has);
        } else if (action_type == ActionListCollections) {
            std::vector<std::string> names;
            db->ListCollections(names);
        } else if (action_type == ActionGetCollectionInfo) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            snapshot::CollectionPtr collection;
            snapshot::FieldElementMappings fields_schema;
            db->GetCollectionInfo(collection_name, collection, fields_schema);
        } else if (action_type == ActionGetCollectionStats) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            milvus::json collection_stats;
            db->GetCollectionStats(collection_name, collection_stats);
        } else if (action_type == ActionCountEntities) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            int64_t count = 0;
            db->CountEntities(collection_name, count);
        } else if (action_type == ActionCreatePartition) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string partition_name;
            ScriptCodec::DecodePartitionName(json_obj, partition_name);

            db->CreatePartition(collection_name, partition_name);
        } else if (action_type == ActionDropPartition) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string partition_name;
            ScriptCodec::DecodePartitionName(json_obj, partition_name);

            db->DropPartition(collection_name, partition_name);
        } else if (action_type == ActionHasPartition) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string partition_name;
            ScriptCodec::DecodePartitionName(json_obj, partition_name);

            bool has = false;
            db->HasPartition(collection_name, partition_name, has);
        } else if (action_type == ActionListPartitions) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            std::vector<std::string> partition_names;
            db->ListPartitions(collection_name, partition_names);
        } else if (action_type == ActionCreateIndex) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string field_name;
            ScriptCodec::DecodeFieldName(json_obj, field_name);
            CollectionIndex index;
            ScriptCodec::Decode(json_obj, index);

            std::vector<std::string> partition_names;
            db->CreateIndex(nullptr, collection_name, field_name, index);
        } else if (action_type == ActionDropIndex) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string field_name;
            ScriptCodec::DecodeFieldName(json_obj, field_name);

            db->DropIndex(collection_name, field_name);
        } else if (action_type == ActionDescribeIndex) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string field_name;
            ScriptCodec::DecodeFieldName(json_obj, field_name);

            CollectionIndex index;
            db->DescribeIndex(collection_name, field_name, index);
        } else if (action_type == ActionInsert) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::string partition_name;
            ScriptCodec::DecodePartitionName(json_obj, partition_name);
            DataChunkPtr data_chunk;
            ScriptCodec::Decode(json_obj, data_chunk);

            db->Insert(collection_name, partition_name, data_chunk, 0);
        } else if (action_type == ActionGetEntityByID) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            IDNumbers id_array;
            ScriptCodec::Decode(json_obj, id_array);
            std::vector<std::string> field_names;
            ScriptCodec::DecodeFieldNames(json_obj, field_names);

            std::vector<bool> valid_row;
            DataChunkPtr data_chunk;
            db->GetEntityByID(collection_name, id_array, field_names, valid_row, data_chunk);
        } else if (action_type == ActionDeleteEntityByID) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            IDNumbers id_array;
            ScriptCodec::Decode(json_obj, id_array);

            db->DeleteEntityByID(collection_name, id_array, 0);
        } else if (action_type == ActionListIDInSegment) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            int64_t segment_id = 0;
            ScriptCodec::DecodeSegmentID(json_obj, segment_id);

            IDNumbers entity_ids;
            db->ListIDInSegment(collection_name, segment_id, entity_ids);
        } else if (action_type == ActionQuery) {
            query::QueryPtr query_ptr;
            ScriptCodec::Decode(json_obj, query_ptr);

            if (query_ptr != nullptr) {
                engine::QueryResultPtr result;
                db->Query(nullptr, query_ptr, result);
            }
        } else if (action_type == ActionLoadCollection) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            std::vector<std::string> field_names;
            ScriptCodec::DecodeFieldNames(json_obj, field_names);
            bool force = false;
            ScriptCodec::DecodeForce(json_obj, force);

            std::vector<bool> valid_row;
            DataChunkPtr data_chunk;
            db->LoadCollection(nullptr, collection_name, field_names, force);
        } else if (action_type == ActionFlush) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);

            if (collection_name.empty()) {
                db->Flush();
            } else {
                db->Flush(collection_name);
            }
        } else if (action_type == ActionCompact) {
            std::string collection_name;
            ScriptCodec::DecodeCollectionName(json_obj, collection_name);
            double threshold = 0.0;
            ScriptCodec::DecodeThreshold(json_obj, threshold);

            db->Compact(nullptr, collection_name, threshold);
        } else {
            std::string msg = "Unsupportted action: " + action_type;
            LOG_SERVER_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }

        return Status::OK();
    } catch (std::exception& ex) {
        std::string msg = "Failed to perform script action, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
        return Status(DB_ERROR, msg);
    }
}

}  // namespace engine
}  // namespace milvus
