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

#include "db/Types.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Snapshot.h"
#include "utils/Json.h"

#include <string>

namespace milvus {
namespace engine {

extern const char* JSON_ROW_COUNT;
extern const char* JSON_ID;
extern const char* JSON_PARTITIONS;
extern const char* JSON_PARTITION_TAG;
extern const char* JSON_SEGMENTS;
extern const char* JSON_FIELD;
extern const char* JSON_NAME;
extern const char* JSON_FILES;
extern const char* JSON_INDEX_NAME;
extern const char* JSON_DATA_SIZE;
extern const char* JSON_PATH;

Status
SetSnapshotIndex(const std::string& collection_name, const std::string& field_name,
                 engine::CollectionIndex& index_info);

Status
GetSnapshotIndex(const std::string& collection_name, const std::string& field_name,
                 engine::CollectionIndex& index_info);

Status
DeleteSnapshotIndex(const std::string& collection_name, const std::string& field_name);

bool
IsVectorField(const engine::snapshot::FieldPtr& field);

bool
IsVectorField(engine::DataType type);

Status
GetSnapshotInfo(const std::string& collection_name, milvus::json& json_info);

Status
GetSegmentRowCount(const std::string& collection_name, int64_t& segment_row_count);

Status
GetSegmentRowCount(int64_t collection_id, int64_t& segment_row_count);

Status
GetSegmentRowCount(const snapshot::CollectionPtr& collection, int64_t& segment_row_count);

Status
ClearCollectionCache(snapshot::ScopedSnapshotT& ss, const std::string& dir_root);

Status
ClearPartitionCache(snapshot::ScopedSnapshotT& ss, const std::string& dir_root, snapshot::ID_TYPE partition_id);

Status
ClearIndexCache(snapshot::ScopedSnapshotT& ss, const std::string& dir_root, const std::string& field_name);

Status
DropSegment(snapshot::ScopedSnapshotT& ss, snapshot::ID_TYPE segment_id);

}  // namespace engine
}  // namespace milvus
