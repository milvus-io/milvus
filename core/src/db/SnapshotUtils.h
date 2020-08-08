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

#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "db/snapshot/Resources.h"
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

Status
GetSnapshotInfo(const std::string& collection_name, milvus::json& json_info);

Status
GetSegmentFileRelatePath(const SegmentFieldElementVisitorPtr& element_visitor, std::string& path);
}  // namespace engine
}  // namespace milvus
