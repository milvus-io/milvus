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
#include "thirdparty/nlohmann/json.hpp"

#include <string>

namespace milvus {
namespace engine {

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
GetSnapshotInfo(const std::string& collection_name, nlohmann::json& json_info);

}  // namespace engine
}  // namespace milvus
