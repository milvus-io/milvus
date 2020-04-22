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

#include <ctime>
#include <string>

#include "Options.h"
#include "db/Types.h"
#include "db/meta/MetaTypes.h"

namespace milvus {
namespace engine {
namespace utils {

int64_t
GetMicroSecTimeStamp();

Status
CreateCollectionPath(const DBMetaOptions& options, const std::string& collection_id);
Status
DeleteCollectionPath(const DBMetaOptions& options, const std::string& collection_id, bool force = true);

Status
CreateCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file);
Status
GetCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file);
Status
DeleteCollectionFilePath(const DBMetaOptions& options, meta::SegmentSchema& table_file);
Status
DeleteSegment(const DBMetaOptions& options, meta::SegmentSchema& table_file);

Status
GetParentPath(const std::string& path, std::string& parent_path);

bool
IsSameIndex(const CollectionIndex& index1, const CollectionIndex& index2);

bool
IsRawIndexType(int32_t type);

bool
IsBinaryMetricType(int32_t metric_type);

meta::DateT
GetDate(const std::time_t& t, int day_delta = 0);
meta::DateT
GetDate();
meta::DateT
GetDateWithDelta(int day_delta);

struct MetaUriInfo {
    std::string dialect_;
    std::string username_;
    std::string password_;
    std::string host_;
    std::string port_;
    std::string db_name_;
};

Status
ParseMetaUri(const std::string& uri, MetaUriInfo& info);

}  // namespace utils
}  // namespace engine
}  // namespace milvus
