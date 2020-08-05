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
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace utils {

int64_t
GetMicroSecTimeStamp();

bool
IsSameIndex(const CollectionIndex& index1, const CollectionIndex& index2);

bool
IsBinaryMetricType(const std::string& metric_type);

engine::DateT
GetDate(const std::time_t& t, int day_delta = 0);
engine::DateT
GetDate();
engine::DateT
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

void
SendExitSignal();

void
GetIDFromChunk(const engine::DataChunkPtr& chunk, engine::IDNumbers& ids);

}  // namespace utils
}  // namespace engine
}  // namespace milvus
