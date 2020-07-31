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

#include <set>
#include <string>
#include <vector>

#include "db/Types.h"

namespace milvus {
namespace engine {
namespace snapshot {

using ID_TYPE = int64_t;
using NUM_TYPE = int64_t;
using FTYPE_TYPE = int64_t;
using TS_TYPE = int64_t;
using LSN_TYPE = int64_t;
using SIZE_TYPE = uint64_t;
using MappingT = std::set<ID_TYPE>;
using IDS_TYPE = std::vector<ID_TYPE>;

enum State { PENDING = 0, ACTIVE = 1, DEACTIVE = 2, INVALID = 999 };

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
