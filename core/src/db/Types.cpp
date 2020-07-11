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

#include <faiss/Index.h>

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/engine/ExecutionEngine.h"
#include "db/meta/MetaTypes.h"
#include "segment/Types.h"
#include "utils/Json.h"

namespace milvus {
namespace engine {

const char* DEFAULT_UID_NAME = "_uid";
const char* DEFAULT_BLOOM_FILTER_NAME = "_blf";
const char* DEFAULT_DELETED_DOCS_NAME = "_del";

}  // namespace engine
}  // namespace milvus
