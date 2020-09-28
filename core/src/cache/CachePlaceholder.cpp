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

#include "cache/CachePlaceholder.h"
#include "cache/CpuCacheMgr.h"
#include "db/IDGenerator.h"
#include "utils/Log.h"

namespace milvus {
namespace cache {

CachePlaceholder::CachePlaceholder(int64_t size) {
    auto id = engine::SafeIDGenerator::GetInstance().GetNextIDNumber();
    item_key_ = "Placehold_" + std::to_string(id);
    DataObjPtr mock_obj = std::make_shared<MockDataObj>(size);
    CpuCacheMgr::GetInstance().InsertItem(item_key_, mock_obj);
}

CachePlaceholder::~CachePlaceholder() {
    Erase();
}

void
CachePlaceholder::Erase() {
    CpuCacheMgr::GetInstance().EraseItem(item_key_);
}

}  // namespace cache
}  // namespace milvus
