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

#include "db/snapshot/ResourceHolders.h"
#include <iostream>
#include <sstream>
#include "db/snapshot/CompoundOperations.h"

namespace milvus {
namespace engine {
namespace snapshot {

void
CollectionsHolder::Reset() {
    BaseT::Reset();
    name_map_.clear();
}

CollectionsHolder::ResourcePtr
CollectionsHolder::Load(const std::string& name) {
    LoadOperationContext context;
    context.name = name;
    auto op = std::make_shared<LoadOperation<Collection>>(context);
    op->Push();
    auto c = op->GetResource();
    if (c) {
        BaseT::AddNoLock(c);
        return c;
    }
    return nullptr;
}

CollectionsHolder::ScopedT
CollectionsHolder::GetCollection(const std::string& name, bool scoped) {
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    auto cit = name_map_.find(name);
    if (cit == name_map_.end()) {
        auto ret = Load(name);
        if (!ret)
            return BaseT::ScopedT();
        return BaseT::ScopedT(ret, scoped);
    }
    return BaseT::ScopedT(cit->second, scoped);
}

bool
CollectionsHolder::Add(CollectionsHolder::ResourcePtr resource) {
    if (!resource)
        return false;
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    return BaseT::AddNoLock(resource);
}

bool
CollectionsHolder::Release(const std::string& name) {
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    auto it = name_map_.find(name);
    if (it == name_map_.end()) {
        return false;
    }

    BaseT::id_map_.erase(it->second->GetID());
    name_map_.erase(it);
    return true;
}

bool
CollectionsHolder::Release(ID_TYPE id) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = id_map_.find(id);
    if (it == id_map_.end()) {
        return false;
    }

    BaseT::id_map_.erase(it);
    name_map_.erase(it->second->GetName());
    return true;
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
