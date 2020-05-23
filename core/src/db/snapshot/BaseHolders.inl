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

#include "BaseHolders.h"
#include "Operations.h"
#include <iostream>
#include <memory>

namespace milvus {
namespace engine {
namespace snapshot {

template <typename ResourceT, typename Derived>
void ResourceHolder<ResourceT, Derived>::Dump(const std::string& tag) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::cout << typeid(*this).name() << " Dump Start [" << tag <<  "]:" << id_map_.size() << std::endl;
    for (auto& kv : id_map_) {
        /* std::cout << "\t" << kv.second->ToString() << std::endl; */
        std::cout << "\t" << kv.first << " RefCnt " << kv.second->RefCnt() << std::endl;
    }
    std::cout << typeid(*this).name() << " Dump   End [" << tag <<  "]" << std::endl;
}

template <typename ResourceT, typename Derived>
void ResourceHolder<ResourceT, Derived>::Reset() {
    id_map_.clear();
}

template <typename ResourceT, typename Derived>
typename ResourceHolder<ResourceT, Derived>::ResourcePtr
ResourceHolder<ResourceT, Derived>::Load(ID_TYPE id) {
    LoadOperationContext context;
    context.id = id;
    auto op = std::make_shared<LoadOperation<ResourceT>>(context);
    op->Push();
    auto c = op->GetResource();
    if (c) {
        Add(c);
        return c;
    }
    return nullptr;
}

template <typename ResourceT, typename Derived>
typename ResourceHolder<ResourceT, Derived>::ResourcePtr
ResourceHolder<ResourceT, Derived>::Load(const std::string& name) {
    return nullptr;
}

template <typename ResourceT, typename Derived>
typename ResourceHolder<ResourceT, Derived>::ScopedT
ResourceHolder<ResourceT, Derived>::GetResource(ID_TYPE id, bool scoped) {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto cit = id_map_.find(id);
        if (cit != id_map_.end()) {
            return ScopedT(cit->second, scoped);
        }
    }
    auto ret = Load(id);
    if (!ret) return ScopedT();
    return ScopedT(ret, scoped);
}

template <typename ResourceT, typename Derived>
void
ResourceHolder<ResourceT, Derived>::OnNoRefCallBack(typename ResourceHolder<ResourceT, Derived>::ResourcePtr resource) {
    HardDelete(resource->GetID());
    Release(resource->GetID());
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::ReleaseNoLock(ID_TYPE id) {
    auto it = id_map_.find(id);
    if (it == id_map_.end()) {
        return false;
    }

    id_map_.erase(it);
    return true;
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::Release(ID_TYPE id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return ReleaseNoLock(id);
}

template <typename ResourceT, typename Derived>
bool
ResourceHolder<ResourceT, Derived>::HardDelete(ID_TYPE id) {
    auto op = std::make_shared<HardDeleteOperation<ResourceT>>(id);
    op->Push(false);
    return true;
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::AddNoLock(typename ResourceHolder<ResourceT, Derived>::ResourcePtr resource) {
    if (!resource) return false;
    if (id_map_.find(resource->GetID()) != id_map_.end()) {
        return false;
    }

    id_map_[resource->GetID()] = resource;
    resource->RegisterOnNoRefCB(std::bind(&Derived::OnNoRefCallBack, this, resource));
    return true;
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::Add(typename ResourceHolder<ResourceT, Derived>::ResourcePtr resource) {
    std::unique_lock<std::mutex> lock(mutex_);
    return AddNoLock(resource);
}

} // namespace snapshot
} // namespace engine
} // namespace milvus
