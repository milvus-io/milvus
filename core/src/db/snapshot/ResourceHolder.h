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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "config/ServerConfig.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/ResourceGCEvent.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/ScopedResource.h"
#include "db/snapshot/Store.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename ResourceT, typename Derived>
class ResourceHolder {
    using ResourcePtr = std::shared_ptr<ResourceT>;
    using ScopedT = ScopedResource<ResourceT>;
    using ScopedPtr = std::shared_ptr<ScopedT>;
    using IdMapT = std::map<ID_TYPE, ResourcePtr>;
    using Ptr = std::shared_ptr<Derived>;

 protected:
    ResourceHolder() = default;
    virtual ~ResourceHolder() = default;

 public:
    static Derived&
    GetInstance() {
        static Derived holder;
        return holder;
    }

    ScopedT
    GetResource(StorePtr store, ID_TYPE id, bool scoped = true) {
        return Load(store, id, scoped);
    }

    ScopedT
    GetResource(ID_TYPE id, bool scoped = true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            auto cit = id_map_.find(id);
            if (cit != id_map_.end()) {
                return ScopedT(cit->second, scoped);
            }
        }
        return ScopedT();
    }

    virtual bool
    Add(ResourcePtr resource) {
        std::unique_lock<std::mutex> lock(mutex_);
        return AddNoLock(resource);
    }

    virtual bool
    Release(ID_TYPE id) {
        std::unique_lock<std::mutex> lock(mutex_);
        return ReleaseNoLock(id);
    }

    ScopedT
    Load(StorePtr store, ID_TYPE id, bool scoped = true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            auto cit = id_map_.find(id);
            if (cit != id_map_.end()) {
                if (!cit->second->IsActive()) {
                    return ScopedT();
                }
                return ScopedT(cit->second, scoped);
            }
        }
        auto ret = DoLoad(store, id);
        if (!ret) {
            return ScopedT();
        }
        return ScopedT(ret, scoped);
    }

    virtual void
    Reset() {
        id_map_.clear();
    }

    virtual void
    Dump(const std::string& tag = "") {
        std::unique_lock<std::mutex> lock(mutex_);
        LOG_ENGINE_DEBUG_ << typeid(*this).name() << " Dump Start [" << tag << "]:" << id_map_.size();
        for (auto& kv : id_map_) {
            /* std::cout << "\t" << kv.second->ToString() << std::endl; */
            LOG_ENGINE_DEBUG_ << "\t" << kv.first << " RefCnt " << kv.second->ref_count();
        }
        LOG_ENGINE_DEBUG_ << typeid(*this).name() << " Dump   End [" << tag << "]";
    }

 private:
    bool
    AddNoLock(ResourcePtr resource) {
        if (!resource) {
            return false;
        }
        if (id_map_.find(resource->GetID()) != id_map_.end()) {
            return false;
        }
        id_map_[resource->GetID()] = resource;
        resource->RegisterOnNoRefCB(std::bind(&Derived::OnNoRefCallBack, this, resource));
        return true;
    }

    bool
    ReleaseNoLock(ID_TYPE id) {
        auto it = id_map_.find(id);
        if (it == id_map_.end()) {
            return false;
        }
        id_map_.erase(it);
        return true;
    }

    virtual void
    OnNoRefCallBack(ResourcePtr resource) {
        resource->Deactivate();
        Release(resource->GetID());
        auto evt_ptr = std::make_shared<ResourceGCEvent<ResourceT>>(resource);
        EventExecutor::GetInstance().Submit(evt_ptr);
    }

    virtual ResourcePtr
    DoLoad(StorePtr store, ID_TYPE id) {
        LoadOperationContext context;
        context.id = id;
        auto op = std::make_shared<LoadOperation<ResourceT>>(context);
        (*op)(store);
        typename ResourceT::Ptr c;
        auto status = op->GetResource(c);
        if (status.ok() && c->IsActive()) {
            /* if (status.ok()) { */
            Add(c);
            return c;
        }
        return nullptr;
    }

 private:
    std::mutex mutex_;
    IdMapT id_map_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
