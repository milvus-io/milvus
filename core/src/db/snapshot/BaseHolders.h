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
#include "ResourceTypes.h"
#include "ScopedResource.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename ResourceT, typename Derived>
class ResourceHolder {
 public:
    using ResourcePtr = std::shared_ptr<ResourceT>;
    /* using ResourcePtr = typename ResourceT::Ptr; */
    using ScopedT = ScopedResource<ResourceT>;
    using ScopedPtr = std::shared_ptr<ScopedT>;
    using IdMapT = std::map<ID_TYPE, ResourcePtr>;
    using Ptr = std::shared_ptr<Derived>;
    ScopedT
    GetResource(ID_TYPE id, bool scoped = true);

    bool
    AddNoLock(ResourcePtr resource);
    bool
    ReleaseNoLock(ID_TYPE id);

    virtual bool
    Add(ResourcePtr resource);
    virtual bool
    Release(ID_TYPE id);
    virtual bool
    HardDelete(ID_TYPE id);

    static Derived&
    GetInstance() {
        static Derived holder;
        return holder;
    }

    virtual void
    Reset();

    virtual void
    Dump(const std::string& tag = "");

 protected:
    virtual void
    OnNoRefCallBack(ResourcePtr resource);

    virtual ResourcePtr
    Load(ID_TYPE id);
    virtual ResourcePtr
    Load(const std::string& name);
    ResourceHolder() = default;
    virtual ~ResourceHolder() = default;

    std::mutex mutex_;
    IdMapT id_map_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus

#include "BaseHolders.inl"
