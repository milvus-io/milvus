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

#include "Cache.h"
#include "utils/Log.h"

#include <memory>
#include <string>

namespace milvus {
namespace cache {

template <typename ItemObj>
class CacheMgr {
 public:
    virtual uint64_t
    ItemCount() const;

    virtual bool
    ItemExists(const std::string& key);

    virtual ItemObj
    GetItem(const std::string& key);

    virtual void
    InsertItem(const std::string& key, const ItemObj& data);

    virtual void
    EraseItem(const std::string& key);

    virtual bool
    Reserve(const int64_t size);

    virtual void
    PrintInfo();

    virtual void
    ClearCache();

    int64_t
    CacheUsage() const;

    int64_t
    CacheCapacity() const;

    void
    SetCapacity(int64_t capacity);

 protected:
    CacheMgr();

    virtual ~CacheMgr();

 protected:
    std::shared_ptr<Cache<ItemObj>> cache_;
};

}  // namespace cache
}  // namespace milvus

#include "cache/CacheMgr.inl"
