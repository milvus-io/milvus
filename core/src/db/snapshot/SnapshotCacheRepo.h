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

#include "db/snapshot/CacheRepo.h"
#include "db/snapshot/SnapshotHolder.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename T>
class SnapshotHolderCacheHelper {
 public:
     using Cache = CacheRepo<T, ID_TYPE, ID_TYPE>;
     using ThisT = SnapshotHolderCacheHelper<T>;
     struct HookRegistar {
        HookRegistar() {
            SnapshotHolder::RegisterHooker(ThisT::Hooker);
        }
     };

     static void
     Hooker(Snapshot::Ptr ss);
};

template <typename T>
void
SnapshotHolderCacheHelper<T>::Hooker(Snapshot::Ptr ss) {
    std::cout << "Clear SS-" << ss->GetID() << std::endl;
    std::cout << "PRE INDEX_SIZE=" << Cache::IndexSize() << std::endl;
    Cache::Clear(ss->GetID());
    std::cout << "POST INDEX_SIZE=" << Cache::IndexSize() << std::endl;
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
