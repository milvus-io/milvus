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

#include <experimental/filesystem>

#include "db/snapshot/Store.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

class MetaEvent {
 public:
    virtual Status Process(StorePtr) = 0;
};

class GCEvent : virtual public MetaEvent {
 protected:
    template <class ResourceT>
    void
    RemoveWithSuffix(typename ResourceT::Ptr res, const std::string& path, const std::set<std::string>& suffix_set) {
        for (auto& suffix : suffix_set) {
            if (suffix.empty()) {
                continue;
            }
            auto adjusted = path + suffix;
            if (std::experimental::filesystem::is_regular_file(adjusted)) {
                auto ok = std::experimental::filesystem::remove(adjusted);
                LOG_ENGINE_DEBUG_ << "[GC] Remove FILE " << res->ToString() << " " << adjusted << " " << ok;
                return;
            }
        }
        LOG_ENGINE_DEBUG_ << "[GC] Remove STALE OBJECT " << path << " for " << res->ToString();
    }
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
