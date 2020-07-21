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

#include <memory>
#include <string>
#include <vector>

#include "db/snapshot/Event.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Store.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

class InActiveResourcesGCEvent : public MetaEvent, public Operations {
 public:
    using Ptr = std::shared_ptr<InActiveResourcesGCEvent>;

    InActiveResourcesGCEvent() : Operations(OperationContext(), ScopedSnapshotT(), OperationsType::O_Leaf) {
    }

    ~InActiveResourcesGCEvent() = default;

    Status
    Process(StorePtr store) override {
        return store->Apply(*this);
    }

    Status
    OnExecute(StorePtr store) override {
        std::cout << "Executing InActiveResourcesGCEvent" << std::endl;
        return Status::OK();
    }
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
