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

#include <experimental/filesystem>
#include <memory>
#include <string>
#include <vector>

#include "db/snapshot/MetaEvent.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Store.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

class InActiveResourcesGCEvent : public GCEvent, public Operations {
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

        STATUS_CHECK(ClearInActiveResources<Collection>(store));
        STATUS_CHECK(ClearInActiveResources<CollectionCommit>(store));
        STATUS_CHECK(ClearInActiveResources<Partition>(store));
        STATUS_CHECK(ClearInActiveResources<PartitionCommit>(store));
        STATUS_CHECK(ClearInActiveResources<Segment>(store));
        STATUS_CHECK(ClearInActiveResources<SegmentCommit>(store));
        STATUS_CHECK(ClearInActiveResources<SegmentFile>(store));
        STATUS_CHECK(ClearInActiveResources<SchemaCommit>(store));
        STATUS_CHECK(ClearInActiveResources<Field>(store));
        STATUS_CHECK(ClearInActiveResources<FieldCommit>(store));
        STATUS_CHECK(ClearInActiveResources<FieldElement>(store));

        return Status::OK();
    }

 private:
    template <typename ResourceT>
    Status
    ClearInActiveResources(StorePtr store) {
        std::vector<typename ResourceT::Ptr> resources;
        STATUS_CHECK(store->GetInActiveResources<ResourceT>(resources));

        for (auto& res : resources) {
            std::string res_path = GetResPath<ResourceT>(dir_root_, res);
            if (res_path.empty()) {
                /* std::cout << "[GC] No remove action for " << res_->ToString() << std::endl; */
            } else if (std::experimental::filesystem::is_directory(res_path)) {
                auto ok = std::experimental::filesystem::remove_all(res_path);
                /* std::cout << "[GC] Remove dir " << res_->ToString() << " " << res_path << " " << ok << std::endl; */
            } else if (std::experimental::filesystem::is_regular_file(res_path)) {
                auto ok = std::experimental::filesystem::remove(res_path);
                /* std::cout << "[GC] Remove file " << res_->ToString() << " " << res_path << " " << ok << std::endl; */
            } else {
                RemoveWithSuffix<ResourceT>(res, res_path, store->GetSuffixSet());
                std::cout << "[GC] Remove stale " << res_path << " for " << res->ToString() << std::endl;
            }

            /* remove resource from meta */
            auto hd_op = std::make_shared<HardDeleteOperation<ResourceT>>(res->GetID());
            STATUS_CHECK((*hd_op)(store));
        }

        return Status::OK();
    }

 private:
    std::string dir_root_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
