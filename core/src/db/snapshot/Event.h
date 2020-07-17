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

#include <boost/filesystem.hpp>
#include <memory>
#include <string>
#include <vector>

#include "db/snapshot/Operations.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Store.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

class MetaEvent {
 public:
    virtual Status
    Process(StorePtr) = 0;
};

template <class ResourceT>
class ResourceGCEvent : public MetaEvent {
 public:
    using Ptr = std::shared_ptr<ResourceGCEvent>;

    explicit ResourceGCEvent(class ResourceT::Ptr res) : res_(res) {
    }

    ~ResourceGCEvent() = default;

    Status
    Process(StorePtr store) override {
        /* mark resource as 'deleted' in meta */
        auto sd_op = std::make_shared<SoftDeleteOperation<ResourceT>>(res_->GetID());
        STATUS_CHECK((*sd_op)(store));

        /* TODO: physically clean resource */
        std::string res_path = GetResPath<ResourceT>(dir_root_, res_);
        /* if (!boost::filesystem::exists(res_path)) { */
        /*     return Status::OK(); */
        /* } */
        if (boost::filesystem::is_directory(res_path)) {
            boost::filesystem::remove_all(res_path);
        } else {
            boost::filesystem::remove(res_path);
        }

        /* remove resource from meta */
        auto hd_op = std::make_shared<HardDeleteOperation<ResourceT>>(res_->GetID());
        STATUS_CHECK((*hd_op)(store));

        return Status::OK();
    }

 private:
    class ResourceT::Ptr res_;
    std::string dir_root_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
