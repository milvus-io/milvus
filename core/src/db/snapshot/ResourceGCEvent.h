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
#include <set>
#include <string>

#include "db/snapshot/MetaEvent.h"
#include "db/snapshot/Operations.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Store.h"
#include "utils/Status.h"

namespace milvus::engine::snapshot {

template <class ResourceT>
class ResourceGCEvent : public GCEvent {
 public:
    using Ptr = std::shared_ptr<ResourceGCEvent>;

    explicit ResourceGCEvent(typename ResourceT::Ptr res) : res_(res) {
    }

    ~ResourceGCEvent() = default;

    Status
    Process(StorePtr store) override {
        /* mark resource as 'deleted' in meta */
        auto sd_op = std::make_shared<SoftDeleteOperation<ResourceT>>(res_->GetID());
        STATUS_CHECK((*sd_op)(store));

        /* TODO: physically clean resource */
        auto res_prefix = store->GetRootPath();
        std::string res_path = GetResPath<ResourceT>(res_prefix, res_);
        auto do_remove = [&](bool do_throw) -> Status {
            try {
                if (res_path.empty()) {
                    /* std::cout << "[GC] No remove action for " << res_->ToString() << std::endl; */
                } else if (std::experimental::filesystem::is_directory(res_path)) {
                    LOG_ENGINE_DEBUG_ << "[GC] Remove DIR " << res_->ToString() << " " << res_path;
                    auto ok = std::experimental::filesystem::remove_all(res_path);
                } else if (std::experimental::filesystem::is_regular_file(res_path)) {
                    LOG_ENGINE_DEBUG_ << "[GC] Remove FILE " << res_->ToString() << " " << res_path;
                    auto ok = std::experimental::filesystem::remove(res_path);
                } else {
                    RemoveWithSuffix<ResourceT>(res_, res_path, store->GetSuffixSet());
                }
            } catch (const std::experimental::filesystem::filesystem_error& er) {
                LOG_SERVER_ERROR_ << "[GC] Error when removing path " << res_path << ": " << er.what();
                if (do_throw) {
                    throw;
                }
                return Status(DB_ERROR, er.what());
            }
            return Status::OK();
        };
        auto retry = 3;
        Status status;
        while (--retry >= 0) {
            status = do_remove(false);
            if (status.ok()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            LOG_ENGINE_DEBUG_ << "[GC] Retry remove: " << res_path;
        }

        auto hard_delete_meta = [](ID_TYPE id, StorePtr store) -> Status {
            auto hd_op = std::make_shared<HardDeleteOperation<ResourceT>>(id);
            return ((*hd_op)(store));
        };

        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "[GC] Stale Resource: " << res_path << " need to be cleanup later";
            return status;
        }

        STATUS_CHECK(hard_delete_meta(res_->GetID(), store));

        return Status::OK();
    }

 private:
    typename ResourceT::Ptr res_;
};

}  // namespace milvus::engine::snapshot
