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
#include <type_traits>
#include <vector>

#include "db/meta/MetaSession.h"
#include "db/snapshot/Resources.h"
#include "utils/Exception.h"

namespace milvus::engine::meta {

class MetaAdapter {
 public:
    explicit MetaAdapter(MetaEnginePtr engine) : engine_(engine) {
    }

    SessionPtr
    CreateSession() {
        return std::make_shared<MetaSession>(engine_);
    }

    template <typename T>
    Status
    Select(int64_t id, typename T::Ptr& resource) {
        // TODO move select logic to here
        auto session = CreateSession();
        std::vector<typename T::Ptr> resources;
        auto status = session->Select<T, snapshot::ID_TYPE>(snapshot::IdField::Name, {id}, {}, resources);
        if (status.ok() && !resources.empty()) {
            // TODO: may need to check num of resources
            resource = resources.at(0);
        }

        return status;
    }

    template <typename ResourceT, typename U>
    Status
    SelectBy(const std::string& field, const std::vector<U>& values, std::vector<typename ResourceT::Ptr>& resources) {
        auto session = CreateSession();
        return session->Select<ResourceT, U>(field, values, {}, resources);
    }

    template <typename ResourceT, typename U>
    Status
    SelectResourceIDs(std::vector<int64_t>& ids, const std::string& filter_field, const std::vector<U>& filter_values) {
        std::vector<typename ResourceT::Ptr> resources;
        auto session = CreateSession();
        auto status = session->Select<ResourceT, U>(filter_field, filter_values, {F_ID}, resources);
        if (!status.ok()) {
            return status;
        }

        for (auto& res : resources) {
            ids.push_back(res->GetID());
        }

        return Status::OK();
    }

    template <typename ResourceT>
    Status
    Apply(snapshot::ResourceContextPtr<ResourceT> resp, int64_t& result_id) {
        auto session = CreateSession();
        STATUS_CHECK(session->Apply<ResourceT>(resp));

        std::vector<int64_t> result_ids;
        STATUS_CHECK(session->Commit(result_ids));

        if (result_ids.size() != 1) {
            return Status(DB_ERROR, "Result id is wrong");
        }

        result_id = result_ids.at(0);
        return Status::OK();
    }

    Status
    TruncateAll() {
        return engine_->TruncateAll();
    }

 private:
    MetaEnginePtr engine_;
};

using MetaAdapterPtr = std::shared_ptr<MetaAdapter>;

}  // namespace milvus::engine::meta
