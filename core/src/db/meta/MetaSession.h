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
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/meta/MetaFieldHelper.h"
#include "db/meta/MetaFieldGS.h"
#include "db/meta/MetaFieldValueHelper.h"
#include "db/meta/MetaResourceAttrs.h"
#include "db/meta/backend/MetaEngine.h"
#include "db/meta/condition/MetaRelation.h"
#include "db/snapshot/BaseResource.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Utils.h"
#include "utils/Exception.h"
#include "utils/Json.h"
#include "utils/Status.h"

namespace milvus::engine::meta {
using snapshot::FETYPE_TYPE;
using snapshot::FTYPE_TYPE;

class MetaSession {
 public:
    explicit MetaSession(MetaEnginePtr engine) : db_engine_(engine), pos_(-1) {
    }

    ~MetaSession() = default;

 public:
    template <typename ResourceT, typename U>
    Status
    Select(const std::string& field, const std::vector<U>& value, const std::vector<std::string>& target_attrs,
           std::vector<typename ResourceT::Ptr>& resources);

    template <typename T>
    Status
    Query(const MetaCombinationPtr filter, std::vector<typename T::Ptr>& resources) {
        MetaFilterContext context;
        context.table_ = T::Name;
        context.combination_ = filter;
        AttrsMapList attrs;
        auto status = db_engine_->Filter(context, attrs);

        if (status.ok()) {
            for (auto raw : attrs) {
                auto resource = snapshot::CreateResPtr<T>();
                status = AttrMap2Resource<T>(raw, resource);
                if (!status.ok()) {
                    resources.clear();
                    return status;
                }
                resources.push_back(std::move(resource));
            }
        }

        return status;
    }

    template <typename ResourceT>
    Status
    Apply(snapshot::ResourceContextPtr<ResourceT> resp);

    Status
    ResultPos() {
        if (apply_context_.empty()) {
            return Status(SERVER_UNEXPECTED_ERROR, "Session is empty");
        }
        pos_ = apply_context_.size() - 1;

        return Status::OK();
    }

    Status
    Commit(std::vector<int64_t>& result_ids) {
        return db_engine_->ExecuteTransaction(apply_context_, result_ids);
    }

    Status
    Commit(int64_t& result_id) {
        if (apply_context_.empty()) {
            return Status::OK();
        }

        if (pos_ < 0) {
            throw Exception(1, "Result pos is small than 0");
            //            return Status(SERVER_UNEXPECTED_ERROR, "Result pos is small than 0");
        }
        std::vector<int64_t> result_ids;
        auto status = db_engine_->ExecuteTransaction(apply_context_, result_ids);
        if (!status.ok()) {
            return status;
        }

        result_id = result_ids.at(pos_);
        return Status::OK();
    }

 private:
    std::vector<MetaApplyContext> apply_context_;
    int64_t pos_;
    MetaEnginePtr db_engine_;
};

template <typename T, typename U>
Status
MetaSession::Select(const std::string& field, const std::vector<U>& values,
                    const std::vector<std::string>& target_attrs, std::vector<typename T::Ptr>& resources) {
    MetaQueryContext context;
    context.table_ = T::Name;

    if (!field.empty()) {
        std::vector<std::string> field_values;
        for (auto& v : values) {
            field_values.push_back(FieldValue2Str(v));
        }
        context.filter_attrs_ = {{field, field_values}};
    }

    if (!target_attrs.empty()) {
        context.all_required_ = false;
        context.query_fields_ = target_attrs;
    }

    AttrsMapList attrs;
    auto status = db_engine_->Query(context, attrs);
    if (!status.ok()) {
        return status;
    }

    if (attrs.empty()) {
        return Status::OK();
    }

    for (auto raw : attrs) {
        auto resource = snapshot::CreateResPtr<T>();
        AttrMap2Resource<T>(raw, resource);
        resources.push_back(std::move(resource));
    }

    return Status::OK();
}

template <typename ResourceT>
Status
MetaSession::Apply(snapshot::ResourceContextPtr<ResourceT> resp) {
    // TODO: may here not need to store resp
    auto status = Status::OK();
    std::string sql;

    MetaApplyContext context;
    context.op_ = resp->Op();
    if (context.op_ == oAdd) {
        status = ResourceContextAddAttrMap<ResourceT>(resp, context.attrs_);
    } else if (context.op_ == oUpdate) {
        status = ResourceContextUpdateAttrMap<ResourceT>(resp, context.attrs_);
        context.id_ = resp->Resource()->GetID();
    } else if (context.op_ == oDelete) {
        context.id_ = resp->ID();
    } else {
        return Status(SERVER_UNEXPECTED_ERROR, "Unknown resource context operation");
    }

    if (!status.ok()) {
        return status;
    }

    context.table_ = resp->Table();
    apply_context_.push_back(context);

    return Status::OK();
}

using SessionPtr = std::shared_ptr<MetaSession>;

}  // namespace milvus::engine::meta
