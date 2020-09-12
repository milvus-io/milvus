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
#include <utility>

#include "db/meta/backend/MetaContext.h"
#include "db/snapshot/BaseResource.h"
#include "db/snapshot/ResourceTypes.h"

namespace milvus::engine::snapshot {

using ResourceContextOp = meta::MetaContextOp;

template <typename ResourceT>
class ResourceContext {
 public:
    using ResPtr = typename ResourceT::Ptr;
    using Ptr = std::shared_ptr<ResourceContext<ResourceT>>;

 public:
    ResourceContext(const std::string& table, ID_TYPE id, ResourceContextOp op, ResPtr res, std::set<std::string> attrs)
        : table_(table), id_(id), resource_(std::move(res)), op_(op), attrs_(std::move(attrs)) {
    }

    ~ResourceContext() = default;

 public:
    void
    AddResource(ResPtr res) {
        table_ = ResourceT::Name;
        resource_ = std::shared_ptr<ResourceT>(std::move(res));
    }

    void
    AddAttr(const std::string& attr) {
        attrs_.insert(attr);
    }

    void
    AddAttrs(const std::set<std::string>& attrs) {
        attrs_.insert(attrs.begin(), attrs.end());
    }

    void
    UpdateOp(const ResourceContextOp op) {
        op_ = op;
    }

    ResPtr
    Resource() {
        return resource_;
    }

    ResourceContextOp
    Op() {
        return op_;
    }

    ID_TYPE
    ID() const {
        return id_;
    }

    std::set<std::string>&
    Attrs() {
        return attrs_;
    }

    std::string
    Table() {
        return table_;
    }

 private:
    std::string table_;
    ID_TYPE id_;
    ResPtr resource_;
    ResourceContextOp op_;
    std::set<std::string> attrs_;
};

template <typename T>
class ResourceContextBuilder {
 public:
    ResourceContextBuilder() : table_(T::Name), op_(meta::oAdd) {
    }

    ResourceContextBuilder<T>&
    SetResource(typename T::Ptr res) {
        table_ = T::Name;
        id_ = res->GetID();
        resource_ = std::move(res);
        return *this;
    }

    ResourceContextBuilder<T>&
    SetOp(ResourceContextOp op) {
        op_ = op;
        return *this;
    }

    ResourceContextBuilder<T>&
    SetID(ID_TYPE id) {
        id_ = id;
        return *this;
    }

    ResourceContextBuilder<T>&
    SetTable(const std::string& table) {
        table_ = table;
        return *this;
    }

    ResourceContextBuilder<T>&
    AddAttr(const std::string& attr) {
        attrs_.insert(attr);
        return *this;
    }

    ResourceContextBuilder<T>&
    AddAttrs(const std::set<std::string>& attrs) {
        attrs_.insert(attrs.begin(), attrs.end());
        return *this;
    }

 public:
    typename ResourceContext<T>::Ptr
    CreatePtr() {
        return std::make_shared<ResourceContext<T>>(table_, id_, op_, resource_, attrs_);
    }

 private:
    std::string table_;
    typename ResourceContext<T>::ResPtr resource_;
    ID_TYPE id_{};
    ResourceContextOp op_;
    std::set<std::string> attrs_;
};

template <typename ResourceT>
using ResourceContextPtr = typename ResourceContext<ResourceT>::Ptr;

}  // namespace milvus::engine::snapshot
