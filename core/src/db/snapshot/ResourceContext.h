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

#include "db/impl/Context.h"
#include "db/snapshot/BaseResource.h"

namespace milvus::engine::snapshot {

template <typename ResourceT>
class ResourceContext {
 public:
    using ResPtr = typename ResourceT::Ptr;
    using Ptr = std::shared_ptr<ResourceContext>;

    class Builder {
     public:
        Builder&
        SetResource(typename ResourceT::Ptr res) {
            table_ = ResourceT::Name;
            id_ = res->GetID();
            resource_ = std::shared_ptr<ResourceT>(std::move(res));
            return *this;
        }

        Builder&
        SetOp(ResourceContextOp op) {
            op_ = op;
            return *this;
        }

        Builder&
        SetID(ID_TYPE id) {
            id_ = id;
            return *this;
        }

        Builder&
        SetTable(const std::string& table) {
            table_ = table;
            return *this;
        }

        Builder&
        AddAttr(const std::string& attr) {
            attrs_.insert(attr);
            return *this;
        }

        Builder&
        AddAttrs(const std::set<std::string>& attrs) {
            attrs_.insert(attrs.begin(), attrs.end());
            return *this;
        }

     public:
        ResourceContext::Ptr
        CreatePtr() {
            return std::make_shared<ResourceContext<ResourceT>>(table_, id_, op_, resource_, attrs_);
        }

     private:
        std::string table_;
        ResPtr resource_;
        ID_TYPE id_;
        SourceContextOp op_;
        std::set<std::string> attrs_;
    };

 public:
    ResourceContext(const std::string& table, ID_TYPE id, ResourceContextOp op, ResPtr res, std::set<std::string> attrs)
        : table_(table), id_(id), resource_(std::move(res)), op_(op), attrs_(std::move(attrs)) {
    }

    ~ResourceContext() = default;

 public:

    void
    AddResource(ResPtr res) {
        table_ = SourceT::Name;
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
    UpdateOp(const SourceContextOp op) {
        op_ = op;
    }

    SrcPtr
    Resource() {
        return resource_;
    }

    SourceContextOp
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
    SourceContextOp op_;
    std::set<std::string> attrs_;
};

template <typename ResourceT>
using ResourceContextPtr = ResourceContext<ResourceT>::Ptr;

}  // namespace milvus::engine::snapshot
