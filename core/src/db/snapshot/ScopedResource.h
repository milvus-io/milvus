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

namespace milvus {
namespace engine {
namespace snapshot {

template <typename ResourceT>
class ScopedResource {
 public:
    using ThisT = ScopedResource<ResourceT>;
    using Ptr = std::shared_ptr<ThisT>;
    using ResourcePtr = std::shared_ptr<ResourceT>;
    ScopedResource();
    explicit ScopedResource(ResourcePtr res, bool scoped = true);

    ScopedResource(const ScopedResource<ResourceT>& res);

    ScopedResource<ResourceT>&
    operator=(const ScopedResource<ResourceT>& res);

    ResourcePtr
    Get() {
        return res_;
    }
    const ResourcePtr&
    Get() const {
        return res_;
    }

    ResourceT operator*() const {
        return *res_;
    }
    ResourcePtr operator->() const {
        return res_;
    }

    operator bool() const {
        return (res_ != nullptr);
    }

    ~ScopedResource();

 protected:
    ResourcePtr res_;
    bool scoped_;
};

template <typename ResourceT>
ScopedResource<ResourceT>::ScopedResource() : res_(nullptr), scoped_(false) {
}

template <typename ResourceT>
ScopedResource<ResourceT>::ScopedResource(ScopedResource<ResourceT>::ResourcePtr res, bool scoped)
    : res_(res), scoped_(scoped) {
    if (scoped) {
        /* std::cout << "Do Ref" << std::endl; */
        res_->Ref();
    }
}

template <typename ResourceT>
ScopedResource<ResourceT>&
ScopedResource<ResourceT>::operator=(const ScopedResource<ResourceT>& res) {
    if (this->res_ == res.res_)
        return *this;
    if (scoped_) {
        res_->UnRef();
    }
    res_ = res.res_;
    scoped_ = res.scoped_;
    if (scoped_) {
        res_->Ref();
    }
    return *this;
}

template <typename ResourceT>
ScopedResource<ResourceT>::ScopedResource(const ScopedResource<ResourceT>& res) {
    res_ = res.res_;
    if (res.scoped_) {
        res_->Ref();
    }
    scoped_ = res.scoped_;
}

template <typename ResourceT>
ScopedResource<ResourceT>::~ScopedResource() {
    if (scoped_) {
        /* std::cout << "Do UnRef" << std::endl; */
        res_->UnRef();
    }
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
