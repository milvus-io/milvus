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
#include <any>
#include <functional>
#include <memory>
#include <vector>

namespace milvus {
namespace engine {
namespace snapshot {

using OnNoRefCBF = std::function<void(void)>;

class ReferenceProxy {
 public:
    void
    RegisterOnNoRefCB(OnNoRefCBF cb);

    virtual void
    Ref();
    virtual void
    UnRef();

    int
    RefCnt() const {
        return refcnt_;
    }

    void
    ResetCnt() {
        refcnt_ = 0;
    }

    virtual ~ReferenceProxy();

 protected:
    int refcnt_ = 0;
    std::vector<OnNoRefCBF> on_no_ref_cbs_;
};

using ReferenceResourcePtr = std::shared_ptr<ReferenceProxy>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
