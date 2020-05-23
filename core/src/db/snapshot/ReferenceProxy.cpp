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

#include "db/snapshot/ReferenceProxy.h"
#include <assert.h>
#include <iostream>

namespace milvus {
namespace engine {
namespace snapshot {

void
ReferenceProxy::Ref() {
    refcnt_ += 1;
    /* std::cout << this << " refcnt = " << refcnt_ << std::endl; */
}

void
ReferenceProxy::UnRef() {
    if (refcnt_ == 0)
        return;
    refcnt_ -= 1;
    /* std::cout << this << " refcnt = " << refcnt_ << std::endl; */
    if (refcnt_ == 0) {
        for (auto& cb : on_no_ref_cbs_) {
            cb();
        }
    }
}

void
ReferenceProxy::RegisterOnNoRefCB(OnNoRefCBF cb) {
    on_no_ref_cbs_.emplace_back(cb);
}

ReferenceProxy::~ReferenceProxy() {
    /* OnDeRef(); */
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
