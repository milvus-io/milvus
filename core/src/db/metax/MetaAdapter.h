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

#include "db/metax/MetaProxy.h"
#include "db/metax/MetaResField.h"
#include "db/metax/MetaResFieldHelper.h"
#include "db/metax/MetaTraits.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Resources.h"

#include "utils/Exception.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

class MetaAdapter {
 public:
    explicit MetaAdapter(MetaProxyPtr proxy) : proxy_(proxy) {
    }

    template <typename R/*, typename std::enable_if<is_decay_base_of_v<snapshot::BaseResource<R>, R>>::type* = nullptr*/>
    Status
    Insert(std::shared_ptr<R> res, snapshot::ID_TYPE& result_id) {
        static_assert(is_decay_base_of_v<snapshot::BaseResource<R>, R>);
        auto fields = GenFieldTupleFromRes<R>(res);
        return proxy_->Insert(fields, result_id);
    }

    template <typename... Args>
    Status
    Select(Args... args) {
        return proxy_->Select(std::forward<Args>(args)...);
    }

 private:
    MetaProxyPtr proxy_;
};

using MetaAdapterPtr = std::shared_ptr<MetaAdapter>;

}  // namespace milvus::engine::metax
