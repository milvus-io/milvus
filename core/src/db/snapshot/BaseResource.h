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
#include <sstream>
#include <string>

#include "ReferenceProxy.h"

namespace milvus::engine::snapshot {

template <typename DerivedT>
class BaseResource : public ReferenceProxy {
 public:
    virtual std::string
    ToString() const {
        std::stringstream ss;
        const DerivedT& derived = static_cast<const DerivedT&>(*this);
        ss << DerivedT::Name << ": id=" << derived.GetID() << " state=" << derived.GetState();
        return ss.str();
    }
};

}  // namespace milvus::engine::snapshot
