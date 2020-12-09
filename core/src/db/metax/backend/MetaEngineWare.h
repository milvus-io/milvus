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

#include "db/metax/MetaResField.h"
#include "utils/Status.h"

namespace milvus::engine::metax {

class MetaEngineWare {
 public:
    virtual ~MetaEngineWare() = default;

    virtual Status
    Insert(const MetaResFieldTuple& fields, snapshot::ID_TYPE& result_id) = 0;
};

using MetaEngineWarePtr = std::shared_ptr<MetaEngineWare>;

}  // namespace milvus::engine::metax
