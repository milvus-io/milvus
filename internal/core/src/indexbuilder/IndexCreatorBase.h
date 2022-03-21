// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include "knowhere/common/Dataset.h"
#include "knowhere/common/BinarySet.h"
#include <memory>
#include <knowhere/index/Index.h>

namespace milvus::indexbuilder {
class IndexCreatorBase {
 public:
    virtual ~IndexCreatorBase() = default;

    virtual void
    Build(const knowhere::DatasetPtr& dataset) = 0;

    virtual knowhere::BinarySet
    Serialize() = 0;

    virtual void
    Load(const knowhere::BinarySet&) = 0;

    // virtual knowhere::IndexPtr
    // GetIndex() = 0;
};

using IndexCreatorBasePtr = std::unique_ptr<IndexCreatorBase>;

}  // namespace milvus::indexbuilder
