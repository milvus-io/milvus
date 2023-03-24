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

#include <memory>
#include "common/Types.h"
#include "storage/FileManager.h"

namespace milvus::indexbuilder {
class IndexCreatorBase {
 public:
    virtual ~IndexCreatorBase() = default;

    virtual void
    Build(const milvus::DatasetPtr& dataset) = 0;

    virtual void
    Build() = 0;

    virtual milvus::BinarySet
    Serialize() = 0;

    // used for test.
    virtual void
    Load(const milvus::BinarySet&) = 0;

    virtual BinarySet
    Upload() = 0;
};

using IndexCreatorBasePtr = std::unique_ptr<IndexCreatorBase>;

}  // namespace milvus::indexbuilder
