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

#include "db/meta/condition/MetaBaseCondition.h"

namespace milvus::engine::meta {

enum Comb { and_, or_, one_ };

class MetaBaseCombination : public MetaBaseCondition {
 public:
    explicit MetaBaseCombination(Comb cond) : cond_(cond) {
    }

    ~MetaBaseCombination() override = default;

 protected:
    std::string
    Relation() const {
        switch (cond_) {
            case and_:
                return "AND";
            case or_:
                return "OR";
            default:
                return "";
        }
    }

 protected:
    Comb cond_;
};

using MetaCombinationPtr = std::shared_ptr<MetaBaseCombination>;

}  // namespace milvus::engine::meta
