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

#include <iostream>
#include <memory>
#include <vector>
#include <string>

# include "GeneralQuery.h"

namespace milvus {

enum class Occur {
    INVALID = 0,
    MUST,
    MUST_NOT,
    SHOULD,
};

class BooleanQuery {
 public:
    BooleanQuery() {}

    explicit BooleanQuery(Occur occur) : occur_(occur) {}

    void
    AddLeafQuery(LeafQueryPtr leaf_query) {
        leaf_queries_.emplace_back(leaf_query);
    }

    void
    AddBooleanQuery(std::shared_ptr<BooleanQuery> boolean_query) {
        boolean_queries_.emplace_back(boolean_query);
    }

    std::vector<std::shared_ptr<BooleanQuery>>&
    GetBooleanQueries() {
        return boolean_queries_;
    }

    std::vector<LeafQueryPtr>&
    GetLeafQueries() {
        return leaf_queries_;
    }

    Occur
    GetOccur() {
        return occur_;
    }

 private:
    Occur occur_;
    std::vector<std::shared_ptr<BooleanQuery>> boolean_queries_;
    std::vector<LeafQueryPtr> leaf_queries_;
};
using BooleanQueryPtr = std::shared_ptr<BooleanQuery>;

} // namespace milvus
