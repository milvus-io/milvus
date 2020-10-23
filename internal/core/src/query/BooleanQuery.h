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
#include <vector>

#include "GeneralQuery.h"
#include "utils/Status.h"

namespace milvus {
namespace query_old {

enum class Occur {
    INVALID = 0,
    MUST,
    MUST_NOT,
    SHOULD,
};

class BooleanQuery {
 public:
    BooleanQuery() {
    }

    explicit BooleanQuery(Occur occur) : occur_(occur) {
    }

    Occur
    getOccur() {
        return occur_;
    }

    void
    SetOccur(Occur occur) {
        occur_ = occur;
    }

    void
    AddBooleanQuery(std::shared_ptr<BooleanQuery> boolean_clause) {
        boolean_clauses_.emplace_back(boolean_clause);
    }

    void
    AddLeafQuery(LeafQueryPtr leaf_query) {
        leaf_queries_.emplace_back(leaf_query);
    }

    void
    SetLeafQuery(std::vector<LeafQueryPtr> leaf_queries) {
        leaf_queries_ = leaf_queries;
    }

    std::vector<std::shared_ptr<BooleanQuery>>
    getBooleanQueries() {
        return boolean_clauses_;
    }

    BinaryQueryPtr&
    getBinaryQuery() {
        return binary_query_;
    }

    std::vector<LeafQueryPtr>&
    getLeafQueries() {
        return leaf_queries_;
    }

 private:
    Occur occur_ = Occur::INVALID;
    std::vector<std::shared_ptr<BooleanQuery>> boolean_clauses_;
    std::vector<LeafQueryPtr> leaf_queries_;
    BinaryQueryPtr binary_query_ = std::make_shared<BinaryQuery>();
};
using BooleanQueryPtr = std::shared_ptr<BooleanQuery>;

}  // namespace query_old
}  // namespace milvus
