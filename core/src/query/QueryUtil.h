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

#include "BooleanQuery.h"

namespace milvus {
namespace query {

class QueryUtil {
 public:
    static BinaryQueryPtr
    ConstructBinTree(std::vector<BooleanQueryPtr>& clauses, QueryRelation relation, uint64_t idx);

    static Status
    ConstructLeafBinTree(std::vector<LeafQueryPtr>& leaf_clauses, const BinaryQueryPtr& binary_query, uint64_t idx);

    static Status
    GenBinaryQuery(const BooleanQueryPtr& clause, BinaryQueryPtr& binary_query);

    static uint64_t
    BinaryQueryHeight(BinaryQueryPtr& binary_query);

    static Status
    ValidateBooleanQuery(BooleanQueryPtr& boolean_query);

    static bool
    ValidateBinaryQuery(BinaryQueryPtr& binary_query);

    /**
     * rules:
     * 1. The child node of 'should' and 'must_not' can only be 'term query' and 'range query'.
     * 2. One layer cannot include bool query and leaf query.
     * 3. The direct child node of 'bool' node cannot be 'should' node or 'must_not' node.
     * 4. All filters are pre-filtered(Do structure query first, then use the result to do filtering for vector query).
     *
     */
    static Status
    rule_1(BooleanQueryPtr& boolean_query, std::vector<BooleanQueryPtr>& paths);

    static Status
    rule_2(BooleanQueryPtr& boolean_query);
};

}  // namespace query
}  // namespace milvus
