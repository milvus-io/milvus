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
#include <string>
#include <vector>
#include "utils/Json.h"

namespace milvus {
namespace query {

enum class CompareOperator {
    LT = 0,
    LTE,
    EQ,
    GT,
    GTE,
    NE,
};

enum class QueryRelation {
    INVALID = 0,
    R1,
    R2,
    R3,
    R4,
    AND,
    OR,
};

struct QueryColumn {
    std::string name;
    std::string column_value;
};

struct TermQuery {
    std::string field_name;
    std::vector<std::string> field_value;
    float boost;
};
using TermQueryPtr = std::shared_ptr<TermQuery>;

struct CompareExpr {
    CompareOperator compare_operator;
    std::string operand;
};

struct RangeQuery {
    std::string field_name;
    std::vector<CompareExpr> compare_expr;
    float boost;
};
using RangeQueryPtr = std::shared_ptr<RangeQuery>;

struct VectorRecord {
    std::vector<float> float_data;
    std::vector<uint8_t> binary_data;
};

struct VectorQuery {
    std::string field_name;
    milvus::json extra_params;
    int64_t topk;
    float boost;
    VectorRecord query_vector;
};
using VectorQueryPtr = std::shared_ptr<VectorQuery>;

struct LeafQuery;
using LeafQueryPtr = std::shared_ptr<LeafQuery>;

struct BinaryQuery;
using BinaryQueryPtr = std::shared_ptr<BinaryQuery>;

struct GeneralQuery {
    LeafQueryPtr leaf;
    BinaryQueryPtr bin = std::make_shared<BinaryQuery>();
};
using GeneralQueryPtr = std::shared_ptr<GeneralQuery>;

struct LeafQuery {
    TermQueryPtr term_query;
    RangeQueryPtr range_query;
    VectorQueryPtr vector_query;
    float query_boost;
};

struct BinaryQuery {
    GeneralQueryPtr left_query;
    GeneralQueryPtr right_query;
    QueryRelation relation;
    float query_boost;
};

}  // namespace query
}  // namespace milvus
