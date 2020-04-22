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

namespace milvus {

/**
 * @brief Entity inserted, currently each entity represent a vector
 */
struct Entity {
    std::vector<float> float_data;     ///< Vector raw float data
    std::vector<uint8_t> binary_data;  ///< Vector raw binary data
};


// base class of all queries
struct Sort {
    std::string field_name;
    int64_t rules;      // 0 is inc, 1 is dec
};

struct Query {
    std::string field_name;
    int64_t from;
    int64_t size;
    Sort sort;
    float min_score;
    float boost;
};

enum class CompareOperator {
    LT = 0,
    LTE,
    EQ,
    GT,
    GTE,
    NE,
};

struct QueryColumn {
    std::string name;
    std::string column_value;
};

struct TermQuery : Query {
    std::vector<int8_t> field_value;
};
using TermQueryPtr = std::shared_ptr<TermQuery>;

struct CompareExpr {
    CompareOperator compare_operator;
    std::string operand;
};

struct RangeQuery : Query {
    std::vector<CompareExpr> compare_expr;
};
using RangeQueryPtr = std::shared_ptr<RangeQuery>;

struct RowRecord {
    std::vector<float> float_data;
    std::vector<uint8_t> binary_data;
};

struct VectorQuery : Query {
    uint64_t topk;
    float distance_limitation;
    float query_boost;
    std::vector<Entity> query_vector;
    std::string extra_params;
};
using VectorQueryPtr = std::shared_ptr<VectorQuery>;

struct LeafQuery {
    TermQueryPtr term_query_ptr;
    RangeQueryPtr range_query_ptr;
    VectorQueryPtr vector_query_ptr;
    float query_boost;
};
using LeafQueryPtr = std::shared_ptr<LeafQuery>;

}