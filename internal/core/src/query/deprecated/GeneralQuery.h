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
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Types.h"
#include "utils/Json.h"

namespace milvus {
namespace query_old {

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
    milvus::json json_obj;
    //    std::string field_name;
    //    std::vector<uint8_t> field_value;
    //    float boost;
};
using TermQueryPtr = std::shared_ptr<TermQuery>;

struct CompareExpr {
    CompareOperator compare_operator;
    std::string operand;
};

struct RangeQuery {
    milvus::json json_obj;
    //    std::string field_name;
    //    std::vector<CompareExpr> compare_expr;
    //    float boost;
};
using RangeQueryPtr = std::shared_ptr<RangeQuery>;

struct VectorRecord {
    size_t vector_count;
    std::vector<float> float_data;
    std::vector<uint8_t> binary_data;
};

struct VectorQuery {
    std::string field_name;
    milvus::json extra_params = {};
    int64_t topk;
    int64_t nq;
    std::string metric_type = "";
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
    std::string vector_placeholder;
    float query_boost;
};

struct BinaryQuery {
    GeneralQueryPtr left_query;
    GeneralQueryPtr right_query;
    QueryRelation relation;
    float query_boost;
    bool is_not = false;
};

struct Query {
    GeneralQueryPtr root;
    std::unordered_map<std::string, VectorQueryPtr> vectors;

    std::string collection_id;
    std::vector<std::string> partitions;
    std::vector<std::string> field_names;
    std::set<std::string> index_fields;
    std::unordered_map<std::string, std::string> metric_types;
    std::string index_type;
};

using QueryPtr = std::shared_ptr<Query>;

}  // namespace query_old

namespace query {
struct QueryDeprecated {
    int64_t num_queries;                //
    int topK;                           // topK of queries
    std::string field_name;             // must be fakevec, whose data_type must be VEC_FLOAT(DIM)
    std::vector<float> query_raw_data;  // must be size of num_queries * DIM
};

// std::unique_ptr<Query> CreateNaiveQueryPtr(int64_t num_queries, int topK, std::string& field_name, const float*
// raw_data) {
//    return std::
//}

using QueryDeprecatedPtr = std::shared_ptr<QueryDeprecated>;
}  // namespace query
}  // namespace milvus
