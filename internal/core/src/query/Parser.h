// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "Plan.h"

namespace milvus::query {

class Parser {
 public:
    friend std::unique_ptr<Plan>
    CreatePlan(const Schema& schema, const std::string& dsl_str);

 private:
    std::unique_ptr<Plan>
    CreatePlanImpl(const Json& dsl);

    explicit Parser(const Schema& schema) : schema(schema) {
    }

    // vector node parser, should be called exactly once per pass.
    std::unique_ptr<VectorPlanNode>
    ParseVecNode(const Json& out_body);

    // Dispatcher of all parse function
    // NOTE: when nullptr, it is a pure vector node
    ExprPtr
    ParseAnyNode(const Json& body);

    ExprPtr
    ParseMustNode(const Json& body);

    ExprPtr
    ParseShouldNode(const Json& body);

    ExprPtr
    ParseMustNotNode(const Json& body);

    // parse the value of "should"/"must"/"must_not" entry
    std::vector<ExprPtr>
    ParseItemList(const Json& body);

    // parse the value of "range" entry
    ExprPtr
    ParseRangeNode(const Json& out_body);

    // parse the value of "term" entry
    ExprPtr
    ParseTermNode(const Json& out_body);

    // parse the value of "term" entry
    ExprPtr
    ParseCompareNode(const Json& out_body);

 private:
    // template implementation of leaf parser
    // used by corresponding parser

    template <typename T>
    ExprPtr
    ParseRangeNodeImpl(const FieldName& field_name, const Json& body);

    template <typename T>
    ExprPtr
    ParseTermNodeImpl(const FieldName& field_name, const Json& body);

 private:
    const Schema& schema;
    std::map<std::string, FieldId> tag2field_;  // PlaceholderName -> field id
    std::optional<std::unique_ptr<VectorPlanNode>> vector_node_opt_;
};

}  // namespace milvus::query
