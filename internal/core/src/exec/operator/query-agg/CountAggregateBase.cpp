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

#include "CountAggregateBase.h"

namespace milvus {
namespace exec {

void
registerCount(const std::string name) {
    std::vector<std::shared_ptr<expr::AggregateFunctionSignature>> signatures{
        expr::AggregateFunctionSignatureBuilder()
            .argumentType(DataType::INT64)
            .intermediateType(DataType::INT64)
            .returnType(DataType::INT64)
            .build()};

    exec::registerAggregateFunction(
        name,
        signatures,
        [name](plan::AggregationNode::Step /*step*/,
               const std::vector<DataType>& /*argumentTypes*/,
               const QueryConfig& /*config*/) -> std::unique_ptr<Aggregate> {
            return std::make_unique<CountAggregate>();
        });
}

void
registerCountAggregate(const std::string& prefix) {
    registerCount(prefix + KCount);
    LOG_INFO("Registered Count Aggregate Function");
}
}  // namespace exec
}  // namespace milvus
