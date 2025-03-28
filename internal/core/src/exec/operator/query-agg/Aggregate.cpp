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
#include "Aggregate.h"
#include "exec/expression/Utils.h"

namespace milvus {
namespace exec {

void
Aggregate::setOffsetsInternal(int32_t offset,
                              int32_t nullByte,
                              uint8_t nullMask,
                              int32_t initializedByte,
                              uint8_t initializedMask,
                              int32_t rowSizeOffset) {
    offset_ = offset;
    nullByte_ = nullByte;
    nullMask_ = nullMask;
    initializedByte_ = initializedByte;
    initializedMask_ = initializedMask;
    rowSizeOffset_ = rowSizeOffset;
}

const AggregateFunctionEntry*
getAggregateFunctionEntry(const std::string& name) {
    auto sanitizedName = milvus::exec::sanitizeName(name);

    return aggregateFunctions().withRLock(
        [&](const auto& functionsMap) -> const AggregateFunctionEntry* {
            auto it = functionsMap.find(sanitizedName);
            if (it != functionsMap.end()) {
                return &it->second;
            }
            return nullptr;
        });
}

std::unique_ptr<Aggregate>
Aggregate::create(const std::string& name,
                  plan::AggregationNode::Step step,
                  const std::vector<DataType>& argTypes,
                  const QueryConfig& query_config) {
    if (auto func = getAggregateFunctionEntry(name)) {
        return func->factory(step, argTypes, query_config);
    }
    PanicInfo(UnexpectedError, "Aggregate function not registered: {}", name);
}

bool
isPartialOutput(milvus::plan::AggregationNode::Step step) {
    return step == milvus::plan::AggregationNode::Step::kPartial ||
           step == milvus::plan::AggregationNode::Step::kIntermediate;
}

void
registerAggregateFunction(
    const std::string& name,
    const std::vector<std::shared_ptr<expr::AggregateFunctionSignature>>&
        signatures,
    const AggregateFunctionFactory& factory) {
    auto realName = lowerString(name);
    aggregateFunctions().withWLock([&](auto& aggFunctionMap) {
        aggFunctionMap[realName] = {signatures, factory};
    });
}

AggregateFunctionMap&
aggregateFunctions() {
    static AggregateFunctionMap aggFunctionMap;
    return aggFunctionMap;
}

}  // namespace exec
}  // namespace milvus
