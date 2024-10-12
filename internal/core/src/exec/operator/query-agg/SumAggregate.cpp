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

#include "SumAggregateBase.h"

namespace milvus {
namespace exec {

template <typename TInput, typename TAccumulator, typename ResultType>
using SumAggregate = SumAggregateBase<TInput, TAccumulator, ResultType, false>;
template <template <typename U, typename V, typename W> class T>
void
registerSum(const std::string& name) {
    exec::registerAggregateFunction(
        name,
        [name](const std::vector<DataType>& argumentTypes,
               const QueryConfig& config) -> std::unique_ptr<Aggregate> {
            AssertInfo(argumentTypes.size() == 1,
                       "function:{} only accept one argument",
                       name);
            auto inputType = argumentTypes[0];
            switch (inputType) {
                case DataType::INT8:
                    return std::make_unique<T<int8_t, int64_t, int64_t>>(
                        DataType::INT64);
                case DataType::INT16:
                    return std::make_unique<T<int16_t, int64_t, int64_t>>(
                        DataType::INT64);
                case DataType::INT32:
                    return std::make_unique<T<int32_t, int64_t, int64_t>>(
                        DataType::INT64);
                case DataType::TIMESTAMPTZ:
                case DataType::INT64:
                    return std::make_unique<T<int64_t, int64_t, int64_t>>(
                        DataType::INT64);
                case DataType::DOUBLE:
                    return std::make_unique<T<double, double, double>>(
                        DataType::DOUBLE);
                case DataType::FLOAT:
                    return std::make_unique<T<float, double, double>>(
                        DataType::DOUBLE);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "Unknown input type for {} aggregation {}",
                              name,
                              GetDataTypeName(inputType));
            }
        });
};

void
registerSumAggregate() {
    registerSum<SumAggregate>(milvus::KSum);
    LOG_INFO("Registered Sum Aggregate Function");
}

}  // namespace exec
}  // namespace milvus
