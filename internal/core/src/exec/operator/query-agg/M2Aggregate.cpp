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

#include "M2Aggregate.h"

#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "exec/QueryContext.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

void
registerM2Aggregate() {
    for (bool emitMean : {false, true}) {
        auto name = emitMean ? KMean : KM2;
        exec::registerAggregateFunction(
            name,
            [emitMean, name](
                const std::vector<DataType>& argumentTypes,
                const QueryConfig& /*config*/) -> std::unique_ptr<Aggregate> {
                AssertInfo(argumentTypes.size() == 1,
                           "function:{} only accept one argument",
                           name);
                switch (argumentTypes[0]) {
                    case DataType::INT8:
                        return std::make_unique<M2Aggregate<int8_t>>(emitMean);
                    case DataType::INT16:
                        return std::make_unique<M2Aggregate<int16_t>>(emitMean);
                    case DataType::INT32:
                        return std::make_unique<M2Aggregate<int32_t>>(emitMean);
                    case DataType::INT64:
                        return std::make_unique<M2Aggregate<int64_t>>(emitMean);
                    case DataType::FLOAT:
                        return std::make_unique<M2Aggregate<float>>(emitMean);
                    case DataType::DOUBLE:
                        return std::make_unique<M2Aggregate<double>>(emitMean);
                    default:
                        ThrowInfo(UnexpectedError,
                                  "Unknown input type for {} aggregation {}",
                                  name,
                                  GetDataTypeName(argumentTypes[0]));
                }
            });
    }
    LOG_INFO("Registered M2 and mean Aggregate Functions");
}

}  // namespace exec
}  // namespace milvus
