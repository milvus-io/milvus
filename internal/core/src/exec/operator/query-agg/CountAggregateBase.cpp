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
#include "CountAggregateBase.h"

#include "common/Utils.h"
#include "exec/QueryContext.h"
#include "exec/operator/query-agg/Aggregate.h"
#include "glog/logging.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

void
registerCount(const std::string& name) {
    exec::registerAggregateFunction(
        name,
        [name](const std::vector<DataType>& /*argumentTypes*/,
               const QueryConfig& /*config*/) -> std::unique_ptr<Aggregate> {
            return std::make_unique<CountAggregate>();
        });
}

void
registerCountAggregate() {
    registerCount(milvus::KCount);
    LOG_INFO("Registered Count Aggregate Function");
}
}  // namespace exec
}  // namespace milvus
