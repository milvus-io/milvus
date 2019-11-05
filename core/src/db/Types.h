// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "db/engine/ExecutionEngine.h"

#include <stdint.h>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

typedef int64_t IDNumber;
typedef IDNumber* IDNumberPtr;
typedef std::vector<IDNumber> IDNumbers;

typedef std::vector<std::pair<IDNumber, double>> QueryResult;
typedef std::vector<QueryResult> QueryResults;

struct TableIndex {
    int32_t engine_type_ = (int)EngineType::FAISS_IDMAP;
    int32_t nlist_ = 16384;
    int32_t metric_type_ = (int)MetricType::L2;
};

}  // namespace engine
}  // namespace milvus
