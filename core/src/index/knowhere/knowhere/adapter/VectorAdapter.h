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

#include <string>
#include "knowhere/common/Dataset.h"

namespace knowhere {

namespace meta {
extern const char* DIM;
extern const char* TENSOR;
extern const char* ROWS;
extern const char* IDS;
extern const char* DISTANCE;
};  // namespace meta

#define GETTENSOR(dataset)                         \
    auto dim = dataset->Get<int64_t>(meta::DIM);   \
    auto rows = dataset->Get<int64_t>(meta::ROWS); \
    auto p_data = dataset->Get<const float*>(meta::TENSOR);

}  // namespace knowhere
