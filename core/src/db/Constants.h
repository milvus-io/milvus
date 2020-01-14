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

#include <stdint.h>

namespace milvus {
namespace engine {

constexpr uint64_t K = 1024UL;
constexpr uint64_t M = K * K;
constexpr uint64_t G = K * M;
constexpr uint64_t T = K * G;

constexpr uint64_t MAX_TABLE_FILE_MEM = 128 * M;

constexpr int FLOAT_TYPE_SIZE = sizeof(float);

static constexpr uint64_t ONE_KB = K;
static constexpr uint64_t ONE_MB = ONE_KB * ONE_KB;
static constexpr uint64_t ONE_GB = ONE_KB * ONE_MB;

}  // namespace engine
}  // namespace milvus
