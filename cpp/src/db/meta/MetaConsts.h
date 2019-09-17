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

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

const size_t K = 1024UL;
const size_t M = K*K;
const size_t G = K*M;
const size_t T = K*G;

const size_t S_PS = 1UL;
const size_t MS_PS = 1000*S_PS;
const size_t US_PS = 1000*MS_PS;
const size_t NS_PS = 1000*US_PS;

const size_t SECOND = 1UL;
const size_t M_SEC = 60*SECOND;
const size_t H_SEC = 60*M_SEC;
const size_t D_SEC = 24*H_SEC;
const size_t W_SEC = 7*D_SEC;

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
