// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <string>

#include "index/IndexCacheConfig.h"

namespace milvus::index::test {

// Use a private slot and synchronous warmup so load tests exercise storage I/O
// before Load returns, including corruption and cancellation failures.
inline IndexCacheConfig
TestIndexCacheConfig() {
    static std::atomic<uint64_t> next{0};
    return {"index-load-test-" + std::to_string(next.fetch_add(1)),
            cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                               cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
                               cachinglayer::CellDataType::OTHER,
                               CacheWarmupPolicy::CacheWarmupPolicy_Sync,
                               false},
            {1024 * 1024, 1024 * 1024},
            {},
            4096};
}

}  // namespace milvus::index::test
