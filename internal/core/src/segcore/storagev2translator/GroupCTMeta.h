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
#pragma once

#include "cachinglayer/Translator.h"

namespace milvus::segcore::storagev2translator {

struct GroupCTMeta : public milvus::cachinglayer::Meta {
    std::vector<int64_t> num_rows_until_chunk_;
    std::vector<int64_t> chunk_memory_size_;
    GroupCTMeta(milvus::cachinglayer::StorageType storage_type,
                CacheWarmupPolicy cache_warmup_policy,
                bool support_eviction)
        : milvus::cachinglayer::Meta(
              storage_type, cache_warmup_policy, support_eviction) {
    }
};

}  // namespace milvus::segcore::storagev2translator