// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include "cachinglayer/Translator.h"

namespace milvus::index {

/** @brief Admission estimates and policy for one immutable index generation. */
struct IndexCacheConfig {
    std::string key;
    cachinglayer::Meta meta;
    cachinglayer::ResourceUsage loaded_usage;
    cachinglayer::ResourceUsage loading_overhead;
    int64_t storage_bytes;
    int32_t load_priority{0};
    cachinglayer::ResourceUsage shared_loading_overhead;
};

}  // namespace milvus::index
