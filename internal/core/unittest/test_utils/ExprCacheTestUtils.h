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

#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "exec/expression/ExprCache.h"

namespace milvus::test {

// Backend-only tests have no sealed segment to publish their generation.
// Give repeated keys for one test segment a real token instead of relying on
// the removed generation-zero growing-cache identity.
inline exec::ExprCacheGenerationPtr
CacheTestGeneration(int64_t segment) {
    static std::mutex mutex;
    static std::unordered_map<int64_t, exec::ExprCacheGenerationPtr>
        generations;
    std::lock_guard lock(mutex);
    auto& generation = generations[segment];
    if (!generation || generation->IsRetired()) {
        generation = std::make_shared<exec::ExprCacheGeneration>();
    }
    return generation;
}

struct ExprCacheKey : exec::ExprResCacheManager::Key {
    ExprCacheKey(int64_t segment,
                 ExprCacheSignature signature,
                 exec::ExprCacheGenerationPtr generation = {})
        : Key{segment,
              signature,
              generation ? std::move(generation)
                         : CacheTestGeneration(segment)} {
    }
};

inline std::filesystem::path
CacheFile(const std::filesystem::path& directory,
          int64_t segment,
          uint64_t generation = 0) {
    const auto prefix = "seg_" + std::to_string(segment) + "_" +
                        (generation ? std::to_string(generation) + "_" : "");
    if (std::filesystem::exists(directory)) {
        for (const auto& entry :
             std::filesystem::directory_iterator(directory)) {
            if (entry.path().filename().string().starts_with(prefix) &&
                entry.path().extension() == ".cache") {
                return entry.path();
            }
        }
    }
    return directory / (prefix + "missing.cache");
}
}  // namespace milvus::test
