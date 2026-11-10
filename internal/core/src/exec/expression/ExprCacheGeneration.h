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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

namespace milvus::exec {

// Owned by a published sealed snapshot, not by the cache backend. Retirement
// survives Clear/SetConfig and remains visible to delayed writers holding it.
class ExprCacheGeneration {
 public:
    ExprCacheGeneration();
    ExprCacheGeneration(const ExprCacheGeneration&) = delete;
    ExprCacheGeneration&
    operator=(const ExprCacheGeneration&) = delete;

    const uint64_t id;

 private:
    friend class ExprResCacheManager;
    bool retired_{false};  // guarded by ExprResCacheManager::state_mutex_
};

using ExprCacheGenerationPtr = std::shared_ptr<ExprCacheGeneration>;

struct ExprCacheSegmentKey {
    // Generation zero is reserved for growing segments and backend unit tests.
    ExprCacheSegmentKey(int64_t segment = 0, uint64_t version = 0)
        : segment_id(segment), generation(version) {
    }

    int64_t segment_id;
    uint64_t generation;

    bool
    operator==(const ExprCacheSegmentKey& other) const {
        return segment_id == other.segment_id && generation == other.generation;
    }

    bool
    operator!=(const ExprCacheSegmentKey& other) const {
        return !(*this == other);
    }
};

struct ExprCacheSegmentKeyHasher {
    size_t
    operator()(const ExprCacheSegmentKey& key) const noexcept {
        return std::hash<int64_t>{}(key.segment_id) * 1315423911u ^
               std::hash<uint64_t>{}(key.generation);
    }
};

}  // namespace milvus::exec
