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
#include <limits>
#include <string>

#include "fmt/format.h"

namespace milvus::plan {

typedef std::string PlanNodeId;

class PlanNodeIdGenerator {
 public:
    static PlanNodeIdGenerator&
    GetInstance() {
        static PlanNodeIdGenerator instance;
        return instance;
    }

    explicit PlanNodeIdGenerator(int start_id = 0) : next_id_(start_id) {
    }

    // Plans are built concurrently -- every search request parses its own,
    // and a shared-filter search rebinds one per branch per segment -- so the
    // counter must be atomic. Ids only need to be distinct within a plan;
    // wrapping is harmless.
    PlanNodeId
    Next() {
        auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        if (id == std::numeric_limits<int>::max()) {
            next_id_.store(0, std::memory_order_relaxed);
        }
        return fmt::format("{}", id);
    }

 private:
    std::atomic<int> next_id_;
};

inline PlanNodeId
GetNextPlanNodeId() {
    return PlanNodeIdGenerator::GetInstance().Next();
}

}  // namespace milvus::plan