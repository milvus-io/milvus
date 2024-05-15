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

#include <limits>
#include <memory>
#include <string>
#include <vector>

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

    PlanNodeId
    Next() {
        if (next_id_ >= std::numeric_limits<int>::max()) {
            next_id_ = 0;
        }
        return fmt::format("{}", next_id_++);
    }

    void
    Set(int id) {
        next_id_ = id;
    }

    void
    ReSet() {
        next_id_ = 0;
    }

 private:
    int next_id_;
};

inline PlanNodeId
GetNextPlanNodeId() {
    return PlanNodeIdGenerator::GetInstance().Next();
}

}  // namespace milvus::plan