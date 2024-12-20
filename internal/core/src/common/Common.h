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

#include <iostream>
#include <utility>
#include <variant>
#include "common/Consts.h"

namespace milvus {

extern int64_t FILE_SLICE_SIZE;
extern int64_t HIGH_PRIORITY_THREAD_CORE_COEFFICIENT;
extern int64_t MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT;
extern int64_t LOW_PRIORITY_THREAD_CORE_COEFFICIENT;
extern int CPU_NUM;
extern int64_t EXEC_EVAL_EXPR_BATCH_SIZE;

enum class Role { QueryNode, IndexNode };

const std::unordered_map<Role, std::string> RoleToStringMap = {
    {Role::QueryNode, "querynode"}, {Role::IndexNode, "indexnode"}};

// convert role to string
inline std::string
ToString(Role role) {
    auto it = RoleToStringMap.find(role);
    if (it != RoleToStringMap.end()) {
        return it->second;
    }
    PanicInfo(UnexpectedError, "role {} not found", int(role));
}

// convert string to role
inline Role
FromString(const std::string& role_str) {
    for (const auto& pair : RoleToStringMap) {
        if (pair.second == role_str) {
            return pair.first;
        }
    }
    PanicInfo(UnexpectedError, "role {} not found", role_str);
}

void
SetIndexSliceSize(const int64_t size);

void
SetHighPriorityThreadCoreCoefficient(const int64_t coefficient);

void
SetMiddlePriorityThreadCoreCoefficient(const int64_t coefficient);

void
SetLowPriorityThreadCoreCoefficient(const int64_t coefficient);

void
SetCpuNum(const int core);

void
SetDefaultExecEvalExprBatchSize(int64_t val);

struct BufferView {
    struct Element {
        const char* data_;
        uint64_t* offsets_;
        int start_;
        int end_;
    };

    std::variant<std::vector<Element>, std::pair<char*, size_t>> data_;
};

}  // namespace milvus
