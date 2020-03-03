// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "db/IDGenerator.h"

#include <assert.h>
#include <fiu-local.h>
#include <chrono>
#include <iostream>

namespace milvus {
namespace engine {

IDGenerator::~IDGenerator() = default;

constexpr size_t SimpleIDGenerator::MAX_IDS_PER_MICRO;

IDNumber
SimpleIDGenerator::GetNextIDNumber() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    return micros * MAX_IDS_PER_MICRO;
}

void
SimpleIDGenerator::NextIDNumbers(size_t n, IDNumbers& ids) {
    if (n > MAX_IDS_PER_MICRO) {
        NextIDNumbers(n - MAX_IDS_PER_MICRO, ids);
        NextIDNumbers(MAX_IDS_PER_MICRO, ids);
        return;
    }
    if (n <= 0) {
        return;
    }

    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    micros *= MAX_IDS_PER_MICRO;

    for (int pos = 0; pos < n; ++pos) {
        ids.push_back(micros + pos);
    }
}

void
SimpleIDGenerator::GetNextIDNumbers(size_t n, IDNumbers& ids) {
    ids.clear();
    NextIDNumbers(n, ids);
}

}  // namespace engine
}  // namespace milvus
