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
#include "utils/Log.h"

#include <assert.h>
#include <fiu-local.h>
#include <chrono>
#include <iostream>
#include <string>

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

Status
SimpleIDGenerator::NextIDNumbers(size_t n, IDNumbers& ids) {
    if (n > MAX_IDS_PER_MICRO) {
        NextIDNumbers(n - MAX_IDS_PER_MICRO, ids);
        NextIDNumbers(MAX_IDS_PER_MICRO, ids);
        return Status::OK();
    }
    if (n <= 0) {
        return Status::OK();
    }

    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    micros *= MAX_IDS_PER_MICRO;

    for (int pos = 0; pos < n; ++pos) {
        ids.push_back(micros + pos);
    }
    return Status::OK();
}

Status
SimpleIDGenerator::GetNextIDNumbers(size_t n, IDNumbers& ids) {
    ids.clear();
    NextIDNumbers(n, ids);

    return Status::OK();
}

IDNumber
SafeIDGenerator::GetNextIDNumber() {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    std::lock_guard<std::mutex> lock(mtx_);
    if (micros <= time_stamp_ms_) {
        time_stamp_ms_ += 1;
    } else {
        time_stamp_ms_ = micros;
    }
    return time_stamp_ms_ * MAX_IDS_PER_MICRO;
}

Status
SafeIDGenerator::GetNextIDNumbers(size_t n, IDNumbers& ids) {
    ids.clear();
    std::lock_guard<std::mutex> lock(mtx_);
    while (n > 0) {
        if (n > MAX_IDS_PER_MICRO) {
            Status status = NextIDNumbers(MAX_IDS_PER_MICRO, ids);
            if (!status.ok()) {
                return status;
            }
            n -= MAX_IDS_PER_MICRO;
        } else {
            Status status = NextIDNumbers(n, ids);
            if (!status.ok()) {
                return status;
            }
            break;
        }
    }
    return Status::OK();
}

Status
SafeIDGenerator::NextIDNumbers(size_t n, IDNumbers& ids) {
    if (n <= 0 || n > MAX_IDS_PER_MICRO) {
        std::string msg = "Invalid ID number: " + std::to_string(n);
        LOG_ENGINE_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    auto now = std::chrono::system_clock::now();
    int64_t micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    if (micros <= time_stamp_ms_) {
        time_stamp_ms_ += 1;
    } else {
        time_stamp_ms_ = micros;
    }

    int64_t ID_high_part = time_stamp_ms_ * MAX_IDS_PER_MICRO;

    for (int pos = 0; pos < n; ++pos) {
        ids.push_back(ID_high_part + pos);
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
