// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "utils/Error.h"

#include <string>

namespace zilliz {
namespace milvus {
namespace engine {

class Status {
 public:
    Status(ErrorCode code, const std::string &msg);
    Status();
    ~Status();

    Status(const Status &rhs);

    Status &
    operator=(const Status &rhs);

    Status(Status &&rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }

    Status &
    operator=(Status &&rhs_) noexcept;

    static Status
    OK() { return Status(); }

    bool ok() const { return state_ == nullptr || code() == DB_SUCCESS; }

    std::string ToString() const;

    ErrorCode code() const {
        return (state_ == nullptr) ? DB_SUCCESS : *(ErrorCode*)(state_);
    }

 private:
    const char *state_ = nullptr;

    static const char *CopyState(const char *s);

}; // Status

inline Status::Status(const Status &rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status &Status::operator=(const Status &rhs) {
    if (state_ != rhs.state_) {
        delete[] state_;
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    return *this;
}

inline Status &Status::operator=(Status &&rhs) noexcept {
    std::swap(state_, rhs.state_);
    return *this;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
