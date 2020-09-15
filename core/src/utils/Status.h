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

#pragma once

#include "utils/Error.h"

#include <string>

namespace milvus {

class Status;
#define STATUS_CHECK(func) \
    do {                   \
        Status s = func;   \
        if (!s.ok()) {     \
            return s;      \
        }                  \
    } while (false)

using StatusCode = ErrorCode;

class Status {
 public:
    Status(StatusCode code, const std::string& msg);
    Status() = default;
    virtual ~Status();

    Status(const Status& s);

    Status(Status&& s) noexcept;

    Status&
    operator=(const Status& s);

    Status&
    operator=(Status&& s) noexcept;

    static Status
    OK() {
        return Status();
    }

    bool
    ok() const {
        return state_.empty() || code() == 0;
    }

    StatusCode
    code() const {
        return (state_.empty()) ? 0 : *(StatusCode*)(state_.data());
    }

    std::string
    message() const;

    std::string
    ToString() const;

 private:
    inline void
    CopyFrom(const Status& s);

    inline void
    MoveFrom(Status& s);

 private:
    std::string state_;
};  // Status

}  // namespace milvus
