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

#include "utils/Status.h"
#include "memory"

#include <cstring>

namespace milvus {

constexpr int CODE_WIDTH = sizeof(StatusCode);

Status::Status(StatusCode code, const std::string& msg) {
    // 4 bytes store code
    // 4 bytes store message length
    // the left bytes store message string
    auto length = static_cast<uint32_t>(msg.size());
    // auto result = new char[length + sizeof(length) + CODE_WIDTH];
    state_.resize(length + sizeof(length) + CODE_WIDTH);
    std::memcpy(state_.data(), &code, CODE_WIDTH);
    std::memcpy(state_.data() + CODE_WIDTH, &length, sizeof(length));
    memcpy(state_.data() + sizeof(length) + CODE_WIDTH, msg.data(), length);
}

Status::~Status() {
}

Status::Status(const Status& s) {
    CopyFrom(s);
}

Status::Status(Status&& s) noexcept {
    MoveFrom(s);
}

Status&
Status::operator=(const Status& s) {
    CopyFrom(s);
    return *this;
}

Status&
Status::operator=(Status&& s) noexcept {
    MoveFrom(s);
    return *this;
}

void
Status::CopyFrom(const Status& s) {
    state_.clear();
    if (s.state_.empty()) {
        return;
    }

    uint32_t length = 0;
    memcpy(&length, s.state_.data() + CODE_WIDTH, sizeof(length));
    int buff_len = length + sizeof(length) + CODE_WIDTH;
    state_.resize(buff_len);
    memcpy(state_.data(), s.state_.data(), buff_len);
}

void
Status::MoveFrom(Status& s) {
    state_ = s.state_;
    s.state_.clear();
}

std::string
Status::message() const {
    if (state_.empty()) {
        return "OK";
    }

    std::string msg;
    uint32_t length = 0;
    memcpy(&length, state_.data() + CODE_WIDTH, sizeof(length));
    if (length > 0) {
        msg.append(state_.data() + sizeof(length) + CODE_WIDTH, length);
    }

    return msg;
}

std::string
Status::ToString() const {
    if (state_.empty()) {
        return "OK";
    }

    std::string result;
    switch (code()) {
        case DB_SUCCESS:
            result = "OK ";
            break;
        case DB_ERROR:
            result = "Error: ";
            break;
        case DB_META_TRANSACTION_FAILED:
            result = "Database error: ";
            break;
        case DB_NOT_FOUND:
            result = "Not found: ";
            break;
        case DB_ALREADY_EXIST:
            result = "Already exist: ";
            break;
        case DB_INVALID_PATH:
            result = "Invalid path: ";
            break;
        default:
            result = "Error code(" + std::to_string(code()) + "): ";
            break;
    }

    result += message();
    return result;
}

}  // namespace milvus
