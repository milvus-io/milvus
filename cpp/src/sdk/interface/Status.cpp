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

#include "Status.h"

namespace milvus {

Status::~Status() noexcept {
    if (state_ != nullptr) {
        delete state_;
        state_ = nullptr;
    }
}

static inline std::ostream &operator<<(std::ostream &os, const Status &x) {
    os << x.ToString();
    return os;
}

void Status::MoveFrom(Status &s) {
    delete state_;
    state_ = s.state_;
    s.state_ = nullptr;
}

Status::Status(const Status &s)
        : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}

Status::Status(Status &&s) noexcept {
    MoveFrom(s);
}

Status &Status::operator=(const Status &s) {
    if (state_ != s.state_) {
        CopyFrom(s);
    }
    return *this;
}

Status &Status::operator=(Status &&s) noexcept {
    MoveFrom(s);
    return *this;
}

Status Status::operator&(const Status &status) const noexcept {
    if (ok()) {
        return status;
    } else {
        return *this;
    }
}

Status Status::operator&(Status &&s) const noexcept {
    if (ok()) {
        return std::move(s);
    } else {
        return *this;
    }
}

Status &Status::operator&=(const Status &s) noexcept {
    if (ok() && !s.ok()) {
        CopyFrom(s);
    }
    return *this;
}

Status &Status::operator&=(Status &&s) noexcept {
    if (ok() && !s.ok()) {
        MoveFrom(s);
    }
    return *this;
}

Status::Status(StatusCode code, const std::string &message) {
    state_ = new State;
    state_->code = code;
    state_->message = message;
}

void Status::CopyFrom(const Status &status) {
    delete state_;
    if (status.state_ == nullptr) {
        state_ = nullptr;
    } else {
        state_ = new State(*status.state_);
    }
}

std::string Status::CodeAsString() const {
    if (state_ == nullptr) {
        return "OK";
    }

    const char *type = nullptr;
    switch (code()) {
        case StatusCode::OK:
            type = "OK";
            break;
        case StatusCode::InvalidAgument:
            type = "Invalid agument";
            break;
        case StatusCode::UnknownError:
            type = "Unknown error";
            break;
        case StatusCode::NotSupported:
            type = "Not Supported";
            break;
        case StatusCode::NotConnected:
            type = "Not Connected";
            break;
        default:
            type = "Unknown";
            break;
    }
    return std::string(type);
}

std::string Status::ToString() const {
    std::string result(CodeAsString());
    if (state_ == nullptr) {
        return result;
    }
    result += ": ";
    result += state_->message;
    return result;
}

}

