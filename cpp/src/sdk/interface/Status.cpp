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

#include <cstring>

namespace milvus {

constexpr int CODE_WIDTH = sizeof(ErrorCode);

Status::Status(ErrorCode code, const std::string& msg) {
    //4 bytes store code
    //4 bytes store message length
    //the left bytes store message string
    const uint32_t length = (uint32_t)msg.size();
    char* result = new char[length + sizeof(length) + CODE_WIDTH];
    memcpy(result, &code, CODE_WIDTH);
    memcpy(result + CODE_WIDTH, &length, sizeof(length));
    memcpy(result + sizeof(length) + CODE_WIDTH, msg.data(), length);

    state_ = result;
}

Status::Status()
        : state_(nullptr) {

}

Status::~Status() {
    delete state_;
}

Status::Status(const Status &s)
        : state_(nullptr) {
    CopyFrom(s);
}

Status &Status::operator=(const Status &s) {
    CopyFrom(s);
    return *this;
}

Status::Status(Status &&s)
        : state_(nullptr) {
    MoveFrom(s);
}

Status &Status::operator=(Status &&s) {
    MoveFrom(s);
    return *this;
}

void Status::CopyFrom(const Status &s) {
    delete state_;
    state_ = nullptr;
    if(s.state_ == nullptr) {
        return;
    }

    uint32_t length = 0;
    std::memcpy(&length, s.state_ + CODE_WIDTH, sizeof(length));
    int buff_len = length + sizeof(length) + CODE_WIDTH;
    state_ = new char[buff_len];
    memcpy((void*)state_, (void*)s.state_, buff_len);
}

void Status::MoveFrom(Status &s) {
    delete state_;
    state_ = s.state_;
    s.state_ = nullptr;
}

std::string Status::ToString() const {
    if (state_ == nullptr) {
        return "OK";
    }

    std::string result;
    switch (code()) {
        case StatusCode::OK:
            result = "OK ";
            break;
        case StatusCode::UnknownError:
            result = "Unknown error: ";
            break;
        case StatusCode::NotSupported:
            result = "Not supported: ";
            break;
        case StatusCode::NotConnected:
            result = "Not connected: ";
            break;
        case StatusCode::InvalidAgument:
            result = "Invalid agument: ";
            break;
        case StatusCode::RPCFailed:
            result = "Remote call failed: ";
            break;
        case StatusCode::ServerFailed:
            result = "Service error: ";
            break;
        default:
            result = "Error code(" + std::to_string((int)code()) + "): ";
            break;
    }

    uint32_t length = 0;
    memcpy(&length, state_ + CODE_WIDTH, sizeof(length));
    if(length > 0) {
        result.append(state_ + sizeof(length) + CODE_WIDTH, length);
    }

    return result;
}

}

