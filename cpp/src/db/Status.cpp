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

#include <stdio.h>
#include <cstring>
#include <assert.h>
#include "Status.h"

namespace zilliz {
namespace milvus {
namespace engine {

constexpr int CODE_WIDTH = sizeof(ErrorCode);

Status::Status(ErrorCode code, const std::string& msg) {
    //4 bytes store code
    //4 bytes store message length
    //the left bytes store message string
    const uint32_t length = (uint32_t)msg.size();
    char* result = new char[length + sizeof(length) + CODE_WIDTH];
    std::memcpy(result, &code, CODE_WIDTH);
    std::memcpy(result + CODE_WIDTH, &length, sizeof(length));
    memcpy(result + sizeof(length) + CODE_WIDTH, msg.data(), length);

    state_ = result;
}

Status::Status()
    : state_(nullptr) {

}

Status::~Status() {
    delete[] state_;
}

const char* Status::CopyState(const char* state) {
    uint32_t length = 0;
    std::memcpy(&length, state + CODE_WIDTH, sizeof(length));
    int buff_len = length + sizeof(length) + CODE_WIDTH;
    char* result = new char[buff_len];
    memcpy(result, state, buff_len);
    return result;
}

std::string Status::ToString() const {
    if (state_ == nullptr) return "OK";
    char tmp[32];
    const char* type;
    switch (code()) {
        case DB_SUCCESS:
            type = "OK";
            break;
        case DB_ERROR:
            type = "Error: ";
            break;
        case DB_META_TRANSACTION_FAILED:
            type = "DBTransactionError: ";
            break;
        case DB_NOT_FOUND:
            type = "NotFound: ";
            break;
        case DB_ALREADY_EXIST:
            type = "AlreadyExist: ";
            break;
        case DB_INVALID_PATH:
            type = "InvalidPath: ";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Error code(0x%x): ",
                    static_cast<int>(code()));
            type = tmp;
            break;
    }

    std::string result(type);
    uint32_t length = 0;
    memcpy(&length, state_ + CODE_WIDTH, sizeof(length));
    result.append(state_ + sizeof(length) + CODE_WIDTH, length);
    return result;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
