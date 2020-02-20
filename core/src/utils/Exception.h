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

#include <exception>
#include <string>

namespace milvus {

class Exception : public std::exception {
 public:
    Exception(ErrorCode code, const std::string& message) : code_(code), message_(message) {
    }

    ErrorCode
    code() const throw() {
        return code_;
    }

    virtual const char*
    what() const throw() {
        if (message_.empty()) {
            return "Default Exception.";
        } else {
            return message_.c_str();
        }
    }

    virtual ~Exception() throw() {
    }

 protected:
    ErrorCode code_;
    std::string message_;
};

class InvalidArgumentException : public Exception {
 public:
    InvalidArgumentException() : Exception(SERVER_INVALID_ARGUMENT, "Invalid Argument") {
    }

    explicit InvalidArgumentException(const std::string& message) : Exception(SERVER_INVALID_ARGUMENT, message) {
    }
};

}  // namespace milvus
