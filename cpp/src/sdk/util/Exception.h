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

#include "Status.h"

#include <exception>

namespace milvus {
class Exception : public std::exception {
public:
    Exception(StatusCode error_code,
            const std::string &message = std::string())
        : error_code_(error_code), message_(message) {}

public:
    StatusCode error_code() const {
        return error_code_;
    }

    virtual const char *what() const noexcept {
        return message_.c_str();
    }

private:
    StatusCode error_code_;
    std::string message_;
};
}