// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <exception>
#include <string>

#include "common/EasyAssert.h"

namespace milvus {

// Exceptions for executor module
class ExecDriverException : public std::exception {
 public:
    explicit ExecDriverException(const std::string& msg)
        : std::exception(), exception_message_(msg) {
    }
    const char*
    what() const noexcept {
        return exception_message_.c_str();
    }
    virtual ~ExecDriverException() {
    }

 private:
    std::string exception_message_;
};

// Thrown by the driver when an operator call fails (see CALL_OPERATOR in
// exec/Driver.cpp). It derives from SegcoreError so that the classified error
// code chosen at the original throw site survives the driver wrap all the way
// to the CGO boundary: both FailureCStatus(const std::exception*) and the
// futures consume arm (Future.h thenError<milvus::SegcoreError>) extract the
// code from the SegcoreError base. Non-SegcoreError causes (plain
// std::exception etc.) keep the legacy UnexpectedError classification.
class ExecOperatorException : public SegcoreError {
 public:
    explicit ExecOperatorException(const std::string& msg)
        : SegcoreError(ErrorCode::UnexpectedError, msg) {
    }
    ExecOperatorException(ErrorCode error_code, const std::string& msg)
        : SegcoreError(error_code, msg) {
    }
    virtual ~ExecOperatorException() {
    }
};
}  // namespace milvus
