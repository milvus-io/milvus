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
#include <string_view>
#include <stdexcept>
#include <exception>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "pb/common.pb.h"

/* Paste this on the file if you want to debug. */
namespace milvus {
using ErrorCodeEnum = proto::common::ErrorCode;
namespace impl {
void
EasyAssertInfo(bool value,
               std::string_view expr_str,
               std::string_view filename,
               int lineno,
               std::string_view extra_info,
               ErrorCodeEnum error_code = ErrorCodeEnum::UnexpectedError);

}  // namespace impl

class SegcoreError : public std::runtime_error {
 public:
    SegcoreError(ErrorCodeEnum error_code, const std::string& error_msg)
        : error_code_(error_code), std::runtime_error(error_msg) {
    }

    ErrorCodeEnum
    get_error_code() {
        return error_code_;
    }

 private:
    ErrorCodeEnum error_code_;
};

}  // namespace milvus

#define AssertInfo(expr, info)                                                          \
    do {                                                                                \
        auto _expr_res = bool(expr);                                                    \
        /* call func only when needed */                                                \
        if (!_expr_res) {                                                               \
            milvus::impl::EasyAssertInfo(_expr_res, #expr, __FILE__, __LINE__, (info)); \
        }                                                                               \
    } while (0)

#define Assert(expr) AssertInfo((expr), "")
#define PanicInfo(info)                                                      \
    do {                                                                     \
        milvus::impl::EasyAssertInfo(false, (info), __FILE__, __LINE__, ""); \
        __builtin_unreachable();                                             \
    } while (0)

#define PanicCodeInfo(errcode, info)                                                  \
    do {                                                                              \
        milvus::impl::EasyAssertInfo(false, (info), __FILE__, __LINE__, "", errcode); \
        __builtin_unreachable();                                                      \
    } while (0)
