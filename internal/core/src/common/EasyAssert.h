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
#include <cstdio>
#include <cstdlib>
#include <string>
#include "pb/common.pb.h"
#include "common/type_c.h"

#include "fmt/core.h"

/* Paste this on the file if you want to debug. */
namespace milvus {
enum ErrorCode {
    Success = 0,
    UnexpectedError = 2001,
    NotImplemented = 2002,
    Unsupported = 2003,
    IndexBuildError = 2004,
    IndexAlreadyBuild = 2005,
    ConfigInvalid = 2006,
    DataTypeInvalid = 2007,
    PathInvalid = 2009,
    PathAlreadyExist = 2010,
    PathNotExist = 2011,
    FileOpenFailed = 2012,
    FileCreateFailed = 2013,
    FileReadFailed = 2014,
    FileWriteFailed = 2015,
    BucketInvalid = 2016,
    ObjectNotExist = 2017,
    S3Error = 2018,
    RetrieveError = 2019,
    FieldIDInvalid = 2020,
    FieldAlreadyExist = 2021,
    OpTypeInvalid = 2022,
    DataIsEmpty = 2023,
    DataFormatBroken = 2024,
    JsonKeyInvalid = 2025,
    MetricTypeInvalid = 2026,
    FieldNotLoaded = 2027,
    ExprInvalid = 2028,
    UnistdError = 2030,
    MetricTypeNotMatch = 2031,
    DimNotMatch = 2032,
    ClusterSkip = 2033,
    MemAllocateFailed = 2034,
    MemAllocateSizeNotMatch = 2035,
    MmapError = 2036,
    // timeout or cancel related
    FollyOtherException = 2037,
    FollyCancel = 2038,
    OutOfRange = 2039,
    GcpNativeError = 2040,
    InvalidParameter = 2041,
    TextIndexNotFound = 2042,

    KnowhereError = 2099
};
namespace impl {
void
EasyAssertInfo(bool value,
               std::string_view expr_str,
               std::string_view filename,
               int lineno,
               std::string_view extra_info,
               ErrorCode error_code = ErrorCode::UnexpectedError);

}  // namespace impl

class SegcoreError : public std::runtime_error {
 public:
    static SegcoreError
    success() {
        return {ErrorCode::Success, ""};
    }

    SegcoreError(ErrorCode error_code, const std::string& error_msg)
        : std::runtime_error(error_msg), error_code_(error_code) {
    }

    ErrorCode
    get_error_code() const {
        return error_code_;
    }

    bool
    ok() {
        return error_code_ == ErrorCode::Success;
    }

 private:
    ErrorCode error_code_;
};

inline CStatus
SuccessCStatus() {
    return CStatus{Success, ""};
}

inline CStatus
FailureCStatus(int code, const std::string& msg) {
    return CStatus{code, strdup(msg.data())};
}

inline CStatus
FailureCStatus(const std::exception* ex) {
    if (auto segcore_err = dynamic_cast<const SegcoreError*>(ex)) {
        return CStatus{static_cast<int>(segcore_err->get_error_code()),
                       strdup(segcore_err->what())};
    }
    return CStatus{static_cast<int>(UnexpectedError), strdup(ex->what())};
}

}  // namespace milvus

#define AssertInfo(expr, info, args...)                              \
    do {                                                             \
        auto _expr_res = static_cast<bool>(expr);                    \
        /* call func only when needed */                             \
        if (!_expr_res) {                                            \
            milvus::impl::EasyAssertInfo(_expr_res,                  \
                                         #expr,                      \
                                         __FILE__,                   \
                                         __LINE__,                   \
                                         fmt::format(info, ##args)); \
        }                                                            \
    } while (0)

#define Assert(expr) AssertInfo((expr), "")

#define PanicInfo(errcode, info, args...)                       \
    do {                                                        \
        milvus::impl::EasyAssertInfo(false,                     \
                                     "",                        \
                                     __FILE__,                  \
                                     __LINE__,                  \
                                     fmt::format(info, ##args), \
                                     errcode);                  \
        __builtin_unreachable();                                \
    } while (0)
