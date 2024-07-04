// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <optional>
#include <string>
#include <exception>
#include <memory>
#include <functional>
#include <common/type_c.h>

namespace milvus::futures {

/// @brief LeakyResult is a class that holds the result that can be leaked.
/// @tparam R is a type to real result that can be leak after get operation.
template <class R>
class LeakyResult {
 public:
    /// @brief default construct a empty Result, which is just used for easy contruction.
    LeakyResult() {
    }

    /// @brief create a LeakyResult with error code and error message which means failure.
    /// @param error_code see CStatus difinition.
    /// @param error_msg see CStatus difinition.
    LeakyResult(int error_code, const std::string& error_msg) {
        auto msg = strdup(error_msg.c_str());
        status_ = std::make_optional(CStatus{error_code, msg});
    }

    /// @brief create a LeakyResult with a result which means success.
    /// @param r
    LeakyResult(R* r) : result_(std::make_optional(r)) {
    }

    LeakyResult(const LeakyResult<R>&) = delete;

    LeakyResult(LeakyResult<R>&& other) noexcept {
        if (other.result_.has_value()) {
            result_ = std::move(other.result_);
            other.result_.reset();
        }
        if (other.status_.has_value()) {
            status_ = std::move(other.status_);
            other.status_.reset();
        }
    }

    LeakyResult&
    operator=(const LeakyResult<R>&) = delete;

    LeakyResult&
    operator=(LeakyResult<R>&& other) noexcept {
        if (this != &other) {
            if (other.result_.has_value()) {
                result_ = std::move(other.result_);
                other.result_.reset();
            }
            if (other.status_.has_value()) {
                status_ = std::move(other.status_);
                other.status_.reset();
            }
        }
        return *this;
    }

    /// @brief get the Result or CStatus from LeakyResult, performed a manual memory management.
    /// caller has responsibitiy to release if void* is not nullptr or cstatus is not nullptr.
    /// @return a pair of void* and CStatus is returned, void* => R*.
    /// condition (void* == nullptr and CStatus is failure) or (void* != nullptr and CStatus is success) is met.
    /// release operation of CStatus see common/type_c.h.
    std::pair<void*, CStatus>
    leakyGet() {
        if (result_.has_value()) {
            R* result_ptr = result_.value();
            result_.reset();
            return std::make_pair<void*, CStatus>(result_ptr,
                                                  CStatus{0, nullptr});
        }
        if (status_.has_value()) {
            CStatus status = status_.value();
            status_.reset();
            return std::make_pair<void*, CStatus>(
                nullptr, CStatus{status.error_code, status.error_msg});
        }
        throw std::logic_error("get on a not ready LeakyResult");
    }

    ~LeakyResult() {
        if (result_.has_value()) {
            delete result_.value();
        }
        if (status_.has_value()) {
            free((char*)(status_.value().error_msg));
        }
    }

 private:
    std::optional<CStatus> status_;
    std::optional<R*> result_;
};

};  // namespace milvus::futures