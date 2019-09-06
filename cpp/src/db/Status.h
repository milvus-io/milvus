/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"

#include <string>

namespace zilliz {
namespace milvus {
namespace engine {

class Status {
 public:
    Status(ErrorCode code, const std::string &msg);
    Status();
    ~Status();

    Status(const Status &rhs);

    Status &
    operator=(const Status &rhs);

    Status(Status &&rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }

    Status &
    operator=(Status &&rhs_) noexcept;

    static Status
    OK() { return Status(); }

    bool ok() const { return state_ == nullptr || code() == DB_SUCCESS; }

    std::string ToString() const;

    ErrorCode code() const {
        return (state_ == nullptr) ? DB_SUCCESS : *(ErrorCode*)(state_);
    }

 private:
    const char *state_ = nullptr;

    static const char *CopyState(const char *s);

}; // Status

inline Status::Status(const Status &rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status &Status::operator=(const Status &rhs) {
    if (state_ != rhs.state_) {
        delete[] state_;
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    return *this;
}

inline Status &Status::operator=(Status &&rhs) noexcept {
    std::swap(state_, rhs.state_);
    return *this;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
