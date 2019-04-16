#pragma once

#include <string>

namespace zilliz {
namespace vecwise {
namespace engine {

class Status {
public:
    Status() noexcept : state_(nullptr) {}
    ~Status() { delete[] state_; }

    Status(const Status& rhs);
    Status& operator=(const Status& rhs);

    Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
    Status& operator=(Status&& rhs_) noexcept;

    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg, const std::string& msg2="") {
        return Status(kNotFound, msg, msg2);
    }

    bool ok() const { return state_ == nullptr; }

    bool IsNotFound() const { return code() == kNotFound; }

    std::string ToString() const;

private:
    const char* state_;

    enum Code {
        kOK = 0,
        kNotFound,
    };

    Code code() const {
        return (state_ == nullptr) ? kOK : static_cast<Code>(state_[4]);
    }
    Status(Code code, const std::string& msg, const std::string& msg2);
    static const char* CopyState(const char* s);

}; // Status

inline Status::Status(const Status& rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
    if (state_ != rhs.state_) {
        delete[] state_;
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    return *this;
}

inline Status& Status::operator=(Status&& rhs) noexcept {
    std::swap(state_, rhs.state_);
    return *this;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
