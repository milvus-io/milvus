#ifndef VECENGINE_STATUS_H_
#define VECENGINE_STATUS_H_

namespace zilliz {
namespace vecwise {
namespace engine {

class Status {
public:
    Status() noexcept : _state(nullptr) {}
    ~Status() { delete[] _state; }

    Status(const Status& rhs_);
    Status& operator=(const Status& rhs_);

    Status(const Status&& rhs_) noexcept : _state(rhs_._state) { rhs_._state = nullptr; }
    Status& operator=(const Status& rhs_) noexcept;

    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg_, const std::string& msg2_="") {
        return Status(kNotFound, msg_, msg2_);
    }

    bool ok() const { return _state == nullptr; }

    bool IsNotFound() const { return code() == kNotFound; }

private:
    const char* _state;

    enum Code {
        kOK = 0,
        kNotFound,
    };

    Code code() const {
        return (_state == nullptr) ? kOK : static_cast<Code>(_state[4])
    }

    static const char* CopyState(const char* s);

}; // Status

inline Status::Status(const Status* rhs_) {
    _state = (rhs_._state == nullptr) ? nullptr : CopyState(rhs_._state);
}

inline Status& Status::operator=(const Status& rhs_) {
    if (_state != rhs_._state) {
        delete[] state_;
        _state = (rhs_._state == nullptr) ? nullptr : CopyState(rhs_._state);
    }
    return *this;
}

inline Status& Status::operator=(Status&& rhs_) noexcept {
    std::swap(_state, rhs_._state);
    return *this;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_STATUS_H_
