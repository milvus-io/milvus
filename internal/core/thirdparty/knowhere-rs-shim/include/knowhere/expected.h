#pragma once

#include <cassert>
#include <optional>
#include <string>
#include <utility>

namespace knowhere {

enum class Status {
    success = 0,
    invalid_args = 1,
    invalid_param_in_json = 2,
    out_of_range_in_json = 3,
    type_conflict_in_json = 4,
    invalid_metric_type = 5,
    empty_index = 6,
    not_implemented = 7,
    index_not_trained = 8,
    invalid_index_error = 9,
    invalid_cluster_error = 10,
    invalid_binary_set = 11,
};

inline const char*
Status2String(Status status) {
    switch (status) {
        case Status::success:
            return "success";
        case Status::invalid_args:
            return "invalid_args";
        case Status::invalid_param_in_json:
            return "invalid_param_in_json";
        case Status::out_of_range_in_json:
            return "out_of_range_in_json";
        case Status::type_conflict_in_json:
            return "type_conflict_in_json";
        case Status::invalid_metric_type:
            return "invalid_metric_type";
        case Status::empty_index:
            return "empty_index";
        case Status::not_implemented:
            return "not_implemented";
        case Status::index_not_trained:
            return "index_not_trained";
        case Status::invalid_index_error:
            return "invalid_index_error";
        case Status::invalid_cluster_error:
            return "invalid_cluster_error";
        case Status::invalid_binary_set:
            return "invalid_binary_set";
    }
    return "unknown";
}

template <typename T>
class expected {
 public:
    expected() : value_(std::make_optional<T>()) {
    }

    expected(const T& value) : value_(value) {
    }

    expected(T&& value) : value_(std::move(value)) {
    }

    expected(const Status& error) : error_(error) {
        assert(error != Status::success);
    }

    expected(Status&& error) : error_(error) {
        assert(error != Status::success);
    }

    expected(const expected<T>&) = default;

    expected(expected<T>&&) noexcept = default;

    expected&
    operator=(const expected<T>&) = default;

    expected&
    operator=(expected<T>&&) noexcept = default;

    bool
    has_value() const {
        return value_.has_value();
    }

    const T&
    value() const {
        assert(value_.has_value());
        return *value_;
    }

    T&
    value() {
        assert(value_.has_value());
        return *value_;
    }

    Status
    error() const {
        assert(error_.has_value());
        return *error_;
    }

    const std::string&
    what() const {
        return message_;
    }

    void
    operator<<(const std::string& message) {
        message_ += message;
    }

    expected<T>&
    operator=(const Status& error) {
        assert(error != Status::success);
        value_.reset();
        error_ = error;
        return *this;
    }

 private:
    std::optional<T> value_;
    std::optional<Status> error_;
    std::string message_;
};

#define RETURN_IF_ERROR(expr)            \
    do {                                 \
        auto status = (expr);            \
        if (status != Status::success) { \
            return status;               \
        }                                \
    } while (0)

template <typename T>
Status
DoAssignOrReturn(T& lhs, expected<T>&& rhs) {
    if (rhs.has_value()) {
        lhs = std::move(rhs.value());
        return Status::success;
    }
    return rhs.error();
}

}  // namespace knowhere
