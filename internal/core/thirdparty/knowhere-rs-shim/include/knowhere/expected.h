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

template <typename T>
class expected {
 public:
    expected(const T& value) : value_(value) {
    }

    expected(T&& value) : value_(std::move(value)) {
    }

    expected(Status error) : error_(error) {
        assert(error != Status::success);
    }

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
