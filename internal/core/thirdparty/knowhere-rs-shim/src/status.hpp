#pragma once

#include <cstdint>
#include <string>

#include "knowhere/expected.h"

namespace knowhere {

inline Status
ToStatus(int32_t code) {
    switch (code) {
        case 0:
            return Status::success;
        case 2:
            return Status::invalid_args;
        case 4:
            return Status::not_implemented;
        default:
            return Status::invalid_index_error;
    }
}

template <typename T>
expected<T>
ErrorExpected(Status status, const std::string& message) {
    expected<T> result(status);
    result << message;
    return result;
}

}  // namespace knowhere
