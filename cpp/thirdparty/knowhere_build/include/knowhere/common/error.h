#pragma once

#include <cstdint>
#include "zlibrary/error/error.h"


namespace zilliz {
namespace knowhere {

using Error = zilliz::lib::ErrorCode;

constexpr Error STORE_SUCCESS = zilliz::lib::SUCCESS_CODE;

constexpr Error ERROR_CODE_BASE = 0x36000;
constexpr Error ERROR_CODE_END = 0x37000;

constexpr Error
ToGlobalErrorCode(const Error error_code) {
    return zilliz::lib::ToGlobalErrorCode(error_code, ERROR_CODE_BASE);
}

class Exception : public zilliz::lib::Exception {
 public:
    Exception(const Error error_code,
              const std::string &message = nullptr)
        : zilliz::lib::Exception(error_code, "KNOWHERE", message) {}
};

constexpr Error UNEXPECTED = ToGlobalErrorCode(0x001);
constexpr Error UNSUPPORTED = ToGlobalErrorCode(0x002);
constexpr Error NULL_POINTER = ToGlobalErrorCode(0x003);
constexpr Error OVERFLOW = ToGlobalErrorCode(0x004);
constexpr Error INVALID_ARGUMENT = ToGlobalErrorCode(0x005);
constexpr Error UNSUPPORTED_TYPE = ToGlobalErrorCode(0x006);


} // namespace store
} // namespace zilliz

using Error = zilliz::store::Error;
