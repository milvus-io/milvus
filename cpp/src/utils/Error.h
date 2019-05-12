/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <cstdint>
#include <exception>
#include <string>

namespace zilliz {
namespace vecwise {
namespace server {

using ServerError = int32_t;

constexpr ServerError SERVER_SUCCESS = 0;

constexpr ServerError SERVER_ERROR_CODE_BASE = 0x30000;

constexpr ServerError
ToGlobalServerErrorCode(const ServerError error_code) {
    return SERVER_ERROR_CODE_BASE + SERVER_ERROR_CODE_BASE;
}

constexpr ServerError SERVER_UNEXPECTED_ERROR = ToGlobalServerErrorCode(0x001);
constexpr ServerError SERVER_UNSUPPORTED_ERROR = ToGlobalServerErrorCode(0x002);
constexpr ServerError SERVER_NULL_POINTER = ToGlobalServerErrorCode(0x003);
constexpr ServerError SERVER_INVALID_ARGUMENT = ToGlobalServerErrorCode(0x004);
constexpr ServerError SERVER_FILE_NOT_FOUND = ToGlobalServerErrorCode(0x005);
constexpr ServerError SERVER_NOT_IMPLEMENT = ToGlobalServerErrorCode(0x006);
constexpr ServerError SERVER_BLOCKING_QUEUE_EMPTY = ToGlobalServerErrorCode(0x007);
constexpr ServerError SERVER_LICENSE_VALIDATION_FAIL = ToGlobalServerErrorCode(0x008);
constexpr ServerError SERVER_LICENSE_FILE_NOT_EXIST = ToGlobalServerErrorCode(0x009);

class ServerException : public std::exception {
public:
    ServerException(ServerError error_code,
              const std::string &message = std::string())
            : error_code_(error_code), message_(message) {}

public:
    ServerError error_code() const {
        return error_code_;
    }

    virtual const char *what() const noexcept {
        return message_.c_str();
    }

private:
    ServerError error_code_;
    std::string message_;
};

}  // namespace server
}  // namespace vecwise
}  // namespace zilliz

