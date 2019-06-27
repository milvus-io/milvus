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
namespace milvus {
namespace server {

using ServerError = int32_t;

constexpr ServerError SERVER_SUCCESS = 0;

constexpr ServerError SERVER_ERROR_CODE_BASE = 0x30000;

constexpr ServerError
ToGlobalServerErrorCode(const ServerError error_code) {
    return SERVER_ERROR_CODE_BASE + error_code;
}

constexpr ServerError SERVER_UNEXPECTED_ERROR = ToGlobalServerErrorCode(1);
constexpr ServerError SERVER_UNSUPPORTED_ERROR = ToGlobalServerErrorCode(2);
constexpr ServerError SERVER_NULL_POINTER = ToGlobalServerErrorCode(3);
constexpr ServerError SERVER_INVALID_ARGUMENT = ToGlobalServerErrorCode(4);
constexpr ServerError SERVER_FILE_NOT_FOUND = ToGlobalServerErrorCode(5);
constexpr ServerError SERVER_NOT_IMPLEMENT = ToGlobalServerErrorCode(6);
constexpr ServerError SERVER_BLOCKING_QUEUE_EMPTY = ToGlobalServerErrorCode(7);
constexpr ServerError SERVER_CANNOT_CREATE_FOLDER = ToGlobalServerErrorCode(8);
constexpr ServerError SERVER_CANNOT_CREATE_FILE = ToGlobalServerErrorCode(9);
constexpr ServerError SERVER_CANNOT_DELETE_FOLDER = ToGlobalServerErrorCode(10);
constexpr ServerError SERVER_CANNOT_DELETE_FILE = ToGlobalServerErrorCode(11);

constexpr ServerError SERVER_TABLE_NOT_EXIST = ToGlobalServerErrorCode(100);
constexpr ServerError SERVER_INVALID_TABLE_NAME = ToGlobalServerErrorCode(101);
constexpr ServerError SERVER_INVALID_TABLE_DIMENSION = ToGlobalServerErrorCode(102);
constexpr ServerError SERVER_INVALID_TIME_RANGE = ToGlobalServerErrorCode(103);
constexpr ServerError SERVER_INVALID_VECTOR_DIMENSION = ToGlobalServerErrorCode(104);
constexpr ServerError SERVER_INVALID_INDEX_TYPE = ToGlobalServerErrorCode(105);
constexpr ServerError SERVER_INVALID_ROWRECORD = ToGlobalServerErrorCode(106);
constexpr ServerError SERVER_INVALID_ROWRECORD_ARRAY = ToGlobalServerErrorCode(107);
constexpr ServerError SERVER_INVALID_TOPK = ToGlobalServerErrorCode(108);
constexpr ServerError SERVER_ILLEGAL_VECTOR_ID = ToGlobalServerErrorCode(109);
constexpr ServerError SERVER_ILLEGAL_SEARCH_RESULT = ToGlobalServerErrorCode(110);
constexpr ServerError SERVER_CACHE_ERROR = ToGlobalServerErrorCode(111);

constexpr ServerError SERVER_LICENSE_FILE_NOT_EXIST = ToGlobalServerErrorCode(500);
constexpr ServerError SERVER_LICENSE_VALIDATION_FAIL = ToGlobalServerErrorCode(501);

constexpr ServerError DB_META_TRANSACTION_FAILED = ToGlobalServerErrorCode(1000);

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
}  // namespace milvus
}  // namespace zilliz

