/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include <stdio.h>
#include <cstring>
#include <assert.h>
#include "Status.h"

namespace zilliz {
namespace milvus {
namespace engine {

constexpr int CODE_WIDTH = sizeof(ErrorCode);

Status::Status(ErrorCode code, const std::string& msg) {
    //4 bytes store code
    //4 bytes store message length
    //the left bytes store message string
    const uint32_t length = (uint32_t)msg.size();
    char* result = new char[length + sizeof(length) + CODE_WIDTH];
    std::memcpy(result, &code, CODE_WIDTH);
    std::memcpy(result + CODE_WIDTH, &length, sizeof(length));
    memcpy(result + sizeof(length) + CODE_WIDTH, msg.data(), length);

    state_ = result;
}

Status::Status()
    : state_(nullptr) {

}

Status::~Status() {
    delete[] state_;
}

const char* Status::CopyState(const char* state) {
    uint32_t length = 0;
    std::memcpy(&length, state + CODE_WIDTH, sizeof(length));
    int buff_len = length + sizeof(length) + CODE_WIDTH;
    char* result = new char[buff_len];
    memcpy(result, state, buff_len);
    return result;
}

std::string Status::ToString() const {
    if (state_ == nullptr) return "OK";
    char tmp[32];
    const char* type;
    switch (code()) {
        case DB_SUCCESS:
            type = "OK";
            break;
        case DB_ERROR:
            type = "Error: ";
            break;
        case DB_META_TRANSACTION_FAILED:
            type = "DBTransactionError: ";
            break;
        case DB_NOT_FOUND:
            type = "NotFound: ";
            break;
        case DB_ALREADY_EXIST:
            type = "AlreadyExist: ";
            break;
        case DB_INVALID_PATH:
            type = "InvalidPath: ";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unkown code(%d): ",
                    static_cast<int>(code()));
            type = tmp;
            break;
    }

    std::string result(type);
    uint32_t length = 0;
    memcpy(&length, state_ + CODE_WIDTH, sizeof(length));
    result.append(state_ + sizeof(length) + CODE_WIDTH, length);
    return result;
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
