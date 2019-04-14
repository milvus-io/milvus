/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <cstdint>

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

}  // namespace server
}  // namespace vecwise
}  // namespace zilliz

