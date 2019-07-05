/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Status.h"

#include <exception>

namespace milvus {
class Exception : public std::exception {
public:
    Exception(StatusCode error_code,
            const std::string &message = std::string())
        : error_code_(error_code), message_(message) {}

public:
    StatusCode error_code() const {
        return error_code_;
    }

    virtual const char *what() const noexcept {
        return message_.c_str();
    }

private:
    StatusCode error_code_;
    std::string message_;
};
}