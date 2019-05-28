/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <exception>
#include <string>

namespace zilliz {
namespace vecwise {
namespace engine {

class Exception : public std::exception {
public:
    Exception(const std::string& message)
        : message_(message) {
        }

    Exception()
        : message_() {
        }

    virtual const char* what() const throw() {
        if (message_.empty()) {
            return "Default Exception.";
        } else {
            return message_.c_str();
        }
    }

    virtual ~Exception() throw() {};

protected:

    std::string message_;
};

class InvalidArgumentException : public Exception {
public:
    InvalidArgumentException() : Exception("Invalid Argument"){};
    InvalidArgumentException(const std::string& message) : Exception(message) {};
};

class OutOfRangeException : public Exception {
public:
    OutOfRangeException() : Exception("Out Of Range"){};
    OutOfRangeException(const std::string& message) : Exception(message) {};
};

} // namespace engine
} // namespace vecwise
} // namespace zilliz
