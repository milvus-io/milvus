#pragma once

#include <exception>
#include <iostream>
#include <string>
#include <utility>

namespace knowhere {

class KnowhereException : public std::exception {
 public:
    explicit KnowhereException(std::string message)
        : message_(std::move(message)) {
    }

    const char*
    what() const noexcept override {
        return message_.c_str();
    }

 private:
    std::string message_;
};

inline void
SetThreadName(const std::string&) {
}

inline std::string
GetThreadName() {
    return "shim";
}

}  // namespace knowhere

#define LOG_KNOWHERE_TRACE_ std::clog
#define LOG_KNOWHERE_DEBUG_ std::clog
#define LOG_KNOWHERE_INFO_ std::clog
#define LOG_KNOWHERE_WARNING_ std::clog
#define LOG_KNOWHERE_ERROR_ std::clog
#define LOG_KNOWHERE_FATAL_ std::clog
