#include <stdio.h>
#include <cstring>
#include <assert.h>
#include "status.h"

namespace zilliz {
namespace vecwise {
namespace engine {

const char* Status::CopyState(const char* state) {
    uint32_t size;
    std::memcpy(&size, state, sizeof(size));
    char* result = new char[size+5];
    memcpy(result, state, size+5);
    return result;
}

Status::Status(Code code, const std::string& msg, const std::string& msg2) {
    assert(code != kOK);
    const uint32_t len1 = msg.size();
    const uint32_t len2 = msg2.size();
    const uint32_t size = len1 + (len2 ? (2+len2) : 0);
    char* result = new char[size+5];
    std::memcpy(result, &size, sizeof(size));
    result[4] = static_cast<char>(code);
    memcpy(result+5, msg.data(), len1);
    if (len2) {
        result[5 + len1] = ':';
        result[6 + len1] = ' ';
        memcpy(result + 7 + len1, msg2.data(), len2);
    }
    state_ = result;
}

std::string Status::ToString() const {
    if (state_ == nullptr) return "OK";
    char tmp[30];
    const char* type;
    switch (code()) {
        case kOK:
            type = "OK";
            break;
        case kNotFound:
            type = "NotFound: ";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unkown code(%d): ",
                    static_cast<int>(code()));
            type = tmp;
            break;
    }

    std::string result(type);
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    result.append(state_ + 5, length);
    return result;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
