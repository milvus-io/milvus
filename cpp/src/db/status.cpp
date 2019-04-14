#include "status.h"

namespace vecengine {

const char* Status::CopyState(const char* state_) {
    uint32_t size;
    memcpy(&size, state_, sizeof(size));
    char result = new char[size+5];
    memcpy(result, state_, size+5);
    return result;
}

Status::Status(Code code_, const std::string& msg_, const std::string& msg2_) {
    assert(code_ != kOK);
    const uint32_t len1 = msg_.size();
    const uint32_t len2 = msg2_.size();
    const uint32_t size = len1 + (len2 ? (2+len2) : 0);
    char* result = new char[size+5];
    memcpy(result, &size, sizeof(size));
    result[4] = static_cast<char>(code);
    memcpy(result+5, msg_.data(), len1);
    if (len2) {
        result[5 + len1] = ':';
        result[6 + len1] = ' ';
        memcpy(result + 7 + len1, msg2_.data(), len2);
    }
    _state = result;
}

std::string Status::ToString() const {
    if (_state == nullptr) return "OK";
    char tmp[30];
    const char* type;
    switch (code()) {
        case kOK:
            type = "OK";
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

} // namespace vecengine
