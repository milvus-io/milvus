#pragma once
#include <string_view>

namespace milvus::impl {
inline
void EasyAssertInfo(bool value, std::string_view expr_str, std::string_view filename, int lineno,
                    std::string_view extra_info) {
    if (!value) {
        std::string info;
        info += "Assert \"" + std::string(expr_str) + "\"";
        info += " at " + std::string(filename) + ":" + std::to_string(lineno);
        info += " => " + std::string(extra_info);
        throw std::runtime_error(info);
    }
}
}

#define AssertInfo(expr, info) impl::EasyAssertInfo(bool(expr), #expr, __FILE__, __LINE__, (info))
#define Assert(expr) AssertInfo((expr), "")
