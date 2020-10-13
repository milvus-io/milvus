
#include <iostream>
#include "EasyAssert.h"
// #define BOOST_STACKTRACE_USE_ADDR2LINE
#define BOOST_STACKTRACE_USE_BACKTRACE
#include <boost/stacktrace.hpp>


namespace milvus::impl {
void EasyAssertInfo(bool value, std::string_view expr_str, std::string_view filename, int lineno,
                    std::string_view extra_info) {
    if (!value) {
        std::string info;
        info += "Assert \"" + std::string(expr_str) + "\"";
        info += " at " + std::string(filename) + ":" + std::to_string(lineno) + "\n";
        if(!extra_info.empty()) {
            info += " => " + std::string(extra_info);
        }
        auto fuck = boost::stacktrace::stacktrace();
        std::cout << fuck;
        // std::string s = fuck;
        // info += ;
        throw std::runtime_error(info);
    }
}
}