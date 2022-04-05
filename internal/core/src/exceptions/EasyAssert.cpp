// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include "EasyAssert.h"
// #define BOOST_STACKTRACE_USE_BACKTRACE
#include <boost/stacktrace.hpp>
#include <sstream>

namespace milvus::impl {

std::string
EasyStackTrace() {
    std::string output;
#ifdef BOOST_STACKTRACE_USE_BACKTRACE
    auto stack_info = boost::stacktrace::stacktrace();
    std::ostringstream ss;
    ss << stack_info;
    output = std::string(ss.str());
#endif
    return output;
}

void
EasyAssertInfo(bool value,
               std::string_view expr_str,
               std::string_view filename,
               int lineno,
               std::string_view extra_info,
               ErrorCodeEnum error_code) {
    // enable error code
    if (!value) {
        std::string info;
        info += "Assert \"" + std::string(expr_str) + "\"";
        info += " at " + std::string(filename) + ":" + std::to_string(lineno) + "\n";
        if (!extra_info.empty()) {
            info += " => " + std::string(extra_info);
        }
        std::cout << info << std::endl;
        throw SegcoreError(error_code, std::string(info));
    }
}

}  // namespace milvus::impl
