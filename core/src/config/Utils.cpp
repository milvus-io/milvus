// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "config/Utils.h"

#include <algorithm>
#include <sstream>
#include <unordered_map>

namespace milvus {
namespace server {

std::unordered_map<std::string, int64_t> BYTE_UNITS = {
    {"b", 1},
    {"k", 1024},
    {"m", 1024 * 1024},
    {"g", 1024 * 1024 * 1024},
};

bool
is_number(const std::string& s) {
    return !s.empty() && std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isdigit(c); }) == s.end();
}

bool
is_alpha(const std::string& s) {
    return !s.empty() && std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isalpha(c); }) == s.end();
}

std::string
str_tolower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

int64_t
parse_bytes(const std::string& str, std::string& err) {
    try {
        std::string s = str;
        if (is_number(s))
            return std::stoll(s);
        if (s.length() == 0)
            return 0;

        auto last_two = s.substr(s.length() - 2, 2);
        auto last_one = s.substr(s.length() - 1);
        if (is_alpha(last_two) && is_alpha(last_one))
            if (last_one == "b" or last_one == "B")
                s = s.substr(0, s.length() - 1);
        auto& units = BYTE_UNITS;
        auto suffix = str_tolower(s.substr(s.length() - 1));

        std::string digits_part;
        if (is_number(suffix)) {
            digits_part = s;
            suffix = 'b';
        } else {
            digits_part = s.substr(0, s.length() - 1);
        }

        if (units.find(suffix) != units.end() or is_number(suffix)) {
            auto digits = std::stoll(digits_part);
            return digits * units[suffix];
        } else {
            std::stringstream ss;
            ss << "The specified value for memory (" << str << ") should specify the units."
               << "The postfix should be one of the `b` `k` `m` `g` characters";
            err = ss.str();
        }
    } catch (...) {
        err = "Unknown error happened on parse bytes.";
    }
    return 0;
}

}  // namespace server
}  // namespace milvus
