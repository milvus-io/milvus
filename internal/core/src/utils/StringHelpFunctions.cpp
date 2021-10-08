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

#include "utils/StringHelpFunctions.h"
#include "utils/Log.h"

#include <fiu/fiu-local.h>
#include <algorithm>
#include <regex>
#include <string>

namespace milvus {

void
StringHelpFunctions::TrimStringBlank(std::string& string) {
    if (!string.empty()) {
        static std::string s_format(" \n\r\t");
        string.erase(0, string.find_first_not_of(s_format));
        string.erase(string.find_last_not_of(s_format) + 1);
    }
}

void
StringHelpFunctions::TrimStringQuote(std::string& string, const std::string& qoute) {
    if (!string.empty()) {
        string.erase(0, string.find_first_not_of(qoute));
        string.erase(string.find_last_not_of(qoute) + 1);
    }
}

void
StringHelpFunctions::SplitStringByDelimeter(const std::string& str,
                                            const std::string& delimeter,
                                            std::vector<std::string>& result) {
    if (str.empty()) {
        return;
    }

    size_t prev = 0;
    while (true) {
        size_t pos = str.find_first_of(delimeter, prev);
        if (pos == std::string::npos) {
            result.emplace_back(str.substr(prev));
            break;
        } else {
            result.emplace_back(str.substr(prev, pos - prev));
            prev = pos + 1;
        }
    }
}

void
StringHelpFunctions::MergeStringWithDelimeter(const std::vector<std::string>& strs,
                                              const std::string& delimeter,
                                              std::string& result) {
    if (strs.empty()) {
        result = "";
        return;
    }

    result = strs[0];
    for (size_t i = 1; i < strs.size(); i++) {
        result = result + delimeter + strs[i];
    }
}

Status
StringHelpFunctions::SplitStringByQuote(const std::string& str,
                                        const std::string& delimeter,
                                        const std::string& quote,
                                        std::vector<std::string>& result) {
    if (quote.empty()) {
        SplitStringByDelimeter(str, delimeter, result);
        return Status::OK();
    }

    size_t last = 0;
    size_t index = str.find_first_of(quote, last);
    if (index == std::string::npos) {
        SplitStringByDelimeter(str, delimeter, result);
        return Status::OK();
    }

    std::string process_str = str;
    while (index != std::string::npos) {
        std::string prefix = process_str.substr(last, index - last);
        std::string append_prefix;
        if (!prefix.empty()) {
            std::vector<std::string> prefix_split;
            SplitStringByDelimeter(prefix, delimeter, prefix_split);
            for (size_t i = 0; i < prefix_split.size() - 1; i++) {
                result.push_back(prefix_split[i]);
            }
            append_prefix = prefix_split[prefix_split.size() - 1];
        }
        last = index + 1;
        std::string postfix = process_str.substr(last);
        index = postfix.find_first_of(quote, 0);
        fiu_do_on("StringHelpFunctions.SplitStringByQuote.invalid_index", index = std::string::npos);

        if (index == std::string::npos) {
            return Status(SERVER_UNEXPECTED_ERROR, "");
        }
        std::string quoted_text = postfix.substr(0, index);
        append_prefix += quoted_text;

        last = index + 1;
        index = postfix.find_first_of(delimeter, last);
        fiu_do_on("StringHelpFunctions.SplitStringByQuote.index_gt_last", last = 0);
        fiu_do_on("StringHelpFunctions.SplitStringByQuote.invalid_index2", index = std::string::npos);

        if (index != std::string::npos) {
            if (index > last) {
                append_prefix += postfix.substr(last, index - last);
            }
        } else {
            append_prefix += postfix.substr(last);
        }
        result.emplace_back(append_prefix);
        fiu_do_on("StringHelpFunctions.SplitStringByQuote.last_is_end", last = postfix.length());

        if (last == postfix.length()) {
            return Status::OK();
        }

        process_str = postfix.substr(index + 1);
        last = 0;
        index = process_str.find_first_of(quote, last);
    }

    if (!process_str.empty()) {
        SplitStringByDelimeter(process_str, delimeter, result);
    }

    return Status::OK();
}

bool
StringHelpFunctions::IsRegexMatch(const std::string& target_str, const std::string& pattern_str) {
    // if target_str equals pattern_str, return true
    if (target_str == pattern_str) {
        return true;
    }

    // regex match
    // for illegal regex expression, the std::regex will throw exception, regard as unmatch
    try {
        std::regex pattern(pattern_str);
        std::smatch results;
        return std::regex_match(target_str, results, pattern);
    } catch (std::exception& e) {
        LOG_SERVER_ERROR_ << "Regex exception: " << e.what();
    }

    return false;
}

Status
StringHelpFunctions::ConvertToBoolean(const std::string& str, bool& value) {
    std::string s = str;
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    value = s == "true" || s == "on" || s == "yes" || s == "1";

    return Status::OK();
}

}  // namespace milvus
