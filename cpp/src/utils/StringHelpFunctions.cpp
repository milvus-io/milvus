/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "StringHelpFunctions.h"

namespace zilliz {
namespace milvus {
namespace server {

void StringHelpFunctions::TrimStringBlank(std::string &string) {
    if (!string.empty()) {
        static std::string s_format(" \n\r\t");
        string.erase(0, string.find_first_not_of(s_format));
        string.erase(string.find_last_not_of(s_format) + 1);
    }
}

void StringHelpFunctions::TrimStringQuote(std::string &string, const std::string &qoute) {
    if (!string.empty()) {
        string.erase(0, string.find_first_not_of(qoute));
        string.erase(string.find_last_not_of(qoute) + 1);
    }
}

ServerError StringHelpFunctions::SplitStringByDelimeter(const std::string &str,
                                                        const std::string &delimeter,
                                                        std::vector<std::string> &result) {
    size_t last = 0;
    size_t index = str.find_first_of(delimeter, last);
    while (index != std::string::npos) {
        result.emplace_back(str.substr(last, index - last));
        last = index + 1;
        index = str.find_first_of(delimeter, last);
    }
    if (index - last > 0) {
        std::string temp = str.substr(last);
        result.emplace_back(temp);
    }

    return SERVER_SUCCESS;
}

ServerError StringHelpFunctions::SplitStringByQuote(const std::string &str,
                                                    const std::string &delimeter,
                                                    const std::string &quote,
                                                    std::vector<std::string> &result) {
    if (quote.empty()) {
        return SplitStringByDelimeter(str, delimeter, result);
    }

    size_t last = 0;
    size_t index = str.find_first_of(quote, last);
    if (index == std::string::npos) {
        return SplitStringByDelimeter(str, delimeter, result);
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
        if (index == std::string::npos) {
            return SERVER_UNEXPECTED_ERROR;
        }
        std::string quoted_text = postfix.substr(0, index);
        append_prefix += quoted_text;

        last = index + 1;
        index = postfix.find_first_of(delimeter, last);
        if (index != std::string::npos) {
            if (index > last) {
                append_prefix += postfix.substr(last, index - last);
            }
        } else {
            append_prefix += postfix.substr(last);
        }
        result.emplace_back(append_prefix);

        if (last == postfix.length()) {
            return SERVER_SUCCESS;
        }

        process_str = postfix.substr(index + 1);
        last = 0;
        index = process_str.find_first_of(quote, last);
    }

    if (!process_str.empty()) {
        return SplitStringByDelimeter(process_str, delimeter, result);
    }

    return SERVER_SUCCESS;
}

}
}
}