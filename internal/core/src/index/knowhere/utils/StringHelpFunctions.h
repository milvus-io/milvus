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

#pragma once

#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {

class StringHelpFunctions {
 private:
    StringHelpFunctions() = default;

 public:
    // trim blanks from begin and end
    // "  a b c  "  =>  "a b c"
    static void
    TrimStringBlank(std::string& string);

    // trim quotes from begin and end
    // "'abc'"  =>  "abc"
    static void
    TrimStringQuote(std::string& string, const std::string& qoute);

    // split string by delimeter ','
    // a,b,c            a | b | c
    // a,b,             a | b |
    // ,b,c               | b | c
    // ,b,                | b |
    // ,,                 |   |
    // a                    a
    static void
    SplitStringByDelimeter(const std::string& str, const std::string& delimeter, std::vector<std::string>& result);

    // merge strings with delimeter
    // "a", "b", "c"  => "a,b,c"
    static void
    MergeStringWithDelimeter(const std::vector<std::string>& strs, const std::string& delimeter, std::string& result);

    // assume the collection has two columns, quote='\"', delimeter=','
    //  a,b             a | b
    //  "aa,gg,yy",b    aa,gg,yy | b
    //  aa"dd,rr"kk,pp  aadd,rrkk | pp
    //  "aa,bb"         aa,bb
    //  55,1122\"aa,bb\",yyy,\"kkk\"    55 | 1122aa,bb | yyy | kkk
    //  "55,1122"aa,bb",yyy,"kkk"   illegal
    static Status
    SplitStringByQuote(const std::string& str,
                       const std::string& delimeter,
                       const std::string& quote,
                       std::vector<std::string>& result);

    // std regex match function
    // regex grammar reference: http://www.cplusplus.com/reference/regex/ECMAScript/
    static bool
    IsRegexMatch(const std::string& target_str, const std::string& pattern);

    // conversion rules refer to ValidationUtil::ValidateStringIsBool()
    // "true", "on", "yes", "1" ==> true
    // "false", "off", "no", "0", "" ==> false
    static Status
    ConvertToBoolean(const std::string& str, bool& value);
};

}  // namespace milvus
