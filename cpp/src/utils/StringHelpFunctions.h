/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "./Error.h"

#include <vector>

namespace zilliz {
namespace vecwise {
namespace server {

class StringHelpFunctions {
private:
    StringHelpFunctions() = default;

public:
    static void TrimStringLineBreak(std::string &string);

    static void TrimStringBlank(std::string &string);

    static void TrimStringQuote(std::string &string, const std::string &qoute);

    //split string by delimeter ','
    // a,b,c            a | b | c
    // a,b,             a | b |
    // ,b,c               | b | c
    // ,b,                | b |
    // ,,                 |   |
    // a                    a
    static ServerError SplitStringByDelimeter(const std::string &str,
                                              const std::string &delimeter,
                                              std::vector<std::string> &result);

    //assume the table has two columns, quote='\"', delimeter=','
    //  a,b             a | b
    //  "aa,gg,yy",b    aa,gg,yy | b
    //  aa"dd,rr"kk,pp  aadd,rrkk | pp
    //  "aa,bb"         aa,bb
    //  55,1122\"aa,bb\",yyy,\"kkk\"    55 | 1122aa,bb | yyy | kkk
    //  "55,1122"aa,bb",yyy,"kkk"   illegal
    static ServerError SplitStringByQuote(const std::string &str,
                                          const std::string &delimeter,
                                          const std::string &quote,
                                          std::vector<std::string> &result);

};

}
}
}
