////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "knowhere/common/exception.h"
#include <cstdio>

namespace zilliz {
namespace knowhere {


KnowhereException::KnowhereException(const std::string &msg):msg(msg) {}

KnowhereException::KnowhereException(const std::string &m, const char *funcName, const char *file, int line) {
    int size = snprintf(nullptr, 0, "Error in %s at %s:%d: %s",
                        funcName, file, line, m.c_str());
    msg.resize(size + 1);
    snprintf(&msg[0], msg.size(), "Error in %s at %s:%d: %s",
             funcName, file, line, m.c_str());
}

const char *KnowhereException::what() const noexcept {
    return msg.c_str();
}

}
}