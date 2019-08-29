////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "knowhere/common/config.h"

using namespace zilliz::knowhere;

int main(){
    Config cfg;

    cfg["size"] = size_t(199);
    auto size = cfg.get_with_default("size", 123);
    auto size_2 = cfg["size"].as<int>();
    printf("%d", size_2);
}
