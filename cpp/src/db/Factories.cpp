////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <stdlib.h>
#include <time.h>
#include <sstream>

#include "Factories.h"


namespace zilliz {
namespace vecwise {
namespace engine {

DBMetaOptions DBMetaOptionsFactory::Build(const std::string& path) {
    auto p = path;
    if(p == "") {
        srand(time(nullptr));
        std::stringstream ss;
        ss << "/tmp/" << rand();
        p = ss.str();
    }
    DBMetaOptions meta;
    meta.path = p;
    return meta;
}

Options OptionsFactory::Build() {
    auto meta = DBMetaOptionsFactory::Build();
    Options options;
    options.meta = meta;
    return options;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
