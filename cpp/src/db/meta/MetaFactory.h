////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "Meta.h"
#include "db/Options.h"

namespace zilliz {
namespace milvus {
namespace engine {

class MetaFactory {
public:
    static DBMetaOptions BuildOption(const std::string &path = "");

    static meta::MetaPtr Build(const DBMetaOptions &metaOptions, const int &mode);
};


}
}
}
