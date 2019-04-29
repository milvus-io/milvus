////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include "db/DB.h"

namespace zilliz {
namespace vecwise {
namespace engine {

struct DBMetaOptionsFactory {
    static DBMetaOptions Build(const std::string& path = "");
};

struct OptionsFactory {
    static Options Build();
};


} // namespace engine
} // namespace vecwise
} // namespace zilliz
