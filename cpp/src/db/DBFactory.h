////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "DB.h"
#include "Options.h"

#include <string>
#include <memory>

namespace zilliz {
namespace milvus {
namespace engine {

class DBFactory {
public:
    static Options BuildOption();

    static DBPtr Build(const Options& options);
};


}
}
}
