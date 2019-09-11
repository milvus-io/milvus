////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "MemManager.h"
#include "db/meta/Meta.h"

namespace zilliz {
namespace milvus {
namespace engine {

class MemManagerFactory {
public:
    static MemManagerPtr Build(const std::shared_ptr<meta::Meta> &meta, const Options &options);
};

}
}
}
