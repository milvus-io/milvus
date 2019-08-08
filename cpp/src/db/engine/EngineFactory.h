/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "db/Status.h"
#include "ExecutionEngine.h"

namespace zilliz {
namespace milvus {
namespace engine {

class EngineFactory {
public:
    static ExecutionEnginePtr Build(uint16_t dimension,
                                    const std::string& location,
                                    EngineType type);
};

}
}
}
