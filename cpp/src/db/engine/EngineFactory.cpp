/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "EngineFactory.h"
#include "ExecutionEngineImpl.h"
#include "db/Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

ExecutionEnginePtr
EngineFactory::Build(uint16_t dimension,
                     const std::string &location,
                     EngineType index_type,
                     MetricType metric_type,
                     int32_t nlist) {

    if(index_type == EngineType::INVALID) {
        ENGINE_LOG_ERROR << "Unsupported engine type";
        return nullptr;
    }

    ENGINE_LOG_DEBUG << "EngineFactory EngineTypee: " << (int)index_type;
    ExecutionEnginePtr execution_engine_ptr =
            std::make_shared<ExecutionEngineImpl>(dimension, location, index_type, metric_type, nlist);

    execution_engine_ptr->Init();
    return execution_engine_ptr;
}

}
}
}