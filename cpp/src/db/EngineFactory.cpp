/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "EngineFactory.h"
//#include "FaissExecutionEngine.h"
#include "ExecutionEngineImpl.h"
#include "Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

#if 0
ExecutionEnginePtr
EngineFactory::Build(uint16_t dimension,
                     const std::string &location,
                     EngineType type) {

    ExecutionEnginePtr execution_engine_ptr;

    switch (type) {
        case EngineType::FAISS_IDMAP: {
            execution_engine_ptr =
                ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, "IDMap", "IDMap,Flat"));
            break;
        }

        case EngineType::FAISS_IVFFLAT_GPU: {
            execution_engine_ptr =
                ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, "IVF", "IDMap,Flat"));
            break;
        }

        default: {
            ENGINE_LOG_ERROR << "Unsupported engine type";
            return nullptr;
        }
    }

    execution_engine_ptr->Init();
    return execution_engine_ptr;
}
#else
ExecutionEnginePtr
EngineFactory::Build(uint16_t dimension,
                     const std::string &location,
                     EngineType type) {

    if(type == EngineType::INVALID) {
        ENGINE_LOG_ERROR << "Unsupported engine type";
        return nullptr;
    }

    ENGINE_LOG_DEBUG << "EngineFactory EngineTypee: " << int(type);
    ExecutionEnginePtr execution_engine_ptr =
            std::make_shared<ExecutionEngineImpl>(dimension, location, type);

    execution_engine_ptr->Init();
    return execution_engine_ptr;
}
#endif

}
}
}