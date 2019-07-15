/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "EngineFactory.h"
#include "FaissExecutionEngine.h"
#include "Log.h"


namespace zilliz {
namespace milvus {
namespace engine {

ExecutionEnginePtr
EngineFactory::Build(uint16_t dimension,
                     const std::string &location,
                     EngineType type) {

    ExecutionEnginePtr execution_engine_ptr;

    switch (type) {
        case EngineType::FAISS_IDMAP: {
            execution_engine_ptr =
                ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, BUILD_INDEX_TYPE_IDMAP, "IDMap,Flat"));
            break;
        }

        case EngineType::FAISS_IVFFLAT: {
            execution_engine_ptr =
                ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, BUILD_INDEX_TYPE_IVF, "IDMap,Flat"));
            break;
        }

        case EngineType::FAISS_IVFSQ8: {
            execution_engine_ptr =
                    ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, BUILD_INDEX_TYPE_IVFSQ8, "IDMap,Flat"));
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

}
}
}