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
        const std::string& location,
        EngineType type) {
    switch(type) {
        case EngineType::FAISS_IDMAP:
            return ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, "IDMap", "IDMap,Flat"));
        case EngineType::FAISS_IVFFLAT:
            return ExecutionEnginePtr(new FaissExecutionEngine(dimension, location, "IVF", "IDMap,Flat"));
        default:
            ENGINE_LOG_ERROR << "Unsupportted engine type";
            return nullptr;
    }
}

}
}
}