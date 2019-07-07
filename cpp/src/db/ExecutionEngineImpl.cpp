/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "ExecutionEngineImpl.h"
#include "Log.h"

#include "wrapper/knowhere/vec_impl.h"
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/cpu_kdt_rng.h"

namespace zilliz {
namespace milvus {
namespace engine {


ExecutionEngineImpl::ExecutionEngineImpl(uint16_t dimension,
        const std::string& location,
        EngineType type)
    : location_(location) {
    index_ = CreatetVecIndex(type);
}

vecwise::engine::VecIndexPtr ExecutionEngineImpl::CreatetVecIndex(EngineType type) {
    std::shared_ptr<zilliz::knowhere::VectorIndex> index;
    switch(type) {
        case EngineType::FAISS_IDMAP: {

            break;
        }
        case EngineType::FAISS_IVFFLAT_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVF>(0);
            break;
        }
        case EngineType::FAISS_IVFFLAT_CPU: {
            index = std::make_shared<zilliz::knowhere::IVF>();
            break;
        }
        case EngineType::SPTAG_KDT_RNT_CPU: {
            index = std::make_shared<zilliz::knowhere::CPUKDTRNG>();
            break;
        }
        default:{
            ENGINE_LOG_ERROR << "Invalid engine type";
            return nullptr;
        }
    }

    return std::make_shared<vecwise::engine::VecIndexImpl>(index);
}

Status ExecutionEngineImpl::AddWithIds(long n, const float *xdata, const long *xids) {

    return Status::OK();
}

size_t ExecutionEngineImpl::Count() const {
    return 0;
}

size_t ExecutionEngineImpl::Size() const {
    return 0;
}

size_t ExecutionEngineImpl::Dimension() const {
    return 0;
}

size_t ExecutionEngineImpl::PhysicalSize() const {
    return 0;
}

Status ExecutionEngineImpl::Serialize() {
    return Status::OK();
}

Status ExecutionEngineImpl::Load() {

    return Status::OK();
}

Status ExecutionEngineImpl::Merge(const std::string& location) {

    return Status::OK();
}

ExecutionEnginePtr
ExecutionEngineImpl::BuildIndex(const std::string& location) {
    return nullptr;
}

Status ExecutionEngineImpl::Search(long n,
                                    const float *data,
                                    long k,
                                    float *distances,
                                    long *labels) const {

    return Status::OK();
}

Status ExecutionEngineImpl::Cache() {

    return Status::OK();
}

Status ExecutionEngineImpl::Init() {

    return Status::OK();
}


} // namespace engine
} // namespace milvus
} // namespace zilliz
