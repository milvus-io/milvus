////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/index/vector_index/idmap.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/cpu_kdt_rng.h"

#include "vec_index.h"
#include "vec_impl.h"


namespace zilliz {
namespace milvus {
namespace engine {

// TODO(linxj): index_type => enum struct
VecIndexPtr GetVecIndexFactory(const IndexType &type) {
    std::shared_ptr<zilliz::knowhere::VectorIndex> index;
    switch (type) {
        case IndexType::FAISS_IDMAP: {
            index = std::make_shared<zilliz::knowhere::IDMAP>();
            return std::make_shared<BFIndex>(index);
        }
        case IndexType::FAISS_IVFFLAT_CPU: {
            index = std::make_shared<zilliz::knowhere::IVF>();
            break;
        }
        case IndexType::FAISS_IVFFLAT_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVF>(0);
            break;
        }
        case IndexType::FAISS_IVFPQ_CPU: {
            index = std::make_shared<zilliz::knowhere::IVFPQ>();
            break;
        }
        case IndexType::FAISS_IVFPQ_GPU: {
            index = std::make_shared<zilliz::knowhere::GPUIVFPQ>(0);
            break;
        }
        case IndexType::SPTAG_KDT_RNT_CPU: {
            index = std::make_shared<zilliz::knowhere::CPUKDTRNG>();
            break;
        }
        //case IndexType::NSG: { // TODO(linxj): bug.
        //    index = std::make_shared<zilliz::knowhere::NSG>();
        //    break;
        //}
        default: {
            return nullptr;
        }
    }
    return std::make_shared<VecIndexImpl>(index);
}

VecIndexPtr LoadVecIndex(const IndexType &index_type, const zilliz::knowhere::BinarySet &index_binary) {
    auto index = GetVecIndexFactory(index_type);
    index->Load(index_binary);
    return index;
}

}
}
}
