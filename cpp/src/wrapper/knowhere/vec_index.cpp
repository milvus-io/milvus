////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/cpu_kdt_rng.h"

#include "vec_index.h"
#include "vec_impl.h"


namespace zilliz {
namespace vecwise {
namespace engine {

// TODO(linxj): index_type => enum struct
VecIndexPtr GetVecIndexFactory(const std::string &index_type) {
    std::shared_ptr<zilliz::knowhere::VectorIndex> index;
    if (index_type == "IVF") {
        index = std::make_shared<zilliz::knowhere::IVF>();
    } else if (index_type == "GPUIVF") {
        index = std::make_shared<zilliz::knowhere::GPUIVF>(0);
    } else if (index_type == "SPTAG") {
        index = std::make_shared<zilliz::knowhere::CPUKDTRNG>();
    }
    // TODO(linxj): Support NSG
    //else if (index_type == "NSG") {
    //    index = std::make_shared<zilliz::knowhere::NSG>();
    //}
    return std::make_shared<VecIndexImpl>(index);
}

VecIndexPtr LoadVecIndex(const std::string &index_type, const zilliz::knowhere::BinarySet &index_binary) {
    auto index = GetVecIndexFactory(index_type);
    index->Load(index_binary);
    return index;
}

}
}
}
