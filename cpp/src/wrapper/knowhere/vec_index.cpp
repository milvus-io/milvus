////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/index/vector_index/gpu_ivf.h"

#include "vec_index.h"
#include "vec_impl.h"


namespace zilliz {
namespace vecwise {
namespace engine {

VecIndexPtr GetVecIndexFactory(const std::string &index_type) {
    std::shared_ptr<zilliz::knowhere::VectorIndex> index;
    if (index_type == "IVF") {
        index = std::make_shared<zilliz::knowhere::IVF>();
    } else if (index_type == "GPUIVF") {
        index = std::make_shared<zilliz::knowhere::GPUIVF>();
    }
    auto ret_index = std::make_shared<VecIndexImpl>(index);
    //return std::static_pointer_cast<VecIndex>(std::make_shared<VecIndexImpl>(index));
    return std::make_shared<VecIndexImpl>(index);
}

}
}
}
