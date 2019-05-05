////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "mutex"

#include <faiss/gpu/StandardGpuResources.h>
#include "faiss/gpu/GpuIndexIVFFlat.h"
#include "faiss/gpu/GpuAutoTune.h"

#include "IndexBuilder.h"


namespace zilliz {
namespace vecwise {
namespace engine {

using std::vector;

static std::mutex gpu_resource;

IndexBuilder::IndexBuilder(const Operand_ptr &opd) {
    opd_ = opd;
}

// Default: build use gpu
Index_ptr IndexBuilder::build_all(const long &nb,
                                  const float* xb,
                                  const long* ids,
                                  const long &nt,
                                  const float* xt) {
    std::shared_ptr<faiss::Index> host_index = nullptr;
    {
        // TODO: list support index-type.
        faiss::Index *ori_index = faiss::index_factory(opd_->d, opd_->index_type.c_str());

        std::lock_guard<std::mutex> lk(gpu_resource);
        faiss::gpu::StandardGpuResources res;
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 0, ori_index);
        if (!device_index->is_trained) {
            nt == 0 || xt == nullptr ? device_index->train(nb, xb)
                                     : device_index->train(nt, xt);
        }
        device_index->add_with_ids(nb, xb, ids);

        host_index.reset(faiss::gpu::index_gpu_to_cpu(device_index));

        delete device_index;
        delete ori_index;
    }

    return std::make_shared<Index>(host_index);
}

Index_ptr IndexBuilder::build_all(const long &nb, const vector<float> &xb,
                                  const vector<long> &ids,
                                  const long &nt, const vector<float> &xt) {
    return build_all(nb, xb.data(), ids.data(), nt, xt.data());
}

// Be Factory pattern later
IndexBuilderPtr GetIndexBuilder(const Operand_ptr &opd) {
    return std::make_shared<IndexBuilder>(opd);
}

}
}
}
