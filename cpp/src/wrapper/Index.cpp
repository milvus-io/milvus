////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#if 0
// TODO: maybe support static search
#ifdef GPU_VERSION

#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/gpu/StandardGpuResources.h"
#include "faiss/gpu/utils/DeviceUtils.h"


#endif

#include "Index.h"
#include "faiss/index_io.h"
#include "faiss/IndexIVF.h"
#include "faiss/IVFlib.h"
#include "faiss/IndexScalarQuantizer.h"
#include "server/ServerConfig.h"
#include "src/wrapper/FaissGpuResources.h"


namespace zilliz {
namespace milvus {
namespace engine {

using std::string;
using std::unordered_map;
using std::vector;

Index::Index(const std::shared_ptr<faiss::Index> &raw_index) {
    index_ = raw_index;
    dim = index_->d;
    ntotal = index_->ntotal;
    store_on_gpu = false;
}

bool Index::reset() {
    try {
        index_->reset();
        ntotal = index_->ntotal;
    }
    catch (std::exception &e) {
//        LOG(ERROR) << e.what();
        return false;
    }
    return true;
}

bool Index::add_with_ids(idx_t n, const float *xdata, const long *xids) {
    try {
        index_->add_with_ids(n, xdata, xids);
        ntotal += n;
    }
    catch (std::exception &e) {
//        LOG(ERROR) << e.what();
        return false;
    }
    return true;
}

bool Index::search(idx_t n, const float *data, idx_t k, float *distances, long *labels) const {
    try {
        index_->search(n, data, k, distances, labels);
    }
    catch (std::exception &e) {
//        LOG(ERROR) << e.what();
        return false;
    }
    return true;
}

void write_index(const Index_ptr &index, const std::string &file_name) {
    write_index(index->index_.get(), file_name.c_str());
}

Index_ptr read_index(const std::string &file_name) {
    std::shared_ptr<faiss::Index> raw_index = nullptr;
    faiss::Index *cpu_index = faiss::read_index(file_name.c_str());

    server::ServerConfig &config = server::ServerConfig::GetInstance();
    server::ConfigNode engine_config = config.GetConfig(server::CONFIG_ENGINE);
    bool use_hybrid_index_ = engine_config.GetBoolValue(server::CONFIG_USE_HYBRID_INDEX, false);

    if (dynamic_cast<faiss::IndexIVFScalarQuantizer *>(cpu_index) != nullptr && use_hybrid_index_) {

        int device_id = engine_config.GetInt32Value(server::CONFIG_HYBRID_INDEX_GPU, 0);
        auto gpu_resources = engine::FaissGpuResources::GetGpuResources(device_id);
        faiss::gpu::GpuClonerOptions clone_option;
        clone_option.storeInCpu = true;
        faiss::Index *gpu_index = faiss::gpu::index_cpu_to_gpu(gpu_resources.get(), device_id, cpu_index, &clone_option);

        delete cpu_index;
        raw_index.reset(gpu_index);
        return std::make_shared<Index>(raw_index);
    } else {
        raw_index.reset(cpu_index);
        return std::make_shared<Index>(raw_index);
    }
}

}
}
}
#endif
