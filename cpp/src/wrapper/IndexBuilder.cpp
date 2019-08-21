////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#if 0
#include "mutex"


#ifdef GPU_VERSION
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuAutoTune.h>
#endif


#include <faiss/IndexFlat.h>
#include <easylogging++.h>
#include "faiss/IndexScalarQuantizer.h"

#include "server/ServerConfig.h"
#include "IndexBuilder.h"
#include "FaissGpuResources.h"


namespace zilliz {
namespace milvus {
namespace engine {

using std::vector;

static std::mutex gpu_resource;
static std::mutex cpu_resource;

IndexBuilder::IndexBuilder(const Operand_ptr &opd) {
    opd_ = opd;

    using namespace zilliz::milvus::server;
    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
    use_hybrid_index_ = engine_config.GetBoolValue(CONFIG_USE_HYBRID_INDEX, false);
    hybrid_index_device_id_ = engine_config.GetInt32Value(server::CONFIG_HYBRID_INDEX_GPU, 0);
}

// Default: build use gpu
Index_ptr IndexBuilder::build_all(const long &nb,
                                  const float *xb,
                                  const long *ids,
                                  const long &nt,
                                  const float *xt) {
    std::shared_ptr<faiss::Index> host_index = nullptr;
#ifdef GPU_VERSION
    {
        LOG(DEBUG) << "Build index by GPU";
        // TODO: list support index-type.
        faiss::MetricType metric_type = opd_->metric_type == "L2" ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;
        faiss::Index *ori_index = faiss::index_factory(opd_->d, opd_->get_index_type(nb).c_str(), metric_type);

        std::lock_guard<std::mutex> lk(gpu_resource);

#ifdef UNITTEST_ONLY
        faiss::gpu::StandardGpuResources res;
        int device_id = 0;
        faiss::gpu::GpuClonerOptions clone_option;
        clone_option.storeInCpu = use_hybrid_index_;
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, device_id, ori_index, &clone_option);
#else
        engine::FaissGpuResources res;
        int device_id = res.GetGpu();
        auto gpu_resources = engine::FaissGpuResources::GetGpuResources(device_id);
        faiss::gpu::GpuClonerOptions clone_option;
        clone_option.storeInCpu = use_hybrid_index_;
        auto device_index = faiss::gpu::index_cpu_to_gpu(gpu_resources.get(), device_id, ori_index, &clone_option);
#endif

        if (!device_index->is_trained) {
            nt == 0 || xt == nullptr ? device_index->train(nb, xb)
                                     : device_index->train(nt, xt);
        }
        device_index->add_with_ids(nb, xb, ids); // TODO: support with add_with_IDMAP

        if (dynamic_cast<faiss::IndexIVFScalarQuantizer*>(ori_index) != nullptr
            && use_hybrid_index_) {
            std::shared_ptr<faiss::Index> device_hybrid_index = nullptr;
            if (hybrid_index_device_id_ != device_id) {
                auto host_hybrid_index = faiss::gpu::index_gpu_to_cpu(device_index);
                auto hybrid_gpu_resources = engine::FaissGpuResources::GetGpuResources(hybrid_index_device_id_);
                auto another_device_index = faiss::gpu::index_cpu_to_gpu(hybrid_gpu_resources.get(),
                                                                         hybrid_index_device_id_,
                                                                         host_hybrid_index,
                                                                         &clone_option);
                device_hybrid_index.reset(another_device_index);
                delete device_index;
                delete host_hybrid_index;
            } else {
                device_hybrid_index.reset(device_index);
            }
            delete ori_index;
            return std::make_shared<Index>(device_hybrid_index);
        }

        host_index.reset(faiss::gpu::index_gpu_to_cpu(device_index));

        delete device_index;
        delete ori_index;
    }
#else
    {
        LOG(DEBUG) << "Build index by CPU";
        faiss::MetricType metric_type = opd_->metric_type == "L2" ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;
        faiss::Index *index = faiss::index_factory(opd_->d, opd_->get_index_type(nb).c_str(), metric_type);
        if (!index->is_trained) {
            nt == 0 || xt == nullptr ? index->train(nb, xb)
                                     : index->train(nt, xt);
        }
        index->add_with_ids(nb, xb, ids);
        host_index.reset(index);
    }
#endif

    return std::make_shared<Index>(host_index);
}

Index_ptr IndexBuilder::build_all(const long &nb, const vector<float> &xb,
                                  const vector<long> &ids,
                                  const long &nt, const vector<float> &xt) {
    return build_all(nb, xb.data(), ids.data(), nt, xt.data());
}

BgCpuBuilder::BgCpuBuilder(const zilliz::milvus::engine::Operand_ptr &opd) : IndexBuilder(opd) {};

Index_ptr BgCpuBuilder::build_all(const long &nb, const float *xb, const long *ids, const long &nt, const float *xt) {
    std::shared_ptr<faiss::Index> index = nullptr;
    faiss::MetricType metric_type = opd_->metric_type == "L2" ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;
    index.reset(faiss::index_factory(opd_->d, opd_->get_index_type(nb).c_str(), metric_type));

    LOG(DEBUG) << "Build index by CPU";
    {
        std::lock_guard<std::mutex> lk(cpu_resource);
        if (!index->is_trained) {
            nt == 0 || xt == nullptr ? index->train(nb, xb)
                                     : index->train(nt, xt);
        }
        index->add_with_ids(nb, xb, ids);
    }

    return std::make_shared<Index>(index);
}

IndexBuilderPtr GetIndexBuilder(const Operand_ptr &opd) {
    if (opd->index_type == "IDMap") {
        IndexBuilderPtr index = nullptr;
        return std::make_shared<BgCpuBuilder>(opd);
    }

    return std::make_shared<IndexBuilder>(opd);
}

}
}
}
#endif
