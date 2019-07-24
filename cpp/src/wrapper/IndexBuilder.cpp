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


#include "server/ServerConfig.h"
#include "IndexBuilder.h"


namespace zilliz {
namespace milvus {
namespace engine {

class GpuResources {
 public:
    static GpuResources &GetInstance() {
        static GpuResources instance;
        return instance;
    }

    void SelectGpu() {
        using namespace zilliz::milvus::server;
        ServerConfig &config = ServerConfig::GetInstance();
        ConfigNode server_config = config.GetConfig(CONFIG_SERVER);
        gpu_num = server_config.GetInt32Value(server::CONFIG_GPU_INDEX, 0);
    }

    int32_t GetGpu() {
        return gpu_num;
    }

 private:
    GpuResources() : gpu_num(0) { SelectGpu(); }

 private:
    int32_t gpu_num;
};

using std::vector;

static std::mutex gpu_resource;
static std::mutex cpu_resource;

IndexBuilder::IndexBuilder(const Operand_ptr &opd) {
    opd_ = opd;
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
        faiss::gpu::StandardGpuResources res;
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, GpuResources::GetInstance().GetGpu(), ori_index);
        if (!device_index->is_trained) {
            nt == 0 || xt == nullptr ? device_index->train(nb, xb)
                                     : device_index->train(nt, xt);
        }
        device_index->add_with_ids(nb, xb, ids); // TODO: support with add_with_IDMAP

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
