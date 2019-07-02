////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

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
#include "server/ServerConfig.h"

namespace zilliz {
namespace milvus {
namespace engine {

using std::string;
using std::unordered_map;
using std::vector;

class Nprobe {
 public:
    static Nprobe &GetInstance() {
        static Nprobe instance;
        return instance;
    }

    void SelectNprobe() {
        using namespace zilliz::milvus::server;
        ServerConfig &config = ServerConfig::GetInstance();
        ConfigNode engine_config = config.GetConfig(CONFIG_ENGINE);
        nprobe_ = engine_config.GetInt32Value(CONFIG_NPROBE, 1000);
    }

    size_t GetNprobe() {
        return nprobe_;
    }

 private:
    Nprobe() : nprobe_(1000) { SelectNprobe(); }

 private:
    size_t nprobe_;
};


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
        if(auto ivf_index = std::dynamic_pointer_cast<faiss::IndexIVF>(index_)) {
            ivf_index->nprobe = Nprobe::GetInstance().GetNprobe();
        }
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
    raw_index.reset(faiss::read_index(file_name.c_str()));
    return std::make_shared<Index>(raw_index);
}

}
}
}
