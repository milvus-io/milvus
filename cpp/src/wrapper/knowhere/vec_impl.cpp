////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <src/utils/Log.h>
#include "knowhere/index/vector_index/idmap.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/common/exception.h"

#include "vec_impl.h"
#include "data_transfer.h"
#include "wrapper_log.h"


namespace zilliz {
namespace milvus {
namespace engine {

using namespace zilliz::knowhere;

server::KnowhereError VecIndexImpl::BuildAll(const long &nb,
                                             const float *xb,
                                             const long *ids,
                                             const Config &cfg,
                                             const long &nt,
                                             const float *xt) {
    try {
        dim = cfg["dim"].as<int>();
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        auto preprocessor = index_->BuildPreprocessor(dataset, cfg);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(dataset, cfg);
        index_->set_index_model(model);
        index_->Add(dataset, cfg);
    } catch (KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (jsoncons::json_exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_INVALID_ARGUMENT;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}

server::KnowhereError VecIndexImpl::Add(const long &nb, const float *xb, const long *ids, const Config &cfg) {
    try {
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        index_->Add(dataset, cfg);
    } catch (KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (jsoncons::json_exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_INVALID_ARGUMENT;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}

server::KnowhereError VecIndexImpl::Search(const long &nq, const float *xq, float *dist, long *ids, const Config &cfg) {
    try {
        auto k = cfg["k"].as<int>();
        auto dataset = GenDataset(nq, dim, xq);

        Config search_cfg;
        auto res = index_->Search(dataset, cfg);
        auto ids_array = res->array()[0];
        auto dis_array = res->array()[1];

        //{
        //    auto& ids = ids_array;
        //    auto& dists = dis_array;
        //    std::stringstream ss_id;
        //    std::stringstream ss_dist;
        //    for (auto i = 0; i < 10; i++) {
        //        for (auto j = 0; j < k; ++j) {
        //            ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
        //            ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
        //        }
        //        ss_id << std::endl;
        //        ss_dist << std::endl;
        //    }
        //    std::cout << "id\n" << ss_id.str() << std::endl;
        //    std::cout << "dist\n" << ss_dist.str() << std::endl;
        //}

        auto p_ids = ids_array->data()->GetValues<int64_t>(1, 0);
        auto p_dist = dis_array->data()->GetValues<float>(1, 0);

        // TODO(linxj): avoid copy here.
        memcpy(ids, p_ids, sizeof(int64_t) * nq * k);
        memcpy(dist, p_dist, sizeof(float) * nq * k);
    } catch (KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (jsoncons::json_exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_INVALID_ARGUMENT;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}

zilliz::knowhere::BinarySet VecIndexImpl::Serialize() {
    return index_->Serialize();
}

server::KnowhereError VecIndexImpl::Load(const zilliz::knowhere::BinarySet &index_binary) {
    index_->Load(index_binary);
    dim = Dimension();
    return server::KNOWHERE_SUCCESS;
}

int64_t VecIndexImpl::Dimension() {
    return index_->Dimension();
}

int64_t VecIndexImpl::Count() {
    return index_->Count();
}

IndexType VecIndexImpl::GetType() {
    return type;
}

float *BFIndex::GetRawVectors() {
    auto raw_index = std::dynamic_pointer_cast<IDMAP>(index_);
    if (raw_index) { return raw_index->GetRawVectors(); }
    return nullptr;
}

int64_t *BFIndex::GetRawIds() {
    return std::static_pointer_cast<IDMAP>(index_)->GetRawIds();
}

server::KnowhereError BFIndex::Build(const int64_t &d) {
    try {
        dim = d;
        std::static_pointer_cast<IDMAP>(index_)->Train(dim);
    } catch (KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (jsoncons::json_exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_INVALID_ARGUMENT;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}

server::KnowhereError BFIndex::BuildAll(const long &nb,
                                        const float *xb,
                                        const long *ids,
                                        const Config &cfg,
                                        const long &nt,
                                        const float *xt) {
    try {
        dim = cfg["dim"].as<int>();
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        std::static_pointer_cast<IDMAP>(index_)->Train(dim);
        index_->Add(dataset, cfg);
    } catch (KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (jsoncons::json_exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_INVALID_ARGUMENT;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}

// TODO(linxj): add lock here.
server::KnowhereError IVFMixIndex::BuildAll(const long &nb,
                                            const float *xb,
                                            const long *ids,
                                            const Config &cfg,
                                            const long &nt,
                                            const float *xt) {
    try {
        dim = cfg["dim"].as<int>();
        auto dataset = GenDatasetWithIds(nb, dim, xb, ids);

        auto preprocessor = index_->BuildPreprocessor(dataset, cfg);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(dataset, cfg);
        index_->set_index_model(model);
        index_->Add(dataset, cfg);

        if (auto device_index = std::dynamic_pointer_cast<GPUIVF>(index_)) {
            auto host_index = device_index->Copy_index_gpu_to_cpu();
            index_ = host_index;
        } else {
            WRAPPER_LOG_ERROR << "Build IVFMIXIndex Failed";
        }
    } catch (KnowhereException &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_UNEXPECTED_ERROR;
    } catch (jsoncons::json_exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_INVALID_ARGUMENT;
    } catch (std::exception &e) {
        WRAPPER_LOG_ERROR << e.what();
        return server::KNOWHERE_ERROR;
    }
    return server::KNOWHERE_SUCCESS;
}

server::KnowhereError IVFMixIndex::Load(const zilliz::knowhere::BinarySet &index_binary) {
    index_ = std::make_shared<IVF>();
    index_->Load(index_binary);
    dim = Dimension();
    return server::KNOWHERE_SUCCESS;
}

}
}
}
