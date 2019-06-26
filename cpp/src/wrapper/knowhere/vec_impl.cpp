////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "knowhere/index/index.h"
#include "knowhere/index/index_model.h"
#include "knowhere/index/index_type.h"
#include "knowhere/adapter/sptag.h"
#include "knowhere/common/tensor.h"

#include "vec_impl.h"
#include "data_transfer.h"

//using Index = zilliz::knowhere::Index;
//using IndexModel = zilliz::knowhere::IndexModel;
//using IndexType = zilliz::knowhere::IndexType;
//using IndexPtr = std::shared_ptr<Index>;
//using IndexModelPtr = std::shared_ptr<IndexModel>;

namespace zilliz {
namespace vecwise {
namespace engine {

using namespace zilliz::knowhere;

void VecIndexImpl::BuildAll(const long &nb,
                            const float *xb,
                            const long *ids,
                            const Config &cfg,
                            const long &nt,
                            const float *xt) {
    using namespace zilliz::knowhere;

    auto d = cfg["dim"].as<int>();
    GENDATASET(nb, d, xb, ids)

    Config train_cfg;
    Config add_cfg;
    Config search_cfg;
    auto model = index_->Train(dataset, cfg);
    index_->set_index_model(model);
    index_->Add(dataset, add_cfg);
}

void VecIndexImpl::Add(const long &nb, const float *xb, const long *ids, const Config &cfg) {
    // TODO: Assert index is trained;

    auto d = cfg["dim"].as<int>();
    GENDATASET(nb, d, xb, ids)

    index_->Add(dataset, cfg);
}

void VecIndexImpl::Search(const long &nq, const float *xq, float *dist, long *ids, const Config &cfg) {
    // TODO: Assert index is trained;

    auto d = cfg["dim"].as<int>();
    auto k = cfg["k"].as<int>();
    GENQUERYDATASET(nq, d, xq)

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

    // TODO: deep copy here.
    auto p_ids = ids_array->data()->GetValues<int64_t>(1, 0);
    auto p_dist = ids_array->data()->GetValues<float>(1, 0);

    memcpy(ids, p_ids, sizeof(int64_t) * nq * k);
    memcpy(dist, p_dist, sizeof(float) * nq * k);
}

zilliz::knowhere::BinarySet VecIndexImpl::Serialize() {
    return index_->Serialize();
}

void VecIndexImpl::Load(const zilliz::knowhere::BinarySet &index_binary) {
    index_->Load(index_binary);
}

}
}
}
