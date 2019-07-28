////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <faiss/IndexFlat.h>

#include "utils.h"


DataGenPtr GetGenerateFactory(const std::string &gen_type) {
    std::shared_ptr<DataGenBase> generator;
    if (gen_type == "default") {
        generator = std::make_shared<DataGenBase>();
    }
    return generator;
}

void DataGenBase::GenData(const int &dim, const int &nb, const int &nq,
                          float *xb, float *xq, long *ids,
                          const int &k, long *gt_ids, float *gt_dis) {
    for (auto i = 0; i < nb; ++i) {
        for (auto j = 0; j < dim; ++j) {
            //p_data[i * d + j] = float(base + i);
            xb[i * dim + j] = drand48();
        }
        xb[dim * i] += i / 1000.;
        ids[i] = i;
    }
    for (size_t i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }

    faiss::IndexFlatL2 index(dim);
    //index.add_with_ids(nb, xb, ids);
    index.add(nb, xb);
    index.search(nq, xq, k, gt_dis, gt_ids);
}

void DataGenBase::GenData(const int &dim,
                          const int &nb,
                          const int &nq,
                          std::vector<float> &xb,
                          std::vector<float> &xq,
                          std::vector<long> &ids,
                          const int &k,
                          std::vector<long> &gt_ids,
                          std::vector<float> &gt_dis) {
    xb.resize(nb * dim);
    xq.resize(nq * dim);
    ids.resize(nb);
    gt_ids.resize(nq * k);
    gt_dis.resize(nq * k);
    GenData(dim, nb, nq, xb.data(), xq.data(), ids.data(), k, gt_ids.data(), gt_dis.data());
}
