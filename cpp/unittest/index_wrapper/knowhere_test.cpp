////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <wrapper/knowhere/vec_index.h>

using namespace zilliz::vecwise::engine;

TEST(knowhere_test, ivf_test) {
    auto d = 128;
    auto nt = 1000;
    auto nb = 10000;
    auto nq = 10;
    //{
    //    std::vector<float> xb;
    //    std::vector<float> xt;
    //    std::vector<float> xq;
    //    std::vector<long> ids;
    //
    //    //prepare train data
    //    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    //    std::random_device rd;
    //    std::mt19937 gen(rd());
    //    xt.resize(nt*d);
    //    for (size_t i = 0; i < nt * d; i++) {
    //        xt[i] = dis_xt(gen);
    //    }
    //    xb.resize(nb*d);
    //    ids.resize(nb);
    //    for (size_t i = 0; i < nb * d; i++) {
    //        xb[i] = dis_xt(gen);
    //        if (i < nb) {
    //            ids[i] = i;
    //        }
    //    }
    //    xq.resize(nq*d);
    //    for (size_t i = 0; i < nq * d; i++) {
    //        xq[i] = dis_xt(gen);
    //    }
    //}

    auto elems = nb * d;
    auto p_data = (float *) malloc(elems * sizeof(float));
    auto p_id = (int64_t *) malloc(elems * sizeof(int64_t));
    assert(p_data != nullptr && p_id != nullptr);

    for (auto i = 0; i < nb; ++i) {
        for (auto j = 0; j < d; ++j) {
            p_data[i * d + j] = drand48();
        }
        p_data[d * i] += i / 1000.;
        p_id[i] = i;
    }

    auto q_elems = nq * d;
    auto q_data = (float *) malloc(q_elems * sizeof(float));

    for (auto i = 0; i < nq; ++i) {
        for (auto j = 0; j < d; ++j) {
            q_data[i * d + j] = drand48();
        }
        q_data[d * i] += i / 1000.;
    }

    Config build_cfg = Config::object{
        {"dim", d},
        {"nlist", 100},
    };

    auto k = 10;
    Config search_cfg = Config::object{
        {"dim", d},
        {"k", k},
    };

    std::vector<float> ret_dist(nq*k);
    std::vector<long> ret_ids(nq*k);

    const std::string& index_type = "IVF";
    auto index = GetVecIndexFactory(index_type);
    index->BuildAll(nb, p_data, p_id, build_cfg);

    auto add_bin = index->Serialize();
    index->Load(add_bin);

    index->Search(nq, q_data, ret_dist.data(), ret_ids.data(), search_cfg);

    std::cout << "he";
}
