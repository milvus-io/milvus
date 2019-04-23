////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include "wrapper/Operand.h"
#include "wrapper/Index.h"
#include "wrapper/IndexBuilder.h"

using namespace zilliz::vecwise::engine;


TEST(operand_test, Wrapper_Test) {
    auto opd = std::make_shared<Operand>();
    opd->index_type = "IVF16384,Flat";
    opd->d = 256;

    std::cout << opd << std::endl;
}

TEST(build_test, Wrapper_Test) {
    // dimension of the vectors to index
    int d = 3;

    // make a set of nt training vectors in the unit cube
    size_t nt = 10000;

    // a reasonable number of cetroids to index nb vectors
    int ncentroids = 16;

    std::random_device rd;
    std::mt19937 gen(rd());

    std::vector<float> xb;
    std::vector<long> ids;

    //prepare train data
    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    std::vector<float> xt(nt * d);
    for (size_t i = 0; i < nt * d; i++) {
        xt[i] = dis_xt(gen);
    }

    //train the index
    auto opd = std::make_shared<Operand>();
    opd->index_type = "IVF16,Flat";
    opd->d = d;
    opd->ncent = ncentroids;
    IndexBuilderPtr index_builder_1 = GetIndexBuilder(opd);
    auto index_1 = index_builder_1->build_all(0, xb, ids, nt, xt);
    ASSERT_TRUE(index_1 != nullptr);

    // size of the database we plan to index
    size_t nb = 100000;

    //prepare raw data
    xb.resize(nb);
    ids.resize(nb);
    for (size_t i = 0; i < nb; i++) {
        xb[i] = dis_xt(gen);
        ids[i] = i;
    }
    index_1->add_with_ids(nb, xb.data(), ids.data());

    //search in first quadrant
    int nq = 1, k = 10;
    std::vector<float> xq = {0.5, 0.5, 0.5};
    float* result_dists = new float[k];
    long* result_ids = new long[k];
    index_1->search(nq, xq.data(), k, result_dists, result_ids);

    for(int i = 0; i < k; i++) {
        if(result_ids[i] < 0) {
            ASSERT_TRUE(false);
            break;
        }

        long id = result_ids[i];
        std::cout << "No." << id << " [" << xb[id*3] << ", " << xb[id*3 + 1] << ", "
            << xb[id*3 + 2] <<"] distance = " << result_dists[i] << std::endl;

        //makesure result vector is in first quadrant
        ASSERT_TRUE(xb[id*3] > 0.0);
        ASSERT_TRUE(xb[id*3 + 1] > 0.0);
        ASSERT_TRUE(xb[id*3 + 2] > 0.0);
    }

    delete[] result_dists;
    delete[] result_ids;
}

TEST(search_test, Wrapper_Test) {
    const int dim = 256;

    size_t nb = 25000;
    size_t nq = 100;
    size_t k = 100;
    std::vector<float> xb(nb*dim);
    std::vector<float> xq(nq*dim);
    std::vector<long> ids(nb*dim);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis_xt(-1.0, 1.0);
    for (size_t i = 0; i < nb*dim; i++) {
        xb[i] = dis_xt(gen);
        ids[i] = i;
    }
    for (size_t i = 0; i < nq*dim; i++) {
        xq[i] = dis_xt(gen);
    }

    // result data
    std::vector<long> nns_gt(nq*k); // nns = nearst neg search
    std::vector<long> nns(nq*k);
    std::vector<float> dis_gt(nq*k);
    std::vector<float> dis(nq*k);
    faiss::Index* index_gt(faiss::index_factory(dim, "IDMap,Flat"));
    index_gt->add_with_ids(nb, xb.data(), ids.data());
    index_gt->search(nq, xq.data(), 10, dis_gt.data(), nns_gt.data());
    std::cout << "data: " << nns_gt[0];

}
