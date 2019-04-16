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
    int d = 64;

    // size of the database we plan to index
    size_t nb = 100000;

    // make a set of nt training vectors in the unit cube
    size_t nt = 150000;

    // a reasonable number of cetroids to index nb vectors
    int ncentroids = 25;

    srand48(35); // seed

    std::vector<float> xb(nb * d);
    for (size_t i = 0; i < nb * d; i++) {
        xb[i] = drand48();
    }

    std::vector<long> ids(nb);
    for (size_t i = 0; i < nb; i++) {
        ids[i] = drand48();
    }

    std::vector<float> xt(nt * d);
    for (size_t i = 0; i < nt * d; i++) {
        xt[i] = drand48();
    }

    auto opd = std::make_shared<Operand>();
    IndexBuilderPtr index_builder_1 = GetIndexBuilder(opd);
    auto index_1 = index_builder_1->build_all(nb, xb, ids, nt, xt);
}

