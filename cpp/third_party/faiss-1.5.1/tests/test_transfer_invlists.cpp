/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <cstdio>
#include <cstdlib>

#include <gtest/gtest.h>

#include <faiss/IndexIVFFlat.h>
#include <faiss/index_io.h>
#include <faiss/AuxIndexStructures.h>
#include <faiss/AutoTune.h>
#include <faiss/VectorTransform.h>
#include <faiss/utils.h>
#include <faiss/IVFlib.h>


namespace {

// parameters to use for the test
int d = 64;
size_t nb = 1000;
size_t nq = 100;
size_t nt = 500;
int k = 10;
int nlist = 40;

using namespace faiss;

typedef faiss::Index::idx_t idx_t;


std::vector<float> get_data (size_t nb, int seed) {
    std::vector<float> x (nb * d);
    float_randn (x.data(), nb * d, seed);
    return x;
}


void test_index_type(const char *factory_string) {

    // transfer inverted lists in nslice slices
    int nslice = 3;

    /****************************************************************
     * trained reference index
     ****************************************************************/

    std::unique_ptr<Index> trained (index_factory (d, factory_string));

    {
        auto xt = get_data (nt, 123);
        trained->train (nt, xt.data());
    }

    // sample nq query vectors to check if results are the same
    auto xq = get_data (nq, 818);


    /****************************************************************
     * source index
     ***************************************************************/
    std::unique_ptr<Index> src_index (clone_index (trained.get()));

    { // add some data to source index
        auto xb = get_data (nb, 245);
        src_index->add (nb, xb.data());
    }

    ParameterSpace().set_index_parameter (src_index.get(), "nprobe", 4);

    // remember reference search result on source index
    std::vector<idx_t> Iref (nq * k);
    std::vector<float> Dref (nq * k);
    src_index->search (nq, xq.data(), k, Dref.data(), Iref.data());

    /****************************************************************
     * destination index -- should be replaced by source index
     ***************************************************************/

    std::unique_ptr<Index> dst_index (clone_index (trained.get()));

    { // initial state: filled in with some garbage
        int nb2 = nb + 10;
        auto xb = get_data (nb2, 366);
        dst_index->add (nb2, xb.data());
    }

    std::vector<idx_t> Inew (nq * k);
    std::vector<float> Dnew (nq * k);

    ParameterSpace().set_index_parameter (dst_index.get(), "nprobe", 4);

    // transfer from source to destination in nslice slices
    for (int sl = 0; sl < nslice; sl++) {

        // so far, the indexes are different
        dst_index->search (nq, xq.data(), k, Dnew.data(), Inew.data());
        EXPECT_TRUE (Iref != Inew);
        EXPECT_TRUE (Dref != Dnew);

        // range of inverted list indices to transfer
        long i0 = sl * nlist / nslice;
        long i1 = (sl + 1) * nlist / nslice;

        std::vector<uint8_t> data_to_transfer;
        {
            std::unique_ptr<ArrayInvertedLists> il
                (ivflib::get_invlist_range (src_index.get(), i0, i1));
            // serialize inverted lists
            VectorIOWriter wr;
            write_InvertedLists (il.get(), &wr);
            data_to_transfer.swap (wr.data);
        }

        // transfer data here from source machine to dest machine

        {
            VectorIOReader reader;
            reader.data.swap (data_to_transfer);

            // deserialize inverted lists
            std::unique_ptr<ArrayInvertedLists> il
                (dynamic_cast<ArrayInvertedLists *>
                 (read_InvertedLists (&reader)));

            // swap inverted lists. Block searches here!
            {
                ivflib::set_invlist_range (dst_index.get(), i0, i1, il.get());
            }
        }

    }
    EXPECT_EQ (dst_index->ntotal, src_index->ntotal);

    // now, the indexes are the same
    dst_index->search (nq, xq.data(), k, Dnew.data(), Inew.data());
    EXPECT_TRUE (Iref == Inew);
    EXPECT_TRUE (Dref == Dnew);

}

}  // namespace


TEST(TRANS, IVFFlat) {
    test_index_type ("IVF40,Flat");
}

TEST(TRANS, IVFFlatPreproc) {
    test_index_type ("PCAR32,IVF40,Flat");
}
