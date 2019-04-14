/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include <faiss/IndexIVF.h>
#include <faiss/AutoTune.h>
#include <faiss/index_io.h>
#include <faiss/IVFlib.h>

using namespace faiss;

typedef Index::idx_t idx_t;


// dimension of the vectors to index
int d = 32;

// nb of training vectors
size_t nt = 5000;

// size of the database points per window step
size_t nb = 1000;

// nb of queries
size_t nq = 200;


int total_size = 40;
int window_size = 10;





std::vector<float> make_data(size_t n)
{
    std::vector <float> database (n * d);
    for (size_t i = 0; i < n * d; i++) {
        database[i] = drand48();
    }
    return database;
}

std::unique_ptr<Index> make_trained_index(const char *index_type)
{
    auto index = std::unique_ptr<Index>(index_factory(d, index_type));
    auto xt = make_data(nt * d);
    index->train(nt, xt.data());
    ParameterSpace().set_index_parameter (index.get(), "nprobe", 4);
    return index;
}

std::vector<idx_t> search_index(Index *index, const float *xq) {
    int k = 10;
    std::vector<idx_t> I(k * nq);
    std::vector<float> D(k * nq);
    index->search (nq, xq, k, D.data(), I.data());
    return I;
}





/*************************************************************
 * Test functions for a given index type
 *************************************************************/


// make a few slices of indexes that can be merged
void make_index_slices (const Index* trained_index,
                        std::vector<std::unique_ptr<Index> > & sub_indexes) {

    for (int i = 0; i < total_size; i++) {
        sub_indexes.emplace_back (clone_index (trained_index));

        printf ("preparing sub-index # %d\n", i);

        Index * index = sub_indexes.back().get();

        auto xb = make_data(nb * d);
        std::vector<long> ids (nb);
        for (int j = 0; j < nb; j++) {
            ids[j] = lrand48();
        }
        index->add_with_ids (nb, xb.data(), ids.data());
    }

}

// build merged index explicitly at sliding window position i
Index *make_merged_index(
         const Index* trained_index,
         const std::vector<std::unique_ptr<Index> > & sub_indexes,
         int i) {

    Index * merged_index = clone_index (trained_index);
    for (int j = i - window_size + 1; j <= i; j++) {
        if (j < 0 || j >= total_size) continue;
        std::unique_ptr<Index> sub_index (
                clone_index (sub_indexes[j].get()));
        IndexIVF *ivf0 = ivflib::extract_index_ivf (merged_index);
        IndexIVF *ivf1 = ivflib::extract_index_ivf (sub_index.get());
        ivf0->merge_from (*ivf1, 0);
        merged_index->ntotal = ivf0->ntotal;
    }
    return merged_index;
}

int test_sliding_window (const char *index_key) {

    std::unique_ptr<Index> trained_index = make_trained_index(index_key);

    // make the index slices
    std::vector<std::unique_ptr<Index> > sub_indexes;

    make_index_slices (trained_index.get(), sub_indexes);

    // now slide over the windows
    std::unique_ptr<Index> index (clone_index (trained_index.get()));
    ivflib::SlidingIndexWindow window (index.get());

    auto xq = make_data (nq * d);

    for (int i = 0; i < total_size + window_size; i++) {

        printf ("doing step %d / %d\n", i, total_size + window_size);

        // update the index
        window.step (i < total_size ? sub_indexes[i].get() : nullptr,
                     i >= window_size);
        printf ("   current n_slice = %d\n", window.n_slice);

        auto new_res = search_index (index.get(), xq.data());

        std::unique_ptr<Index> merged_index (
             make_merged_index (trained_index.get(), sub_indexes, i));

        auto ref_res = search_index (merged_index.get(), xq.data ());

        EXPECT_EQ (ref_res.size(), new_res.size());

        EXPECT_EQ (ref_res, new_res);
    }
    return 0;
}


int test_sliding_invlists (const char *index_key) {

    std::unique_ptr<Index> trained_index = make_trained_index(index_key);

    // make the index slices
    std::vector<std::unique_ptr<Index> > sub_indexes;

    make_index_slices (trained_index.get(), sub_indexes);

    // now slide over the windows
    std::unique_ptr<Index> index (clone_index (trained_index.get()));
    IndexIVF * index_ivf = ivflib::extract_index_ivf (index.get());

    auto xq = make_data (nq * d);

    for (int i = 0; i < total_size + window_size; i++) {

        printf ("doing step %d / %d\n", i, total_size + window_size);

        // update the index
        std::vector<const InvertedLists*> ils;
        for (int j = i - window_size + 1; j <= i; j++) {
            if (j < 0 || j >= total_size) continue;
            ils.push_back (ivflib::extract_index_ivf (
                      sub_indexes[j].get())->invlists);
        }
        if (ils.size() == 0) continue;

        ConcatenatedInvertedLists *ci =
            new ConcatenatedInvertedLists (ils.size(), ils.data());

        // will be deleted by the index
        index_ivf->replace_invlists (ci, true);

        printf ("   nb invlists = %ld\n", ils.size());

        auto new_res = search_index (index.get(), xq.data());

        std::unique_ptr<Index> merged_index (
             make_merged_index (trained_index.get(), sub_indexes, i));

        auto ref_res = search_index (merged_index.get(), xq.data ());

        EXPECT_EQ (ref_res.size(), new_res.size());

        size_t ndiff = 0;
        for (size_t j = 0; j < ref_res.size(); j++) {
            if (ref_res[j] != new_res[j])
                ndiff++;
        }
        printf("  nb differences: %ld / %ld\n",
               ndiff, ref_res.size());
        EXPECT_EQ (ref_res, new_res);
    }
    return 0;
}





/*************************************************************
 * Test entry points
 *************************************************************/

TEST(SlidingWindow, IVFFlat) {
    test_sliding_window ("IVF32,Flat");
}

TEST(SlidingWindow, PCAIVFFlat) {
    test_sliding_window ("PCA24,IVF32,Flat");
}

TEST(SlidingInvlists, IVFFlat) {
    test_sliding_invlists ("IVF32,Flat");
}

TEST(SlidingInvlists, PCAIVFFlat) {
    test_sliding_invlists ("PCA24,IVF32,Flat");
}
