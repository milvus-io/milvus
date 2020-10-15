/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include <faiss/IndexIVF.h>
#include <faiss/index_factory.h>
#include <faiss/VectorTransform.h>
#include <faiss/IVFlib.h>


namespace {

typedef faiss::Index::idx_t idx_t;

/*************************************************************
 * Test utils
 *************************************************************/


// dimension of the vectors to index
int d = 64;

// size of the database we plan to index
size_t nb = 8000;

// nb of queries
size_t nq = 200;

std::vector<float> make_data(size_t n)
{
    std::vector <float> database (n * d);
    for (size_t i = 0; i < n * d; i++) {
        database[i] = drand48();
    }
    return database;
}

std::unique_ptr<faiss::Index> make_index(const char *index_type,
                                         const std::vector<float> & x) {

    auto index = std::unique_ptr<faiss::Index> (
            faiss::index_factory(d, index_type));
    index->train(nb, x.data());
    index->add(nb, x.data());
    return index;
}

/*************************************************************
 * Test functions for a given index type
 *************************************************************/

bool test_search_centroid(const char *index_key) {
    std::vector<float> xb = make_data(nb); // database vectors
    auto index = make_index(index_key, xb);

    /* First test: find the centroids associated to the database
       vectors and make sure that each vector does indeed appear in
       the inverted list corresponding to its centroid */

    std::vector<idx_t> centroid_ids (nb);
    faiss::ivflib::search_centroid(
         index.get(), xb.data(), nb, centroid_ids.data());

    const faiss::IndexIVF * ivf = faiss::ivflib::extract_index_ivf
        (index.get());

    for(int i = 0; i < nb; i++) {
        bool found = false;
        int list_no = centroid_ids[i];
        int list_size = ivf->invlists->list_size (list_no);
        auto * list = ivf->invlists->get_ids (list_no);

        for(int j = 0; j < list_size; j++) {
            if (list[j] == i) {
                found = true;
                break;
            }
        }
        if(!found) return false;
    }
    return true;
}

int test_search_and_return_centroids(const char *index_key) {
    std::vector<float> xb = make_data(nb); // database vectors
    auto index = make_index(index_key, xb);

    std::vector<idx_t> centroid_ids (nb);
    faiss::ivflib::search_centroid(index.get(), xb.data(),
                                   nb, centroid_ids.data());

    faiss::IndexIVF * ivf =
        faiss::ivflib::extract_index_ivf (index.get());
    ivf->nprobe = 4;

    std::vector<float> xq = make_data(nq); // database vectors

    int k = 5;

    // compute a reference search result

    std::vector<idx_t> refI (nq * k);
    std::vector<float> refD (nq * k);
    index->search (nq, xq.data(), k, refD.data(), refI.data());

    // compute search result

    std::vector<idx_t> newI (nq * k);
    std::vector<float> newD (nq * k);

    std::vector<idx_t> query_centroid_ids (nq);
    std::vector<idx_t> result_centroid_ids (nq * k);

    faiss::ivflib::search_and_return_centroids(index.get(),
                                nq, xq.data(), k,
                                newD.data(), newI.data(),
                                query_centroid_ids.data(),
                                result_centroid_ids.data());

    // first verify that we have the same result as the standard search

    if (newI != refI) {
        return 1;
    }

    // then check if the result ids are indeed in the inverted list
    // they are supposed to be in

    for(int i = 0; i < nq * k; i++) {
        int list_no = result_centroid_ids[i];
        int result_no = newI[i];

        if (result_no < 0) continue;

        bool found = false;

        int list_size = ivf->invlists->list_size (list_no);
        auto * list = ivf->invlists->get_ids (list_no);

        for(int j = 0; j < list_size; j++) {
            if (list[j] == result_no) {
                found = true;
                break;
            }
        }
        if(!found) return 2;
    }
    return 0;
}

}  // namespace


/*************************************************************
 * Test entry points
 *************************************************************/

TEST(test_search_centroid, IVFFlat) {
    bool ok = test_search_centroid("IVF32,Flat");
    EXPECT_TRUE(ok);
}

TEST(test_search_centroid, PCAIVFFlat) {
    bool ok = test_search_centroid("PCA16,IVF32,Flat");
    EXPECT_TRUE(ok);
}

TEST(test_search_and_return_centroids, IVFFlat) {
    int err = test_search_and_return_centroids("IVF32,Flat");
    EXPECT_NE(err, 1);
    EXPECT_NE(err, 2);
}

TEST(test_search_and_return_centroids, PCAIVFFlat) {
    int err = test_search_and_return_centroids("PCA16,IVF32,Flat");
    EXPECT_NE(err, 1);
    EXPECT_NE(err, 2);
}
