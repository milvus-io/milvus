/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <gtest/gtest.h>

#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexFlat.h>
#include <faiss/utils.h>


namespace {

// dimension of the vectors to index
int d = 64;

// size of the database we plan to index
size_t nb = 8000;


double eval_codec_error (long ncentroids, long m, const std::vector<float> &v)
{
    faiss::IndexFlatL2 coarse_quantizer (d);
    faiss::IndexIVFPQ index (&coarse_quantizer, d,
                             ncentroids, m, 8);
    index.pq.cp.niter = 10; // speed up train
    index.train (nb, v.data());

    // encode and decode to compute reconstruction error

    std::vector<long> keys (nb);
    std::vector<uint8_t> codes (nb * m);
    index.encode_multiple (nb, keys.data(), v.data(), codes.data(), true);

    std::vector<float> v2 (nb * d);
    index.decode_multiple (nb, keys.data(), codes.data(), v2.data());

    return faiss::fvec_L2sqr (v.data(), v2.data(), nb * d);
}

}  // namespace


TEST(IVFPQ, codec) {

    std::vector <float> database (nb * d);
    for (size_t i = 0; i < nb * d; i++) {
        database[i] = drand48();
    }

    double err0 = eval_codec_error(16, 8, database);

    // should be more accurate as there are more coarse centroids
    double err1 = eval_codec_error(128, 8, database);
    EXPECT_GT(err0, err1);

    // should be more accurate as there are more PQ codes
    double err2 = eval_codec_error(16, 16, database);
    EXPECT_GT(err0, err2);
}
