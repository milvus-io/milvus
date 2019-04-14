/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */



#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <sys/time.h>


#include <faiss/IndexPQ.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

double elapsed ()
{
    struct timeval tv;
    gettimeofday (&tv, nullptr);
    return  tv.tv_sec + tv.tv_usec * 1e-6;
}


int main ()
{
    double t0 = elapsed();

    // dimension of the vectors to index
    int d = 128;

    // size of the database we plan to index
    size_t nb = 1000 * 1000;

    // make a set of nt training vectors in the unit cube
    // (could be the database)
    size_t nt = 100 * 1000;

    //---------------------------------------------------------------
    // Define the core quantizer
    // We choose a multiple inverted index for faster training with less data
    // and because it usually offers best accuracy/speed trade-offs
    //
    // We here assume that its lifespan of this coarse quantizer will cover the
    // lifespan of the inverted-file quantizer IndexIVFFlat below
    // With dynamic allocation, one may give the responsability to free the
    // quantizer to the inverted-file index (with attribute do_delete_quantizer)
    //
    // Note: a regular clustering algorithm would be defined as:
    //       faiss::IndexFlatL2 coarse_quantizer (d);
    //
    // Use nhash=2 subquantizers used to define the product coarse quantizer
    // Number of bits: we will have 2^nbits_coarse centroids per subquantizer
    //                 meaning (2^12)^nhash distinct inverted lists
    size_t nhash = 2;
    size_t nbits_subq = int (log2 (nb+1) / 2);        // good choice in general
    size_t ncentroids = 1 << (nhash * nbits_subq);  // total # of centroids

    faiss::MultiIndexQuantizer coarse_quantizer (d, nhash, nbits_subq);

    printf ("IMI (%ld,%ld): %ld virtual centroids (target: %ld base vectors)",
            nhash, nbits_subq, ncentroids, nb);

    // the coarse quantizer should not be dealloced before the index
    // 4 = nb of bytes per code (d must be a multiple of this)
    // 8 = nb of bits per sub-code (almost always 8)
    faiss::MetricType metric = faiss::METRIC_L2; // can be METRIC_INNER_PRODUCT
    faiss::IndexIVFFlat index (&coarse_quantizer, d, ncentroids, metric);
    index.quantizer_trains_alone = true;

    // define the number of probes. 2048 is for high-dim, overkilled in practice
    // Use 4-1024 depending on the trade-off speed accuracy that you want
    index.nprobe = 2048;


    { // training
        printf ("[%.3f s] Generating %ld vectors in %dD for training\n",
                elapsed() - t0, nt, d);

        std::vector <float> trainvecs (nt * d);
        for (size_t i = 0; i < nt * d; i++) {
            trainvecs[i] = drand48();
        }

        printf ("[%.3f s] Training the index\n", elapsed() - t0);
        index.verbose = true;
        index.train (nt, trainvecs.data());
    }

    size_t nq;
    std::vector<float> queries;

    { // populating the database
        printf ("[%.3f s] Building a dataset of %ld vectors to index\n",
                elapsed() - t0, nb);

        std::vector <float> database (nb * d);
        for (size_t i = 0; i < nb * d; i++) {
            database[i] = drand48();
        }

        printf ("[%.3f s] Adding the vectors to the index\n", elapsed() - t0);

        index.add (nb, database.data());

        // remember a few elements from the database as queries
        int i0 = 1234;
        int i1 = 1244;

        nq = i1 - i0;
        queries.resize (nq * d);
        for (int i = i0; i < i1; i++) {
            for (int j = 0; j < d; j++) {
                queries [(i - i0) * d  + j]  = database [i * d + j];
            }
        }
    }

    { // searching the database
        int k = 5;
        printf ("[%.3f s] Searching the %d nearest neighbors "
                "of %ld vectors in the index\n",
                elapsed() - t0, k, nq);

        std::vector<faiss::Index::idx_t> nns (k * nq);
        std::vector<float>               dis (k * nq);

        index.search (nq, queries.data(), k, dis.data(), nns.data());

        printf ("[%.3f s] Query results (vector ids, then distances):\n",
                elapsed() - t0);

        for (int i = 0; i < nq; i++) {
            printf ("query %2d: ", i);
            for (int j = 0; j < k; j++) {
                printf ("%7ld ", nns[j + i * k]);
            }
            printf ("\n     dis: ");
            for (int j = 0; j < k; j++) {
                printf ("%7g ", dis[j + i * k]);
            }
            printf ("\n");
        }
    }
    return 0;
}
