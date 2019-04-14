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


#include "../IndexIVFPQ.h"
#include "../IndexFlat.h"
#include "../index_io.h"

double elapsed ()
{
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return  tv.tv_sec + tv.tv_usec * 1e-6;
}


int main ()
{

    double t0 = elapsed();

    // dimension of the vectors to index
    int d = 128;

    // size of the database we plan to index
    size_t nb = 200 * 1000;

    // make a set of nt training vectors in the unit cube
    // (could be the database)
    size_t nt = 100 * 1000;

    // make the index object and train it
    faiss::IndexFlatL2 coarse_quantizer (d);

    // a reasonable number of centroids to index nb vectors
    int ncentroids = int (4 * sqrt (nb));

    // the coarse quantizer should not be dealloced before the index
    // 4 = nb of bytes per code (d must be a multiple of this)
    // 8 = nb of bits per sub-code (almost always 8)
    faiss::IndexIVFPQ index (&coarse_quantizer, d,
                             ncentroids, 4, 8);


    { // training
        printf ("[%.3f s] Generating %ld vectors in %dD for training\n",
                elapsed() - t0, nt, d);

        std::vector <float> trainvecs (nt * d);
        for (size_t i = 0; i < nt * d; i++) {
            trainvecs[i] = drand48();
        }

        printf ("[%.3f s] Training the index\n",
                elapsed() - t0);
        index.verbose = true;

        index.train (nt, trainvecs.data());
    }

    { // I/O demo
        const char *outfilename = "/tmp/index_trained.faissindex";
        printf ("[%.3f s] storing the pre-trained index to %s\n",
                elapsed() - t0, outfilename);

        write_index (&index, outfilename);
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

        printf ("[%.3f s] Adding the vectors to the index\n",
                elapsed() - t0);

        index.add (nb, database.data());

        printf ("[%.3f s] imbalance factor: %g\n",
                elapsed() - t0, index.invlists->imbalance_factor());

        // remember a few elements from the database as queries
        int i0 = 1234;
        int i1 = 1243;

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

        printf ("note that the nearest neighbor is not at "
                "distance 0 due to quantization errors\n");
    }

    return 0;
}
