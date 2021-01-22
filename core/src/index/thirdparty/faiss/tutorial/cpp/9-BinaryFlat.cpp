/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <faiss/IndexBinaryFlat.h>
#include <sys/time.h>
#include <unistd.h>

// #define TEST_HAMMING

long int getTime(timeval end, timeval start) {
	return 1000*(end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec)/1000;
}

int main() {
    // freopen("0.txt", "w", stdout);

    size_t d = 128;                          // dimension
    size_t nb = 40000000;                    // database size
    size_t nq = 10;                          // nb of queries

    uint8_t *xb = new uint8_t[d * nb / sizeof(uint8_t)];
    uint8_t *xq = new uint8_t[d * nq / sizeof(uint8_t)];

    // skip 0
    lrand48();

    size_t size_to_long = d * nb / sizeof(int32_t);
    for(size_t i = 0; i < size_to_long; i++) {
        ((int32_t*)xb)[i] = lrand48();
    }

    size_to_long = d * nq / sizeof(long int);
    for(size_t i = 0; i < size_to_long; i++) {
        ((int32_t*)xq)[i] = lrand48();
    }
#ifdef TEST_HAMMING
    printf("test haming\n");
    faiss::IndexBinaryFlat index(d, faiss::MetricType::METRIC_Hamming);
#else
    faiss::IndexBinaryFlat index(d, faiss::MetricType::METRIC_Jaccard);
#endif

    index.add(nb, xb);
    printf("ntotal = %ld d = %d\n", index.ntotal, index.d);

    int k = 10;

#if 0
    {       // sanity check: search 5 first vectors of xb
        int64_t *I = new int64_t[k * 5];
        int32_t *D = new int32_t[k * 5];
        float *d_float = reinterpret_cast<float*>(D);

        index.search(5, xb, k, D, I);

        // print results
        for(int i = 0; i < 5; i++) {
            for(int j = 0; j < k; j++)
#ifdef TEST_HAMMING
                printf("%8ld %d\n", I[i * k + j], D[i * k + j]);
#else
                printf("%8ld %.08f\n", I[i * k + j], d_float[i * k + j]);
#endif
            printf("\n");
        }

        delete [] I;
        delete [] D;
    }
#endif

    {       // search xq
        int64_t *I = new int64_t[k * nq];
        int32_t *D = new int32_t[k * nq];
        float *d_float = reinterpret_cast<float*>(D);

        for (int loop = 1; loop <= nq; loop ++) {
            timeval t0;
            gettimeofday(&t0, 0);

            index.search(loop, xq, k, D, I);

            timeval t1;
            gettimeofday(&t1, 0);
            printf("search nq %d time %ldms\n", loop, getTime(t1,t0));
#if 0
            for (int i = 0; i < loop; i++) {
                for(int j = 0; j < k; j++)
#ifdef TEST_HAMMING
                    printf("%8ld %d\n", I[i * k + j], D[i * k + j]);
#else
                    printf("%8ld %.08f\n", I[j + i * k], d_float[j + i * k]);
#endif
                printf("\n");

#endif
        }

        delete [] I;
        delete [] D;
    }

    delete [] xb;
    delete [] xq;

    return 0;
}


