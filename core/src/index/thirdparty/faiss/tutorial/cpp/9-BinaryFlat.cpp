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

    size_t d = 2048;                          // dimension
    size_t nb = 1000000;                    // database size
    size_t nq = 1000;                          // nb of queries

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
//#ifdef TEST_HAMMING
    // printf("test haming\n");
    faiss::IndexBinaryFlat index(d, faiss::MetricType::METRIC_Hamming);
//#else
    // faiss::IndexBinaryFlat index(d, faiss::MetricType::METRIC_Jaccard);
//#endif
    index.add(nb, xb);
    printf("ntotal = %ld d = %d\n", index.ntotal, index.d);

    int max_topk = 1000;

#if 0
    {       // sanity check: search 5 first vectors of xb
        int64_t *I = new int64_t[k * 5];
        float *D = new int32_t[k * 5];
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
        //for (int t=)
        for(int topk=1;topk<=max_topk;topk*=10)
        {
            int64_t *I = new int64_t[topk * nq];
            int32_t *D = new int32_t[topk * nq];
            //float *d_float = reinterpret_cast<float*>(D);
            timeval t0;
            gettimeofday(&t0, 0);
            //for (int loop = 1; loop <= nq; loop ++) {

            index.search(nq, xq, topk, D, I);

            //}
            timeval t1;
            gettimeofday(&t1, 0);
            printf("topk %d time %ldms\n",topk, getTime(t1,t0));
             for(int i = 0; i < 5;i++)
             {
                 if(i>5) break;
                 if(topk == 10)
                 {
                     for(int j=0; j<5;j++){
                         printf("i: %d j: %d :%8ld %d\n", i,j,I[i * topk + j],D[i * topk + j]);
                     }
                 }

             }
            delete [] I;
            delete [] D;
        }

    }

    delete [] xb;
    delete [] xq;

    return 0;
}
