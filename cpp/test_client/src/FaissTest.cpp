//
// Created by yhmo on 19-4-17.
//

#include "FaissTest.h"

#include "utils/TimeRecorder.h"

#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/index_io.h>
#include <faiss/AutoTune.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/StandardGpuResources.h>

#include <assert.h>

namespace {
    void test_flat() {
        zilliz::vecwise::server::TimeRecorder recorder("test_flat");

        int d = 64;                            // dimension
        int nb = 100000;                       // database size
        int nq = 10000;                        // nb of queries

        float *xb = new float[d * nb];
        float *xq = new float[d * nq];

        for(int i = 0; i < nb; i++) {
            for(int j = 0; j < d; j++)
                xb[d * i + j] = drand48();
            xb[d * i] += i / 1000.;
        }

        for(int i = 0; i < nq; i++) {
            for(int j = 0; j < d; j++)
                xq[d * i + j] = drand48();
            xq[d * i] += i / 1000.;
        }

        recorder.Record("prepare data");

        faiss::IndexFlatL2 index(d);           // call constructor

        recorder.Record("declare index");

        printf("is_trained = %s\n", index.is_trained ? "true" : "false");
        index.add(nb, xb);                     // add vectors to the index
        printf("ntotal = %ld\n", index.ntotal);

        recorder.Record("add index");

        int k = 4;

        {       // sanity check: search 5 first vectors of xb
            long *I = new long[k * 5];
            float *D = new float[k * 5];

            index.search(5, xb, k, D, I);

            // print results
            printf("I=\n");
            for(int i = 0; i < 5; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            printf("D=\n");
            for(int i = 0; i < 5; i++) {
                for(int j = 0; j < k; j++)
                    printf("%7g ", D[i * k + j]);
                printf("\n");
            }

            delete [] I;
            delete [] D;
        }

        recorder.Record("search top 4");

        {       // search xq
            long *I = new long[k * nq];
            float *D = new float[k * nq];

            index.search(nq, xq, k, D, I);

            // print results
            printf("I (5 first results)=\n");
            for(int i = 0; i < 5; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            printf("I (5 last results)=\n");
            for(int i = nq - 5; i < nq; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            delete [] I;
            delete [] D;
        }

        recorder.Record("search xq");

        delete [] xb;
        delete [] xq;

        recorder.Record("delete data");
    }

    void test_gpu() {
        zilliz::vecwise::server::TimeRecorder recorder("test_gpu");

        int d = 64;                            // dimension
        int nb = 100000;                       // database size
        int nq = 10000;                        // nb of queries

        float *xb = new float[d * nb];
        float *xq = new float[d * nq];

        for(int i = 0; i < nb; i++) {
            for(int j = 0; j < d; j++)
                xb[d * i + j] = drand48();
            xb[d * i] += i / 1000.;
        }

        for(int i = 0; i < nq; i++) {
            for(int j = 0; j < d; j++)
                xq[d * i + j] = drand48();
            xq[d * i] += i / 1000.;
        }

        recorder.Record("prepare data");

        faiss::gpu::StandardGpuResources res;

        // Using a flat index

        faiss::gpu::GpuIndexFlatL2 index_flat(&res, d);

        recorder.Record("declare index");

        printf("is_trained = %s\n", index_flat.is_trained ? "true" : "false");
        index_flat.add(nb, xb);  // add vectors to the index
        printf("ntotal = %ld\n", index_flat.ntotal);

        recorder.Record("add index");

        int k = 4;

        {       // search xq
            long *I = new long[k * nq];
            float *D = new float[k * nq];

            index_flat.search(nq, xq, k, D, I);

            // print results
            printf("I (5 first results)=\n");
            for(int i = 0; i < 5; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            printf("I (5 last results)=\n");
            for(int i = nq - 5; i < nq; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            delete [] I;
            delete [] D;
        }

        recorder.Record("search top 4");

        // Using an IVF index

        int nlist = 100;
        faiss::gpu::GpuIndexIVFFlat index_ivf(&res, d, nlist, faiss::METRIC_L2);
        // here we specify METRIC_L2, by default it performs inner-product search

        recorder.Record("declare index");

        assert(!index_ivf.is_trained);
        index_ivf.train(nb, xb);
        assert(index_ivf.is_trained);

        recorder.Record("train index");

        index_ivf.add(nb, xb);  // add vectors to the index

        recorder.Record("add index");

        printf("is_trained = %s\n", index_ivf.is_trained ? "true" : "false");
        printf("ntotal = %ld\n", index_ivf.ntotal);

        {       // search xq
            long *I = new long[k * nq];
            float *D = new float[k * nq];

            index_ivf.search(nq, xq, k, D, I);

            // print results
            printf("I (5 first results)=\n");
            for(int i = 0; i < 5; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            printf("I (5 last results)=\n");
            for(int i = nq - 5; i < nq; i++) {
                for(int j = 0; j < k; j++)
                    printf("%5ld ", I[i * k + j]);
                printf("\n");
            }

            delete [] I;
            delete [] D;
        }

        recorder.Record("search xq");

        delete [] xb;
        delete [] xq;

        recorder.Record("delete data");
    }
}

void FaissTest::test() {

    int ngpus = faiss::gpu::getNumDevices();

    printf("Number of GPUs: %d\n", ngpus);

    test_flat();
    test_gpu();
}