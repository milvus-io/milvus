/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "error_c.h"
#include "Index_c.h"
#include "AutoTune_c.h"
#include "GpuAutoTune_c.h"
#include "StandardGpuResources_c.h"

#define FAISS_TRY(C)                                       \
    {                                                      \
        if (C) {                                           \
            fprintf(stderr, "%s", faiss_get_last_error()); \
            exit(-1);                                      \
        }                                                  \
    }

double drand() {
    return (double)rand() / (double)RAND_MAX;
}

int main() {
    time_t seed = time(NULL);
    srand(seed);
    printf("Generating some data...\n");
    int d = 128;                           // dimension
    int nb = 100000;                       // database size
    int nq = 10000;                        // nb of queries
    float *xb = malloc(d * nb * sizeof(float));
    float *xq = malloc(d * nq * sizeof(float));

    for(int i = 0; i < nb; i++) {
        for(int j = 0; j < d; j++) xb[d * i + j] = drand();
        xb[d * i] += i / 1000.;
    }
    for(int i = 0; i < nq; i++) {
        for(int j = 0; j < d; j++) xq[d * i + j] = drand();
        xq[d * i] += i / 1000.;
    }

    printf("Loading standard GPU resources...\n");
    FaissStandardGpuResources* gpu_res = NULL;
    FAISS_TRY(faiss_StandardGpuResources_new(&gpu_res));

    printf("Building an index...\n");
    FaissIndex* cpu_index = NULL;
    FAISS_TRY(faiss_index_factory(&cpu_index, d, "Flat", METRIC_L2));  // use factory to create index

    printf("Moving index to the GPU...\n");
    FaissGpuIndex* index = NULL;
    FaissGpuClonerOptions* options = NULL;
    FAISS_TRY(faiss_GpuClonerOptions_new(&options));
    FAISS_TRY(faiss_index_cpu_to_gpu_with_options(gpu_res, 0, cpu_index, options, &index));

    printf("is_trained = %s\n", faiss_Index_is_trained(index) ? "true" : "false");
    FAISS_TRY(faiss_Index_add(index, nb, xb));                     // add vectors to the index
    printf("ntotal = %ld\n", faiss_Index_ntotal(index));

    printf("Searching...\n");
    int k = 5;

    {       // sanity check: search 5 first vectors of xb
        idx_t *I = malloc(k * 5 * sizeof(idx_t));
        float *D = malloc(k * 5 * sizeof(float));
        FAISS_TRY(faiss_Index_search(index, 5, xb, k, D, I));
        printf("I=\n");
        for(int i = 0; i < 5; i++) {
            for(int j = 0; j < k; j++) printf("%5ld (d=%2.3f)  ", I[i * k + j], D[i * k + j]);
            printf("\n");
        }
        free(I);
        free(D);
    }
    {       // search xq
        idx_t *I = malloc(k * nq * sizeof(idx_t));
        float *D = malloc(k * nq * sizeof(float));
        FAISS_TRY(faiss_Index_search(index, 5, xb, k, D, I));
        printf("I=\n");
        for(int i = 0; i < 5; i++) {
            for(int j = 0; j < k; j++) printf("%5ld (d=%2.3f)  ", I[i * k + j], D[i * k + j]);
            printf("\n");
        }
        free(I);
        free(D);
    }

    printf("Freeing index...\n");
    faiss_Index_free(index);
    printf("Freeing GPU resources...\n");
    faiss_GpuResources_free(gpu_res);
    faiss_GpuClonerOptions_free(options);
    printf("Done.\n");

    return 0;
}