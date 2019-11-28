/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <unistd.h>

#include <iostream>

#include "faiss/IndexIVF.h"
#include "faiss/IndexFlat.h"
#include "faiss/index_io.h"
#include "faiss/gpu/GpuIndexFlat.h"
#include "faiss/gpu/StandardGpuResources.h"
#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/gpu/GpuCloner.h"
#include "faiss/gpu/GpuClonerOptions.h"
#include "faiss/gpu/GpuIndexIVF.h"
#include "faiss/gpu/GpuIndexIVFSQHybrid.h"

#include "faiss/impl/FaissAssert.h"
#include "faiss/impl/AuxIndexStructures.h"

#include "faiss/IndexFlat.h"
#include "faiss/VectorTransform.h"
#include "faiss/IndexLSH.h"
#include "faiss/IndexPQ.h"

#include "faiss/IndexIVFPQ.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/IndexIVFSpectralHash.h"
#include "faiss/MetaIndexes.h"
#include "faiss/IndexSQHybrid.h"
#include "faiss/IndexHNSW.h"
#include "faiss/OnDiskInvertedLists.h"
#include "faiss/IndexBinaryFlat.h"
#include "faiss/IndexBinaryFromFloat.h"
#include "faiss/IndexBinaryHNSW.h"
#include "faiss/IndexBinaryIVF.h"
#include "faiss/utils/distances.h"
#include "faiss/index_factory.h"

using namespace faiss;

#define PRINT_RESULT 0

void print_result(const char* unit, long number, long k, long nq, long *I) {
    printf("%s: I (2 first results)=\n", unit);
    for(int i = 0; i < number; i++) {
        for(int j = 0; j < k; j++)
            printf("%5ld ", I[i * k + j]);
        printf("\n");
    }

    printf("%s: I (2 last results)=\n", unit);
    for(int i = nq - number; i < nq; i++) {
        for(int j = 0; j < k; j++)
            printf("%5ld ", I[i * k + j]);
        printf("\n");
    }
}


int main() {
    const char* filename = "index500k-h.index";

#if PRINT_RESULT
    int number = 8;
#endif

    int d = 512;                            // dimension
    int nq = 10;                        // nb of queries
    int nprobe = 1;
    float *xq = new float[d * nq];
    for(int i = 0; i < nq; i++) {
        for(int j = 0; j < d; j++) {
            xq[d * i + j] = drand48();
        }
    }
    faiss::distance_compute_blas_threshold = 800;

    faiss::gpu::StandardGpuResources res;

    int k = 8;
    std::shared_ptr<faiss::Index> gpu_index_ivf_ptr;

    const char* index_description = "IVF16384,SQ8Hybrid";
//     const char* index_description = "IVF3276,SQ8";

    faiss::Index *cpu_index = nullptr;
    faiss::IndexIVF* cpu_ivf_index = nullptr;
    if((access(filename,F_OK))==-1) {
        // create database
        long nb = 500000;                       // database size
//        printf("-----------------------\n");
        long size = d * nb;
        float *xb = new float[size];
        memset(xb, 0, size * sizeof(float));
        printf("size: %ld\n", (size * sizeof(float)) );
        for(long i = 0; i < nb; i++) {
            for(long j = 0; j < d; j++) {
                float rand = drand48();
                xb[d * i + j] = rand;
            }
        }

        faiss::Index *ori_index = faiss::index_factory(d, index_description, faiss::METRIC_L2);
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 0, ori_index);

        gpu_index_ivf_ptr = std::shared_ptr<faiss::Index>(device_index);

        assert(!device_index->is_trained);
        device_index->train(nb, xb);
        assert(device_index->is_trained);
        device_index->add(nb, xb);  // add vectors to the index

        printf("is_trained = %s\n", device_index->is_trained ? "true" : "false");
        printf("ntotal = %ld\n", device_index->ntotal);

        cpu_index = faiss::gpu::index_gpu_to_cpu ((device_index));
        faiss::write_index(cpu_index, filename);
        printf("index.index is stored successfully.\n");
        delete [] xb;
    } else {
        cpu_index = faiss::read_index(filename);
    }

    cpu_ivf_index = dynamic_cast<faiss::IndexIVF*>(cpu_index);
    if(cpu_ivf_index != nullptr) {
        cpu_ivf_index->to_readonly();
    }

    auto gpu_executor = [&](int device_id, faiss::gpu::GpuClonerOptions* option, faiss::IndexComposition* index_composition) {
        auto tmp_index = faiss::gpu::index_cpu_to_gpu(&res, device_id, index_composition, option);
        delete tmp_index;
        double t0 = getmillisecs ();
        {
            // cpu to gpu
            tmp_index = faiss::gpu::index_cpu_to_gpu(&res, device_id, index_composition, option);
            gpu_index_ivf_ptr = std::shared_ptr<faiss::Index>(tmp_index);
        }
        double t1 = getmillisecs ();
        printf("CPU to GPU loading time: %0.2f\n", t1 - t0);

        {
            long *I = new long[k * nq];
            float *D = new float[k * nq];

            faiss::gpu::GpuIndexIVFSQHybrid* gpu_index_ivf_hybrid =
                    dynamic_cast<faiss::gpu::GpuIndexIVFSQHybrid*>(gpu_index_ivf_ptr.get());
            gpu_index_ivf_hybrid->setNumProbes(nprobe);
            for(long i = 0; i < 1; ++ i) {
                double t2 = getmillisecs();
                gpu_index_ivf_ptr->search(nq, xq, k, D, I);
                double t3 = getmillisecs();
                printf("* GPU: %d, execution time: %0.2f\n", device_id, t3 - t2);
            }

            // print results
#if PRINT_RESULT
            print_result("GPU", number, k, nq, I);
#endif
            delete [] I;
            delete [] D;
        }
        double t4 = getmillisecs();

        printf("GPU:%d total time: %0.2f\n", device_id, t4 - t0);

    };
    printf("----------------------------------\n");
    auto cpu_executor = [&](faiss::IndexComposition* index_composition) {       // search xq
        printf("CPU: \n");
        long *I = new long[k * nq];
        float *D = new float[k * nq];

        double t4 = getmillisecs();
        faiss::IndexIVF* ivf_index =
                dynamic_cast<faiss::IndexIVF*>(cpu_index);
        ivf_index->nprobe = nprobe;

        faiss::gpu::GpuIndexFlat* is_gpu_flat_index = dynamic_cast<faiss::gpu::GpuIndexFlat*>(ivf_index->quantizer);
        if(is_gpu_flat_index == nullptr) {
            delete ivf_index->quantizer;
            ivf_index->quantizer = index_composition->quantizer;
        }

        cpu_index->search(nq, xq, k, D, I);
        double t5 = getmillisecs();
        printf("CPU execution time: %0.2f\n", t5 - t4);
#if PRINT_RESULT
        print_result("CPU", number, k, nq, I);
#endif
        delete [] I;
        delete [] D;
    };


    faiss::gpu::GpuClonerOptions option0;
    faiss::gpu::GpuClonerOptions option1;

    faiss::IndexComposition index_composition0;
    index_composition0.index = cpu_index;
    index_composition0.quantizer = nullptr;
    index_composition0.mode = 0; // only quantizer

    // Copy quantizer to GPU 0
    auto index1 = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition0, &option0);
    delete index1;

    faiss::IndexComposition index_composition1;
    index_composition1.index = cpu_index;
    index_composition1.quantizer = nullptr;
    index_composition1.mode = 0; // only quantizer

    // Copy quantizer to GPU 1
    index1 = faiss::gpu::index_cpu_to_gpu(&res, 1, &index_composition1, &option1);
    delete index1;

    std::thread t_cpu1(cpu_executor, &index_composition0);
    t_cpu1.join();
    std::thread t_cpu2(cpu_executor, &index_composition1);
    t_cpu2.join();

    index_composition0.mode = 2; // only data
    index_composition1.mode = 2; // only data

    index1 = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition0, &option0);
    delete index1;
    index1 = faiss::gpu::index_cpu_to_gpu(&res, 1, &index_composition1, &option1);
    delete index1;

//    double tx = getmillisecs();
    std::thread t1(gpu_executor, 0, &option0, &index_composition0);
    std::thread t2(gpu_executor, 1, &option1, &index_composition1);
    t1.join();
    t2.join();

//    std::thread t3(gpu_executor, 0, &option0, &index_composition0);
//    std::thread t4(gpu_executor, 1, &option1, &index_composition1);
//    t3.join();
//    t4.join();
//    double ty = getmillisecs();
//    printf("Total GPU execution time: %0.2f\n", ty - tx);
    cpu_executor(&index_composition0);
    cpu_executor(&index_composition1);

    delete [] xq;
    return 0;
}
