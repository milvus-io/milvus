#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <unistd.h>

#include <iostream>

#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/GpuAutoTune.h>


#include "faiss/FaissAssert.h"
#include "faiss/AuxIndexStructures.h"

#include "faiss/IndexFlat.h"
#include "faiss/VectorTransform.h"
#include "faiss/IndexLSH.h"
#include "faiss/IndexPQ.h"
#include "faiss/IndexIVF.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/IndexIVFSpectralHash.h"
#include "faiss/MetaIndexes.h"
#include "faiss/IndexScalarQuantizer.h"
#include "faiss/IndexHNSW.h"
#include "faiss/OnDiskInvertedLists.h"
#include "faiss/IndexBinaryFlat.h"
#include "faiss/IndexBinaryFromFloat.h"
#include "faiss/IndexBinaryHNSW.h"
#include "faiss/IndexBinaryIVF.h"
#include "faiss/gpu/GpuIndexIVFSQ.h"
#include "faiss/utils.h"


using namespace faiss;

void
generate_file(const char *filename,
                   long nb,
                   long dimension,
                   std::string index_desc,
                   faiss::gpu::StandardGpuResources &res) {
    long size = dimension * nb;
    float *xb = new float[size];
    printf("size: %lf(GB)\n", (size * sizeof(float)) / (3 * 1024.0 * 1024 * 1024));
    for (long i = 0; i < nb; i++) {
        for (long j = 0; j < dimension; j++) {
            float rand = drand48();
            xb[dimension * i + j] = rand;
        }
    }

    faiss::Index *ori_index = faiss::index_factory(dimension, index_desc.c_str(), faiss::METRIC_L2);
    auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 0, ori_index);

    assert(!device_index->is_trained);
    device_index->train(nb, xb);
    assert(device_index->is_trained);
    device_index->add(nb, xb);

    faiss::Index *cpu_index = faiss::gpu::index_gpu_to_cpu((device_index));
    faiss::write_index(cpu_index, filename);
    printf("index: %s is stored successfully.\n", filename);
    delete[] xb;

    return;
}

faiss::Index *
get_index(const char *filename) {
    return faiss::read_index(filename);
}

void
execute_on_gpu(faiss::Index *index, float *xq, long nq, long k, long nprobe,
    faiss::gpu::StandardGpuResources &res, long* I, float* D) {

    double t0 = getmillisecs();

    faiss::gpu::CpuToGpuClonerOptions option;
    option.readonly = true;
    faiss::Index *tmp_index = faiss::gpu::cpu_to_gpu(&res, 0, index, &option);
    std::shared_ptr<faiss::Index> gpu_index_ivf_ptr = std::shared_ptr<faiss::Index>(tmp_index);

    double t1 = getmillisecs();
    printf("CPU to GPU loading time: %0.2f\n", t1 - t0);


    double t2 = getmillisecs();
    faiss::gpu::GpuIndexIVF *gpu_index_ivf =
        dynamic_cast<faiss::gpu::GpuIndexIVF *>(gpu_index_ivf_ptr.get());
    gpu_index_ivf->setNumProbes(nprobe);

    gpu_index_ivf_ptr->search(nq, xq, k, D, I);
    double t3 = getmillisecs();
    printf("GPU execution time: %0.2f\n", t3 - t2);
}

void execute_on_cpu(faiss::Index *index, float* xq, long nq, long k, long nprobe, long* I, float* D) {
    faiss::IndexIVF* ivf_index =
        dynamic_cast<faiss::IndexIVF*>(index);
    ivf_index->nprobe = nprobe;
    index->search(nq, xq, k, D, I);
}

float *construct_queries(long nq, long dimension) {
    float *xq = new float[dimension * nq];
    for (int i = 0; i < nq; i++) {
        for (int j = 0; j < dimension; j++) {
            xq[dimension * i + j] = drand48();
        }
    }
    return xq;
}

void print_result(long number, long nq, long k, long *I, float *D) {
    printf("I (%ld first results)=\n", number);
    for (int i = 0; i < number; i++) {
        for (int j = 0; j < k; j++)
            printf("%5ld ", I[i * k + j]);
        printf("\n");
    }

    printf("I (%ld last results)=\n", number);
    for (int i = nq - number; i < nq; i++) {
        for (int j = 0; j < k; j++)
            printf("%5ld ", I[i * k + j]);
        printf("\n");
    }
}

void faiss_setting() {
    faiss::distance_compute_blas_threshold = 800;
}

int main() {
    const char *filename = "index5.index";

#if 0
    long dimension = 512;
    long nb = 6000000;
    long nq = 1000;
    long topk = 16;
    long print_number = 8;
    long nprobe = 32;

    std::string index_desc = "IVF16384,SQ8";
    faiss::gpu::StandardGpuResources res;
    if ((access(filename, F_OK)) == -1) {
        printf("file doesn't exist, create one\n");
        generate_file(filename, nb, dimension, index_desc, res);
    }

    // Construct queries
    float *xq = construct_queries(nq, dimension);

    // Read index
    faiss::Index *index = get_index(filename);

    // Execute on GPU
    long *I = new long[topk * nq];
    float *D = new float[topk * nq];
    execute_on_gpu(index, xq, nq, topk, nprobe, res, I, D);

    // Print results
    print_result(print_number, nq, topk, I, D);
    delete[] I; I = nullptr;
    delete[] D; D = nullptr;

    // Execute on CPU
    I = new long[topk * nq];
    D = new float[topk * nq];
    execute_on_cpu(index, xq, nq, topk, nprobe, I, D);

    // Print results
    print_result(print_number, nq, topk, I, D);
    delete[] I;
    delete[] D;

    return 0;
#else
    int number = 8;
    int d = 512;                            // dimension
    int nq = 1000;                        // nb of queries
    int nprobe = 16;
    float *xq = new float[d * nq];
    for(int i = 0; i < nq; i++) {
        for(int j = 0; j < d; j++) {
            xq[d * i + j] = drand48();
//            printf("%lf ", xq[d * i + j]);
        }
//        xq[d * i] += i / 1000.;
//        printf("\n");
    }
    faiss::distance_compute_blas_threshold = 800;

    faiss::gpu::StandardGpuResources res;

    int k = 16;
    std::shared_ptr<faiss::Index> gpu_index_ivf_ptr;

    const char* index_description = "IVF16384,SQ8";
    // const char* index_description = "IVF3276,Flat";
//    Index *index_factory (int d, const char *description,
//                          MetricType metric = METRIC_L2);

    faiss::Index *cpu_index = nullptr;
    if((access(filename,F_OK))==-1) {
        long nb = 6000000;
        long dimension = d;
        printf("file doesn't exist, create one\n");
        generate_file(filename, nb, dimension, index_description, res);
        /*
        // create database
                               // database size
//        printf("-----------------------\n");
        long size = d * nb;
        float *xb = new float[size];
        memset(xb, 0, size * sizeof(float));
        printf("size: %ld\n", (size * sizeof(float)) );
        for(long i = 0; i < nb; i++) {
            for(long j = 0; j < d; j++) {
                float rand = drand48();
                xb[d * i + j] = rand;
//                printf("%lf ", xb[d * i + j]);
            }
//            xb[d * i] += i / 1000.;
//            printf("\n");
        }

        // Using an IVF index
        // here we specify METRIC_L2, by default it performs inner-product search

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
         */
    } else {
        cpu_index = get_index(filename);
    }

    {
        // cpu to gpu
        double t0 = getmillisecs ();
        faiss::gpu::CpuToGpuClonerOptions option;
        option.readonly = true;
        faiss::Index* tmp_index = faiss::gpu::cpu_to_gpu(&res, 0, cpu_index, &option);

        gpu_index_ivf_ptr = std::shared_ptr<faiss::Index>(tmp_index);

        // Gpu index dump

        auto gpu_index_ivf_sq_ptr = dynamic_cast<faiss::gpu::GpuIndexIVFSQ*>(tmp_index);
//        gpu_index_ivf_sq_ptr->dump();
        double t1 = getmillisecs ();
        printf("CPU to GPU loading time: %0.2f\n", t1 - t0);
        // // Cpu index dump
        // auto cpu_index_ivf_sq_ptr = dynamic_cast<faiss::IndexIVF*>(cpu_index);
        // cpu_index_ivf_sq_ptr->dump();
    }


    {       // search xq
        long *I = new long[k * nq];
        float *D = new float[k * nq];
        double t2 = getmillisecs();
        faiss::gpu::GpuIndexIVF* gpu_index_ivf =
            dynamic_cast<faiss::gpu::GpuIndexIVF*>(gpu_index_ivf_ptr.get());
        gpu_index_ivf->setNumProbes(nprobe);

        gpu_index_ivf_ptr->search(nq, xq, k, D, I);
        double t3 = getmillisecs();
        printf("GPU execution time: %0.2f\n", t3 - t2);

        // print results
        printf("GPU: \n");
#if 0
        printf("GPU: I (2 first results)=\n");
        for(int i = 0; i < number; i++) {
            for(int j = 0; j < k; j++)
                printf("GPU: %5ld(%f) ", I[i * k + j], D[i * k + j]);
            printf("\n");
        }

        printf("GPU: I (2 last results)=\n");
        for(int i = nq - number; i < nq; i++) {
            for(int j = 0; j < k; j++)
                printf("GPU: %5ld(%f) ", I[i * k + j], D[i * k + j]);
            printf("\n");
        }
#else
        printf("I (2 first results)=\n");
        for(int i = 0; i < number; i++) {
            for(int j = 0; j < k; j++)
                printf("%5ld ", I[i * k + j]);
            printf("\n");
        }

        printf("I (2 last results)=\n");
        for(int i = nq - number; i < nq; i++) {
            for(int j = 0; j < k; j++)
                printf("%5ld ", I[i * k + j]);
            printf("\n");
        }
#endif
        delete [] I;
        delete [] D;
    }
    printf("----------------------------------\n");
    {       // search xq
        printf("CPU: \n");
        long *I = new long[k * nq];
        float *D = new float[k * nq];

        double t4 = getmillisecs();
        faiss::IndexIVF* ivf_index =
            dynamic_cast<faiss::IndexIVF*>(cpu_index);
        ivf_index->nprobe = nprobe;
        cpu_index->search(nq, xq, k, D, I);
        double t5 = getmillisecs();
        printf("CPU execution time: %0.2f\n", t5 - t4);
#if 0
        // print results
        printf("CPU: I (2 first results)=\n");
        for(int i = 0; i < number; i++) {
            for(int j = 0; j < k; j++)
                printf("CPU: %5ld(%f) ", I[i * k + j], D[i * k + j]);
            printf("\n");
        }

        printf("CPU: I (2 last results)=\n");
        for(int i = nq - number; i < nq; i++) {
            for(int j = 0; j < k; j++)
                printf("CPU: %5ld(%f) ", I[i * k + j], D[i * k + j]);
            printf("\n");
        }
#else
        // print results
        printf("I (2 first results)=\n");
        for(int i = 0; i < number; i++) {
            for(int j = 0; j < k; j++)
                printf("%5ld ", I[i * k + j]);
            printf("\n");
        }

        printf("I (2 last results)=\n");
        for(int i = nq - number; i < nq; i++) {
            for(int j = 0; j < k; j++)
                printf("%5ld ", I[i * k + j]);
            printf("\n");
        }
#endif
        delete [] I;
        delete [] D;
    }


    delete [] xq;
    return 0;
#endif
}