// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <faiss/AutoTune.h>
#include <faiss/Index.h>
#include <faiss/IndexIVF.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVFSQHybrid.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/index_io.h>
#include <faiss/utils.h>

#include <hdf5.h>

#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

/*****************************************************
 * To run this test, please download the HDF5 from
 *  https://support.hdfgroup.org/ftp/HDF5/releases/
 * and install it to /usr/local/hdf5 .
 *****************************************************/

double
elapsed() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void
normalize(float* arr, size_t nq, size_t dim) {
    for (size_t i = 0; i < nq; i++) {
        double vecLen = 0.0;
        for (size_t j = 0; j < dim; j++) {
            double val = arr[i * dim + j];
            vecLen += val * val;
        }
        vecLen = std::sqrt(vecLen);
        for (size_t j = 0; j < dim; j++) {
            arr[i * dim + j] = (float)(arr[i * dim + j] / vecLen);
        }
    }
}

void*
hdf5_read(const char* file_name, const char* dataset_name, H5T_class_t dataset_class, size_t& d_out, size_t& n_out) {
    hid_t file, dataset, datatype, dataspace, memspace;
    H5T_class_t t_class;   /* data type class */
    H5T_order_t order;     /* data order */
    size_t size;           /* size of the data element stored in file */
    hsize_t dimsm[3];      /* memory space dimensions */
    hsize_t dims_out[2];   /* dataset dimensions */
    hsize_t count[2];      /* size of the hyperslab in the file */
    hsize_t offset[2];     /* hyperslab offset in the file */
    hsize_t count_out[3];  /* size of the hyperslab in memory */
    hsize_t offset_out[3]; /* hyperslab offset in memory */
    int rank;
    void* data_out; /* output buffer */

    /* Open the file and the dataset. */
    file = H5Fopen(file_name, H5F_ACC_RDONLY, H5P_DEFAULT);
    dataset = H5Dopen2(file, dataset_name, H5P_DEFAULT);

    /*
     * Get datatype and dataspace handles and then query
     * dataset class, order, size, rank and dimensions.
     */
    datatype = H5Dget_type(dataset); /* datatype handle */
    t_class = H5Tget_class(datatype);
    assert(t_class == dataset_class || !"Illegal dataset class type");

    order = H5Tget_order(datatype);
    switch (order) {
        case H5T_ORDER_LE:
            printf("Little endian order \n");
            break;
        case H5T_ORDER_BE:
            printf("Big endian order \n");
            break;
        default:
            printf("Illegal endian order \n");
            break;
    }

    size = H5Tget_size(datatype);
    printf("Data size is %d \n", (int)size);

    dataspace = H5Dget_space(dataset); /* dataspace handle */
    rank = H5Sget_simple_extent_ndims(dataspace);
    H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
    n_out = dims_out[0];
    d_out = dims_out[1];
    printf("rank %d, dimensions %lu x %lu \n", rank, n_out, d_out);

    /* Define hyperslab in the dataset. */
    offset[0] = offset[1] = 0;
    count[0] = dims_out[0];
    count[1] = dims_out[1];
    H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

    /* Define the memory dataspace. */
    dimsm[0] = dims_out[0];
    dimsm[1] = dims_out[1];
    dimsm[2] = 1;
    memspace = H5Screate_simple(3, dimsm, NULL);

    /* Define memory hyperslab. */
    offset_out[0] = offset_out[1] = offset_out[2] = 0;
    count_out[0] = dims_out[0];
    count_out[1] = dims_out[1];
    count_out[2] = 1;
    H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count_out, NULL);

    /* Read data from hyperslab in the file into the hyperslab in memory and display. */
    switch (t_class) {
        case H5T_INTEGER:
            data_out = new int[dims_out[0] * dims_out[1]];
            H5Dread(dataset, H5T_NATIVE_INT, memspace, dataspace, H5P_DEFAULT, data_out);
            break;
        case H5T_FLOAT:
            data_out = new float[dims_out[0] * dims_out[1]];
            H5Dread(dataset, H5T_NATIVE_FLOAT, memspace, dataspace, H5P_DEFAULT, data_out);
            break;
        default:
            printf("Illegal dataset class type\n");
            break;
    }

    /* Close/release resources. */
    H5Tclose(datatype);
    H5Dclose(dataset);
    H5Sclose(dataspace);
    H5Sclose(memspace);
    H5Fclose(file);

    return data_out;
}

std::string
get_index_file_name(const std::string& ann_test_name, const std::string& index_key, int32_t data_loops) {
    size_t pos = index_key.find_first_of(',', 0);
    std::string file_name = ann_test_name;
    file_name = file_name + "_" + index_key.substr(0, pos) + "_" + index_key.substr(pos + 1);
    file_name = file_name + "_" + std::to_string(data_loops) + ".index";
    return file_name;
}

bool
parse_ann_test_name(const std::string& ann_test_name, size_t& dim, faiss::MetricType& metric_type) {
    size_t pos1, pos2;

    if (ann_test_name.empty())
        return false;

    pos1 = ann_test_name.find_first_of('-', 0);
    if (pos1 == std::string::npos)
        return false;
    pos2 = ann_test_name.find_first_of('-', pos1 + 1);
    if (pos2 == std::string::npos)
        return false;

    dim = std::stoi(ann_test_name.substr(pos1 + 1, pos2 - pos1 - 1));
    std::string metric_str = ann_test_name.substr(pos2 + 1);
    if (metric_str == "angular") {
        metric_type = faiss::METRIC_INNER_PRODUCT;
    } else if (metric_str == "euclidean") {
        metric_type = faiss::METRIC_L2;
    } else {
        return false;
    }

    return true;
}

int32_t
GetResultHitCount(const faiss::Index::idx_t* ground_index, const faiss::Index::idx_t* index, size_t ground_k, size_t k,
                  size_t nq, int32_t index_add_loops) {
    assert(ground_k <= k);
    int hit = 0;
    for (int i = 0; i < nq; i++) {
        // count the num of results exist in ground truth result set
        // each result replicates INDEX_ADD_LOOPS times
        for (int j_c = 0; j_c < ground_k; j_c++) {
            int r_c = index[i * k + j_c];
            int j_g = 0;
            for (; j_g < ground_k / index_add_loops; j_g++) {
                if (ground_index[i * ground_k + j_g] == r_c) {
                    hit++;
                    continue;
                }
            }
        }
    }
    return hit;
}

void
test_ann_hdf5(const std::string& ann_test_name, const std::string& index_key, int32_t index_add_loops,
              const std::vector<size_t>& nprobes, int32_t search_loops) {
    double t0 = elapsed();

    const std::string ann_file_name = ann_test_name + ".hdf5";

    faiss::MetricType metric_type;
    size_t dim;

    if (!parse_ann_test_name(ann_test_name, dim, metric_type)) {
        printf("Invalid ann test name: %s\n", ann_test_name.c_str());
        return;
    }

    faiss::Index* index;
    size_t d;

    std::string index_file_name = get_index_file_name(ann_test_name, index_key, index_add_loops);
    try {
        index = faiss::read_index(index_file_name.c_str());
        d = dim;
    } catch (...) {
        printf("Cannot read index file: %s\n", index_file_name.c_str());

        printf("[%.3f s] Loading train set\n", elapsed() - t0);

        size_t nb;
        float* xb = (float*)hdf5_read(ann_file_name.c_str(), "train", H5T_FLOAT, d, nb);
        assert(d == dim || !"dataset does not have correct dimension");

        if (metric_type == faiss::METRIC_INNER_PRODUCT) {
            printf("[%.3f s] Normalizing data set \n", elapsed() - t0);
            normalize(xb, nb, d);
        }

        printf("[%.3f s] Preparing index \"%s\" d=%ld\n", elapsed() - t0, index_key.c_str(), d);

        index = faiss::index_factory(d, index_key.c_str(), metric_type);

        printf("[%.3f s] Training on %ld vectors\n", elapsed() - t0, nb);

        index->train(nb, xb);

        printf("[%.3f s] Loading database\n", elapsed() - t0);

        // add index multiple times to get ~1G data set
        for (int i = 0; i < index_add_loops; i++) {
            printf("[%.3f s] Indexing database, size %ld*%ld\n", elapsed() - t0, nb, d);
            index->add(nb, xb);
        }

        faiss::write_index(index, index_file_name.c_str());

        delete[] xb;
    }

    size_t nq;
    float* xq;
    {
        printf("[%.3f s] Loading queries\n", elapsed() - t0);

        size_t d2;
        xq = (float*)hdf5_read(ann_file_name.c_str(), "test", H5T_FLOAT, d2, nq);
        assert(d == d2 || !"query does not have same dimension as train set");
    }

    size_t k;                 // nb of results per query in the GT
    faiss::Index::idx_t* gt;  // nq * k matrix of ground-truth nearest-neighbors
    {
        printf("[%.3f s] Loading ground truth for %ld queries\n", elapsed() - t0, nq);

        // load ground-truth and convert int to long
        size_t nq2;
        int* gt_int = (int*)hdf5_read(ann_file_name.c_str(), "neighbors", H5T_INTEGER, k, nq2);
        assert(nq2 == nq || !"incorrect nb of ground truth entries");

        gt = new faiss::Index::idx_t[k * nq];
        for (int i = 0; i < k * nq; i++) {
            gt[i] = gt_int[i];
        }
        delete[] gt_int;
    }

    for (auto nprobe : nprobes) {
        faiss::ParameterSpace params;

        std::string nprobe_str = "nprobe=" + std::to_string(nprobe);
        params.set_index_parameters(index, nprobe_str.c_str());

        // output buffers
#if 1
        const size_t NQ = 1000, K = 1000;
        faiss::Index::idx_t* I = new faiss::Index::idx_t[NQ * K];
        float* D = new float[NQ * K];

        printf("\n%s | %s | nprobe=%lu\n", ann_test_name.c_str(), index_key.c_str(), nprobe);
        printf("======================================================================================\n");
        for (size_t t_nq = 10; t_nq <= NQ; t_nq *= 10) {   // nq = {10, 100, 1000}
            for (size_t t_k = 100; t_k <= K; t_k *= 10) {  //  k = {100, 1000}
                faiss::indexIVF_stats.quantization_time = 0.0;
                faiss::indexIVF_stats.search_time = 0.0;

                double t_start = elapsed(), t_end;
                for (int i = 0; i < search_loops; i++) {
                    index->search(t_nq, xq, t_k, D, I);
                }
                t_end = elapsed();

                // k = 100 for ground truth
                int32_t hit = GetResultHitCount(gt, I, k, t_k, t_nq, index_add_loops);

                printf("nq = %4ld, k = %4ld, elapse = %.4fs (quant = %.4fs, search = %.4fs), R@ = %.4f\n", t_nq, t_k,
                       (t_end - t_start) / search_loops, faiss::indexIVF_stats.quantization_time / 1000 / search_loops,
                       faiss::indexIVF_stats.search_time / 1000 / search_loops,
                       (hit / float(t_nq * k / index_add_loops)));
            }
        }
        printf("======================================================================================\n");
#else
        printf("[%.3f s] Perform a search on %ld queries\n", elapsed() - t0, nq);

        faiss::Index::idx_t* I = new faiss::Index::idx_t[nq * k];
        float* D = new float[nq * k];

        index->search(nq, xq, k, D, I);

        printf("[%.3f s] Compute recalls\n", elapsed() - t0);

        // evaluate result by hand.
        int n_1 = 0, n_10 = 0, n_100 = 0;
        for (int i = 0; i < nq; i++) {
            int gt_nn = gt[i * k];
            for (int j = 0; j < k; j++) {
                if (I[i * k + j] == gt_nn) {
                    if (j < 1)
                        n_1++;
                    if (j < 10)
                        n_10++;
                    if (j < 100)
                        n_100++;
                }
            }
        }
        printf("R@1 = %.4f\n", n_1 / float(nq));
        printf("R@10 = %.4f\n", n_10 / float(nq));
        printf("R@100 = %.4f\n", n_100 / float(nq));
#endif

        printf("[%.3f s] Search test done\n\n", elapsed() - t0);

        delete[] I;
        delete[] D;
    }

    delete[] xq;
    delete[] gt;
    delete index;
}

#ifdef CUSTOMIZATION
void
test_ivfsq8h(const std::string& ann_test_name, int32_t index_add_loops, const std::vector<size_t>& nprobes,
             bool pure_gpu_mode, int32_t search_loops) {
    double t0 = elapsed();

    const std::string ann_file_name = ann_test_name + ".hdf5";

    faiss::MetricType metric_type;
    size_t dim;

    if (!parse_ann_test_name(ann_test_name, dim, metric_type)) {
        printf("Invalid ann test name: %s\n", ann_test_name.c_str());
        return;
    }

    faiss::distance_compute_blas_threshold = 800;
    faiss::gpu::StandardGpuResources res;

    const std::string index_key = "IVF16384,SQ8Hybrid";

    faiss::Index* cpu_index = nullptr;
    size_t d;

    std::string index_file_name = get_index_file_name(ann_test_name, index_key, index_add_loops);
    try {
        cpu_index = faiss::read_index(index_file_name.c_str());
        d = dim;
    } catch (...) {
        printf("Cannot read index file: %s\n", index_file_name.c_str());

        printf("[%.3f s] Loading train set\n", elapsed() - t0);

        size_t nb;
        float* xb = (float*)hdf5_read(ann_file_name.c_str(), "train", H5T_FLOAT, d, nb);
        assert(d == dim || !"dataset does not have correct dimension");

        printf("[%.3f s] Preparing index \"%s\" d=%ld\n", elapsed() - t0, index_key.c_str(), d);

        faiss::Index* ori_index = faiss::index_factory(d, index_key.c_str(), metric_type);

        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 0, ori_index);

        printf("[%.3f s] Training on %ld vectors\n", elapsed() - t0, nb);

        device_index->train(nb, xb);

        printf("[%.3f s] Loading database\n", elapsed() - t0);

        for (int i = 0; i < index_add_loops; i++) {
            printf("[%.3f s] Indexing database, size %ld*%ld\n", elapsed() - t0, nb, d);
            device_index->add(nb, xb);
        }

        cpu_index = faiss::gpu::index_gpu_to_cpu(device_index);
        faiss::write_index(cpu_index, index_file_name.c_str());

        delete[] xb;
    }

    faiss::IndexIVF* cpu_ivf_index = dynamic_cast<faiss::IndexIVF*>(cpu_index);
    if (cpu_ivf_index != nullptr) {
        cpu_ivf_index->to_readonly();
    }

    size_t nq;
    float* xq;
    {
        printf("[%.3f s] Loading queries\n", elapsed() - t0);

        size_t d2;
        xq = (float*)hdf5_read(ann_file_name.c_str(), "test", H5T_FLOAT, d2, nq);
        assert(d == d2 || !"query does not have same dimension as train set");
    }

    size_t k;
    faiss::Index::idx_t* gt;
    {
        printf("[%.3f s] Loading ground truth for %ld queries\n", elapsed() - t0, nq);

        size_t nq2;
        int* gt_int = (int*)hdf5_read(ann_file_name.c_str(), "neighbors", H5T_INTEGER, k, nq2);
        assert(nq2 == nq || !"incorrect nb of ground truth entries");

        gt = new faiss::Index::idx_t[k * nq];
        for (uint64_t i = 0; i < k * nq; ++i) {
            gt[i] = gt_int[i];
        }
        delete[] gt_int;
    }

    faiss::gpu::GpuClonerOptions option;
    option.allInGpu = true;

    faiss::IndexComposition index_composition;
    index_composition.index = cpu_index;
    index_composition.quantizer = nullptr;

    faiss::Index* index;
    double copy_time;

    if (!pure_gpu_mode) {
        index_composition.mode = 1;  // 0: all data, 1: copy quantizer, 2: copy data
        index = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition, &option);
        delete index;

        copy_time = elapsed();
        index = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition, &option);
        delete index;
    } else {
        index_composition.mode = 2;
        index = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition, &option);
        delete index;

        copy_time = elapsed();
        index = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition, &option);
    }

    copy_time = elapsed() - copy_time;
    printf("[%.3f s] Copy quantizer completed, cost %f s\n", elapsed() - t0, copy_time);

    const size_t NQ = 1000, K = 1000;
    if (!pure_gpu_mode) {
        for (auto nprobe : nprobes) {
            auto ivf_index = dynamic_cast<faiss::IndexIVF*>(cpu_index);
            ivf_index->nprobe = nprobe;

            auto is_gpu_flat_index = dynamic_cast<faiss::gpu::GpuIndexFlat*>(ivf_index->quantizer);
            if (is_gpu_flat_index == nullptr) {
                delete ivf_index->quantizer;
                ivf_index->quantizer = index_composition.quantizer;
            }

            int64_t* I = new faiss::Index::idx_t[NQ * K];
            float* D = new float[NQ * K];

            printf("\n%s | %s-MIX | nprobe=%lu\n", ann_test_name.c_str(), index_key.c_str(), nprobe);
            printf("======================================================================================\n");
            for (size_t t_nq = 10; t_nq <= NQ; t_nq *= 10) {   // nq = {10, 100, 1000}
                for (size_t t_k = 100; t_k <= K; t_k *= 10) {  //  k = {100, 1000}
                    faiss::indexIVF_stats.quantization_time = 0.0;
                    faiss::indexIVF_stats.search_time = 0.0;

                    double t_start = elapsed(), t_end;
                    for (int32_t i = 0; i < search_loops; i++) {
                        cpu_index->search(t_nq, xq, t_k, D, I);
                    }
                    t_end = elapsed();

                    // k = 100 for ground truth
                    int32_t hit = GetResultHitCount(gt, I, k, t_k, t_nq, index_add_loops);

                    printf("nq = %4ld, k = %4ld, elapse = %.4fs (quant = %.4fs, search = %.4fs), R@ = %.4f\n", t_nq,
                           t_k, (t_end - t_start) / search_loops,
                           faiss::indexIVF_stats.quantization_time / 1000 / search_loops,
                           faiss::indexIVF_stats.search_time / 1000 / search_loops,
                           (hit / float(t_nq * k / index_add_loops)));
                }
            }
            printf("======================================================================================\n");

            printf("[%.3f s] Search test done\n\n", elapsed() - t0);

            delete[] I;
            delete[] D;
        }
    } else {
        std::shared_ptr<faiss::Index> gpu_index_ivf_ptr = std::shared_ptr<faiss::Index>(index);

        for (auto nprobe : nprobes) {
            faiss::gpu::GpuIndexIVFSQHybrid* gpu_index_ivf_hybrid =
                dynamic_cast<faiss::gpu::GpuIndexIVFSQHybrid*>(gpu_index_ivf_ptr.get());
            gpu_index_ivf_hybrid->setNumProbes(nprobe);

            int64_t* I = new faiss::Index::idx_t[NQ * K];
            float* D = new float[NQ * K];

            printf("\n%s | %s-GPU | nprobe=%lu\n", ann_test_name.c_str(), index_key.c_str(), nprobe);
            printf("======================================================================================\n");
            for (size_t t_nq = 10; t_nq <= NQ; t_nq *= 10) {   // nq = {10, 100, 1000}
                for (size_t t_k = 100; t_k <= K; t_k *= 10) {  //  k = {100, 1000}
                    faiss::indexIVF_stats.quantization_time = 0.0;
                    faiss::indexIVF_stats.search_time = 0.0;

                    double t_start = elapsed(), t_end;
                    for (int32_t i = 0; i < search_loops; i++) {
                        gpu_index_ivf_ptr->search(nq, xq, k, D, I);
                    }
                    t_end = elapsed();

                    // k = 100 for ground truth
                    int32_t hit = GetResultHitCount(gt, I, k, t_k, t_nq, index_add_loops);

                    printf("nq = %4ld, k = %4ld, elapse = %.4fs (quant = %.4fs, search = %.4fs), R@ = %.4f\n", t_nq,
                           t_k, (t_end - t_start) / search_loops,
                           faiss::indexIVF_stats.quantization_time / 1000 / search_loops,
                           faiss::indexIVF_stats.search_time / 1000 / search_loops,
                           (hit / float(t_nq * k / index_add_loops)));
                }
            }
            printf("======================================================================================\n");

            printf("[%.3f s] Search test done\n\n", elapsed() - t0);

            delete[] I;
            delete[] D;
        }
    }

    delete[] xq;
    delete[] gt;
    delete cpu_index;
}
#endif

/************************************************************************************
 * https://github.com/erikbern/ann-benchmarks
 *
 * Dataset 	Dimensions 	Train_size 	Test_size 	Neighbors 	Distance 	Download
 * Fashion-
 *  MNIST   784         60,000      10,000 	    100         Euclidean   HDF5 (217MB)
 * GIST     960         1,000,000   1,000       100         Euclidean   HDF5 (3.6GB)
 * GloVe    100         1,183,514   10,000      100         Angular     HDF5 (463MB)
 * GloVe    200         1,183,514   10,000      100         Angular     HDF5 (918MB)
 * MNIST    784         60,000 	    10,000      100         Euclidean   HDF5 (217MB)
 * NYTimes  256         290,000     10,000      100         Angular     HDF5 (301MB)
 * SIFT     128         1,000,000   10,000      100         Euclidean   HDF5 (501MB)
 *************************************************************************************/

TEST(FAISSTEST, BENCHMARK) {
    std::vector<size_t> param_nprobes = {8, 128};
    const int32_t SEARCH_LOOPS = 5;
    const int32_t SIFT_INSERT_LOOPS = 2;  // insert twice to get ~1G data set
    const int32_t GLOVE_INSERT_LOOPS = 1;

    test_ann_hdf5("sift-128-euclidean", "IVF4096,Flat", SIFT_INSERT_LOOPS, param_nprobes, SEARCH_LOOPS);
    test_ann_hdf5("sift-128-euclidean", "IVF16384,SQ8", SIFT_INSERT_LOOPS, param_nprobes, SEARCH_LOOPS);
#ifdef CUSTOMIZATION
    test_ann_hdf5("sift-128-euclidean", "IVF16384,SQ8Hybrid", SIFT_INSERT_LOOPS, param_nprobes, SEARCH_LOOPS);
    test_ivfsq8h("sift-128-euclidean", SIFT_INSERT_LOOPS, param_nprobes, false, SEARCH_LOOPS);
    test_ivfsq8h("sift-128-euclidean", SIFT_INSERT_LOOPS, param_nprobes, true, SEARCH_LOOPS);
#endif

    test_ann_hdf5("glove-200-angular", "IVF4096,Flat", GLOVE_INSERT_LOOPS, param_nprobes, SEARCH_LOOPS);
    test_ann_hdf5("glove-200-angular", "IVF16384,SQ8", GLOVE_INSERT_LOOPS, param_nprobes, SEARCH_LOOPS);
#ifdef CUSTOMIZATION
    test_ann_hdf5("glove-200-angular", "IVF16384,SQ8Hybrid", GLOVE_INSERT_LOOPS, param_nprobes, SEARCH_LOOPS);
    test_ivfsq8h("glove-200-angular", GLOVE_INSERT_LOOPS, param_nprobes, false, SEARCH_LOOPS);
    test_ivfsq8h("glove-200-angular", GLOVE_INSERT_LOOPS, param_nprobes, true, SEARCH_LOOPS);
#endif
}
