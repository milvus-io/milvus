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

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cstring>

#include <faiss/AutoTune.h>
#include <faiss/Index.h>
#include <faiss/IndexIVF.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/index_io.h>
#include <faiss/utils.h>

#include <hdf5.h>

#include <vector>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

/*****************************************************
 * To run this test, please download the HDF5 from
 *  https://support.hdfgroup.org/ftp/HDF5/releases/
 * and install it to /usr/local/hdf5 .
 *****************************************************/

double elapsed() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void* hdf5_read(const char *file_name,
                const char *dataset_name,
                H5T_class_t dataset_class,
                size_t &d_out,
                size_t &n_out) {
    hid_t       file, dataset, datatype, dataspace, memspace;
    H5T_class_t t_class;            /* data type class */
    H5T_order_t order;              /* data order */
    size_t      size;               /* size of the data element stored in file */
    hsize_t     dimsm[3];           /* memory space dimensions */
    hsize_t     dims_out[2];        /* dataset dimensions */
    hsize_t     count[2];           /* size of the hyperslab in the file */
    hsize_t     offset[2];          /* hyperslab offset in the file */
    hsize_t     count_out[3];       /* size of the hyperslab in memory */
    hsize_t     offset_out[3];      /* hyperslab offset in memory */
    int         rank;
    void*       data_out;           /* output buffer */

    /* Open the file and the dataset. */
    file = H5Fopen(file_name, H5F_ACC_RDONLY, H5P_DEFAULT);
    dataset = H5Dopen2(file, dataset_name, H5P_DEFAULT);

    /*
     * Get datatype and dataspace handles and then query
     * dataset class, order, size, rank and dimensions.
     */
    datatype = H5Dget_type(dataset);     /* datatype handle */
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

    size  = H5Tget_size(datatype);
    printf("Data size is %d \n", (int)size);

    dataspace = H5Dget_space(dataset);    /* dataspace handle */
    rank      = H5Sget_simple_extent_ndims(dataspace);
    H5Sget_simple_extent_dims(dataspace, dims_out, NULL);
    n_out = dims_out[0];
    d_out = dims_out[1];
    printf("rank %d, dimensions %lu x %lu \n", rank, n_out, d_out);

    /* Define hyperslab in the dataset. */
    offset[0] = offset[1] = 0;
    count[0]  = dims_out[0];
    count[1]  = dims_out[1];
    H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, NULL, count, NULL);

    /* Define the memory dataspace. */
    dimsm[0] = dims_out[0];
    dimsm[1] = dims_out[1];
    dimsm[2] = 1;
    memspace = H5Screate_simple(3, dimsm, NULL);

    /* Define memory hyperslab. */
    offset_out[0] = offset_out[1] = offset_out[2] = 0;
    count_out[0]  = dims_out[0];
    count_out[1]  = dims_out[1];
    count_out[2]  = 1;
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

std::string get_index_file_name(const std::string& ann_test_name,
                                const std::string& index_key,
                                int32_t data_loops) {
    size_t pos = index_key.find_first_of(',', 0);
    std::string file_name = ann_test_name;
    file_name = file_name + "_" + index_key.substr(0, pos) + "_" + index_key.substr(pos+1);
    file_name = file_name + "_" + std::to_string(data_loops) + ".index";
    return file_name;
}

bool parse_ann_test_name(const std::string& ann_test_name,
                         size_t &dim,
                         faiss::MetricType &metric_type) {
    size_t pos1, pos2;

    if (ann_test_name.empty()) return false;

    pos1 = ann_test_name.find_first_of('-', 0);
    if (pos1 == std::string::npos) return false;
    pos2 = ann_test_name.find_first_of('-', pos1 + 1);
    if (pos2 == std::string::npos) return false;

    dim = std::stoi(ann_test_name.substr(pos1+1, pos2-pos1-1));
    std::string metric_str = ann_test_name.substr(pos2+1);
    if (metric_str == "angular") {
        metric_type = faiss::METRIC_INNER_PRODUCT;
    } else if (metric_str == "euclidean") {
        metric_type = faiss::METRIC_L2;
    } else {
        return false;
    }

    return true;
}

void test_ann_hdf5(const std::string& ann_test_name,
                   const std::string& index_key,
                   int32_t index_add_loops,
                   const std::vector<size_t>& nprobes) {
    double t0 = elapsed();

    const std::string ann_file_name = ann_test_name + ".hdf5";

    faiss::MetricType metric_type;
    size_t dim;

    if (!parse_ann_test_name(ann_test_name, dim, metric_type)) {
        printf("Invalid ann test name: %s\n", ann_test_name.c_str());
        return;
    }

    faiss::Index * index;
    size_t d;

    std::string index_file_name = get_index_file_name(ann_test_name, index_key, index_add_loops);
    try {
        index = faiss::read_index(index_file_name.c_str());
        d = dim;
    }
    catch (...) {
        printf("Cannot read index file: %s\n", index_file_name.c_str());

        printf ("[%.3f s] Loading train set\n", elapsed() - t0);

        size_t nb;
        float *xb = (float*)hdf5_read(ann_file_name.c_str(), "train", H5T_FLOAT, d, nb);
        assert(d == dim || !"dataset does not have correct dimension");

        printf ("[%.3f s] Preparing index \"%s\" d=%ld\n",
                elapsed() - t0, index_key.c_str(), d);

        index = faiss::index_factory(d, index_key.c_str(), metric_type);

        printf ("[%.3f s] Training on %ld vectors\n", elapsed() - t0, nb);

        index->train(nb, xb);

        printf ("[%.3f s] Loading database\n", elapsed() - t0);

        // add index multiple times to get ~1G data set
        for (int i = 0; i < index_add_loops; i++) {
            printf ("[%.3f s] Indexing database, size %ld*%ld\n", elapsed() - t0, nb, d);
            index->add(nb, xb);
        }

        faiss::write_index(index, index_file_name.c_str());

        delete [] xb;
    }

    size_t nq;
    float *xq;
    {
        printf ("[%.3f s] Loading queries\n", elapsed() - t0);

        size_t d2;
        xq = (float*)hdf5_read(ann_file_name.c_str(), "test", H5T_FLOAT, d2, nq);
        assert(d == d2 || !"query does not have same dimension as train set");
    }

    size_t k; // nb of results per query in the GT
    faiss::Index::idx_t *gt;  // nq * k matrix of ground-truth nearest-neighbors
    {
        printf ("[%.3f s] Loading ground truth for %ld queries\n", elapsed() - t0, nq);

        // load ground-truth and convert int to long
        size_t nq2;
        int *gt_int = (int*)hdf5_read(ann_file_name.c_str(), "neighbors", H5T_INTEGER, k, nq2);
        assert(nq2 == nq || !"incorrect nb of ground truth entries");

        gt = new faiss::Index::idx_t[k * nq];
        for(int i = 0; i < k * nq; i++) {
            gt[i] = gt_int[i];
        }
        delete [] gt_int;
    }

    for (auto nprobe : nprobes) {

        faiss::ParameterSpace params;

        printf ("[%.3f s] Setting parameter configuration 'nprobe=%lu' on index\n", elapsed() - t0, nprobe);

        std::string nprobe_str = "nprobe=" + std::to_string(nprobe);
        params.set_index_parameters(index, nprobe_str.c_str());

        // output buffers
#if 1
        const size_t NQ = 1000, K = 1000;
        faiss::Index::idx_t *I = new  faiss::Index::idx_t[NQ * K];
        float *D = new float[NQ * K];

        printf ("\n%s | %s | nprobe=%lu\n", ann_test_name.c_str(), index_key.c_str(), nprobe);
        printf ("====================================================\n");
        for (size_t t_nq = 10; t_nq <= NQ; t_nq *= 10) {    // nq = {10, 100, 1000}
            for (size_t t_k = 100; t_k <= K; t_k *= 10) {   //  k = {100, 1000}
                double t_start = elapsed(), t_end;

                index->search(t_nq, xq, t_k, D, I);

                t_end = elapsed();

                // k = 100 for ground truth
                int hit = 0;
                for (int i = 0; i < t_nq; i++) {
                    // count the num of results exist in ground truth result set
                    // consider: each result replicates DATA_LOOPS times
                    for (int j_c = 0; j_c < k; j_c++) {
                        int r_c = I[i * t_k + j_c];
                        for (int j_g = 0; j_g < k/index_add_loops; j_g++) {
                            if (gt[i * k + j_g] == r_c) {
                                hit++;
                                continue;
                            }
                        }
                    }
                }
                printf("nq = %4ld, k = %4ld, elapse = %fs, R@ = %.4f\n",
                       t_nq, t_k, (t_end - t_start), (hit / float(t_nq * k / index_add_loops)));
            }
        }
        printf ("====================================================\n");
#else
        printf ("[%.3f s] Perform a search on %ld queries\n", elapsed() - t0, nq);

        faiss::Index::idx_t *I = new  faiss::Index::idx_t[nq * k];
        float *D = new float[nq * k];

        index->search(nq, xq, k, D, I);

        printf ("[%.3f s] Compute recalls\n", elapsed() - t0);

        // evaluate result by hand.
        int n_1 = 0, n_10 = 0, n_100 = 0;
        for(int i = 0; i < nq; i++) {
            int gt_nn = gt[i * k];
            for(int j = 0; j < k; j++) {
                if (I[i * k + j] == gt_nn) {
                    if(j < 1) n_1++;
                    if(j < 10) n_10++;
                    if(j < 100) n_100++;
                }
            }
        }
        printf("R@1 = %.4f\n", n_1 / float(nq));
        printf("R@10 = %.4f\n", n_10 / float(nq));
        printf("R@100 = %.4f\n", n_100 / float(nq));
#endif

        printf ("[%.3f s] Search test done\n\n", elapsed() - t0);

        delete [] I;
        delete [] D;
    }

    delete [] xq;
    delete [] gt;
    delete index;
}

#ifdef CUSTOMIZATION
void test_ivfsq8h_gpu(const std::string& ann_test_name,
                      int32_t index_add_loops,
                      const std::vector<size_t>& nprobes){
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
    try{
        cpu_index = faiss::read_index(index_file_name.c_str());
        d = dim;
    }
    catch (...){
        printf("Cannot read index file: %s\n", index_file_name.c_str());

        printf ("[%.3f s] Loading train set\n", elapsed() - t0);

        size_t  nb;
        float *xb = (float*)hdf5_read(ann_file_name.c_str(), "train", H5T_FLOAT, d, nb);
        assert(d == dim || !"dataset does not have correct dimension");

        printf ("[%.3f s] Preparing index \"%s\" d=%ld\n", elapsed() - t0, index_key.c_str(), d);

        faiss::Index *ori_index = faiss::index_factory(d, index_key.c_str(), metric_type);

        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 0, ori_index);

        printf ("[%.3f s] Training on %ld vectors\n", elapsed() - t0, nb);

        device_index->train(nb, xb);

        printf ("[%.3f s] Loading database\n", elapsed() - t0);

        for (int i = 0; i < index_add_loops; i++) {
            printf ("[%.3f s] Indexing database, size %ld*%ld\n", elapsed() - t0, nb, d);
            device_index->add(nb, xb);
        }

        cpu_index = faiss::gpu::index_gpu_to_cpu(device_index);
        faiss::write_index(cpu_index, index_file_name.c_str());

        delete []xb;
    }

    faiss::IndexIVF *cpu_ivf_index = dynamic_cast<faiss::IndexIVF*>(cpu_index);
    if(cpu_ivf_index != nullptr) {
        cpu_ivf_index->to_readonly();
    }

    faiss::gpu::GpuClonerOptions option;
    option.allInGpu = true;

    faiss::IndexComposition index_composition;
    index_composition.index = cpu_index;
    index_composition.quantizer = nullptr;
    index_composition.mode = 1;

    auto index = faiss::gpu::index_cpu_to_gpu(&res, 0, &index_composition, &option);
    delete index;

    size_t nq;
    float *xq;
    {
        printf ("[%.3f s] Loading queries\n", elapsed() - t0);

        size_t d2;
        xq = (float*)hdf5_read(ann_file_name.c_str(), "test", H5T_FLOAT, d2, nq);
        assert(d == d2 || !"query does not have same dimension as train set");
    }

    size_t k;
    faiss::Index::idx_t *gt;
    {
        printf ("[%.3f s] Loading ground truth for %ld queries\n", elapsed() - t0, nq);

        size_t nq2;
        int *gt_int = (int*)hdf5_read(ann_file_name.c_str(), "neighbors", H5T_INTEGER, k, nq2);
        assert(nq2 == nq || !"incorrect nb of ground truth entries");

        gt = new faiss::Index::idx_t[k * nq];
        for (unsigned long i = 0; i < k * nq; ++i) {
            gt[i] = gt_int[i];
        }
        delete []gt_int;
    }

    for (auto nprobe : nprobes){
        printf ("[%.3f s] Setting parameter configuration 'nprobe=%lu' on index\n",
                elapsed() - t0, nprobe);

        auto ivf_index = dynamic_cast<faiss::IndexIVF *>(cpu_index);
        ivf_index->nprobe = nprobe;

        auto is_gpu_flat_index = dynamic_cast<faiss::gpu::GpuIndexFlat*>(ivf_index->quantizer);
        if(is_gpu_flat_index == nullptr) {
            delete ivf_index->quantizer;
            ivf_index->quantizer = index_composition.quantizer;
        }

        const size_t NQ = 1000, K = 1000;
        long *I = new  faiss::Index::idx_t[NQ * K];
        float *D = new float[NQ * K];

        printf ("\n%s %ld\n", index_key.c_str(), nprobe);
        printf ("\n%s | %s | nprobe=%lu\n", ann_test_name.c_str(), index_key.c_str(), nprobe);
        printf ("====================================================\n");

        for (size_t t_nq = 10; t_nq <= NQ; t_nq *= 10) {    // nq = {10, 100, 1000}
            for (size_t t_k = 100; t_k <= K; t_k *= 10) {   //  k = {100, 1000}
                double t_start = elapsed(), t_end;

                cpu_index->search(t_nq, xq, t_k, D, I);

                t_end = elapsed();

                // k = 100 for ground truth
                int hit = 0;
                for (unsigned long i = 0; i < t_nq; i++) {
                    // count the num of results exist in ground truth result set
                    // consider: each result replicates DATA_LOOPS times
                    for (unsigned long j_c = 0; j_c < k; j_c++) {
                        int r_c = I[i * t_k + j_c];
                        for (unsigned long j_g = 0; j_g < k/index_add_loops; j_g++) {
                            if (gt[i * k + j_g] == r_c) {
                                hit++;
                                continue;
                            }
                        }
                    }
                }
                printf("nq = %4ld, k = %4ld, elapse = %fs, R@ = %.4f\n",
                       t_nq, t_k, (t_end - t_start), (hit / float(t_nq * k / index_add_loops)));
            }
        }
        printf ("====================================================\n");

        printf ("[%.3f s] Search test done\n\n", elapsed() - t0);

        delete [] I;
        delete [] D;
    }

    delete [] xq;
    delete [] gt;
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

TEST(FAISSTEST, sift1m_L2) {
    test_ann_hdf5("sift-128-euclidean", "IVF4096,Flat",       2, {8, 128});
    test_ann_hdf5("sift-128-euclidean", "IVF16384,SQ8",       2, {8, 128});
    test_ann_hdf5("sift-128-euclidean", "IVF16384,SQ8Hybrid", 2, {8, 128});
#ifdef CUSTOMIZATION
    test_ivfsq8h_gpu("sift-128-euclidean", 2, {8, 128});
#endif

    test_ann_hdf5("glove-200-angular", "IVF4096,Flat",       1, {8, 128});
    test_ann_hdf5("glove-200-angular", "IVF16384,SQ8",       1, {8, 128});
    test_ann_hdf5("glove-200-angular", "IVF16384,SQ8Hybrid", 1, {8, 128});
#ifdef CUSTOMIZATION
    test_ivfsq8h_gpu("glove-200-angular", 2, {128, 1024});
#endif
}

