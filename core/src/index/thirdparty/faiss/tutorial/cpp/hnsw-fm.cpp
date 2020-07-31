/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <iostream>
#include <cstdlib>
#include <cassert>
#include <chrono>

#include <faiss/IndexRHNSW.h>
#include "/usr/include/hdf5/serial/hdf5.h"
#include "/usr/include/hdf5/serial/H5Cpp.h"


void LoadData(const std::string file_location, float *&data, const std::string data_name, int &dim, int &num_vets) {
    hid_t fd;
    herr_t status;
    fd = H5Fopen(file_location.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
    hid_t dataset_id;
    dataset_id = H5Dopen2(fd, data_name.c_str(), H5P_DEFAULT);
    status = H5Dread(dataset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
    hid_t dspace = H5Dget_space(dataset_id);
    hsize_t dims[2];
    H5Sget_simple_extent_dims(dspace, dims, NULL);
    num_vets = dims[0];
    dim = dims[1];
    status = H5Dclose(dataset_id);
    status = H5Fclose(fd);
}

int main() {
    int d = 128;                            // dimension
    int nb = 1000000;                       // database size
//    int nb = 100;                       // database size
//    int nq = 10000;                        // nb of queries
    int nq = 100;                        // nb of queries
    int M = 16;
    int efConstruction = 200;
    int efSearch = 100;
    int topk = 3;

    float *xb = new float[d * nb];
    float *xq = new float[d * nq];

    srand(12345);
    for(int i = 0; i < nb; i++) {
        for(int j = 0; j < d; j++)
            xb[d * i + j] = drand48();
        xb[d * i] += i / 1000.;
    }

    /*
    std::string data_location = "/home/zilliz/workspace/data/sift-128-euclidean.hdf5";
    std::cout << "start load sift data ..." << std::endl;
    auto ts = std::chrono::high_resolution_clock::now();
    LoadData(data_location, xb, "train", d, nb);
    std::cout << "dim: " << d << ", rows: " << nb << std::endl;
    auto te = std::chrono::high_resolution_clock::now();
    std::cout << "load data costs: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms " << std::endl;
    */

    for(int i = 0; i < nq; i++) {
        for(int j = 0; j < d; j++)
            xq[d * i + j] = drand48();
        xq[d * i] += i / 1000.;
    }


    std::shared_ptr<faiss::IndexRHNSWFlat> hnsw = std::make_shared<faiss::IndexRHNSWFlat>(d, M);
    hnsw->hnsw.efConstruction = efConstruction;
    hnsw->hnsw.efSearch = efSearch;

//    hnsw->verbose = true;

    std::cout << "start to build" << std::endl;
    auto ts = std::chrono::high_resolution_clock::now();
    hnsw->add(nb, xb);
    auto te = std::chrono::high_resolution_clock::now();
    std::cout << "build index costs: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms " << std::endl;

    {       // search xq
        long *I = new long[topk * nq];
        float *D = new float[topk * nq];

        ts = std::chrono::high_resolution_clock::now();
        int correct_cnt = 0;
        hnsw->search(nq, xb, topk, D, I, nullptr);
        {
            for (auto i = 0; i < nq; ++ i) {
                if (i == I[i * topk] || D[i * topk] < 1e-5)
                    correct_cnt ++;
                for (auto j = 0; j < topk; ++ j)
                    std::cout << "query " << i << ", topk " << j << ": id = " << I[i * topk + j] << ", dis = " << D[i * topk + j] << std::endl;
            }
        }
        te = std::chrono::high_resolution_clock::now();
        std::cout << "search " << nq << " times costs: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms " << std::endl;
        std::cout << "correct query of top" << topk << " is " << correct_cnt << std::endl;

        delete [] I;
        delete [] D;
    }



    delete [] xb;
    delete [] xq;

    return 0;
}

