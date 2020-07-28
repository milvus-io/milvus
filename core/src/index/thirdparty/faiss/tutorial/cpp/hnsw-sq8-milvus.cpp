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

#include "../../../hnswlib/hnswalg_nm.h"
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

using namespace hnswlib_nm;
int main() {
    int d = 128;                            // dimension
    int nb = 1000000;                       // database size
    int nq = 10000;                        // nb of queries
    int M = 16;
    int efConstruction = 200;
    int efSearch = 100;
    int topk = 10;

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


    L2Space *space = new L2Space(d);
    std::shared_ptr<HierarchicalNSW_NM<float>> hnsw = std::make_shared<HierarchicalNSW_NM<float>>(space, nb, M, efConstruction);
    auto sq8_code = new uint8_t[d * (nb + 2 * sizeof(float))];
    hnsw->sq_train(nb, (const float*)xb, sq8_code);
//    hnsw->SetSq8((float*)(sq8_code + d * nb));

    auto ts = std::chrono::high_resolution_clock::now();
    hnsw->addPoint(xb, 0, 0, 0);
    std::cout << "the first point added" << std::endl;
#pragma omp parallel for
    for (int i = 1; i < nb; ++ i) {
//        hnsw->addPoint(xb + i * d, i, 0, i);
        hnsw->addPoint(xb, i, 0, i);
    }
    auto te = std::chrono::high_resolution_clock::now();
    std::cout << "build index costs: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms " << std::endl;

    {       // search xq
//        long *I = new long[k * nq];
//        float *D = new float[k * nq];

        using P = std::pair<float, int64_t>;
        auto compare = [](const P& v1, const P& v2) { return v1.first < v2.first; };
        hnsw->setEf(efSearch);

        ts = std::chrono::high_resolution_clock::now();
        int correct_cnt = 0;
#pragma omp parallel for
        for (int i = 0; i < nq; ++ i) {
            std::vector<P> ret;
            ret = hnsw->searchKnn_NM((void*)(xb + i * d), topk, compare, nullptr, (float*)sq8_code);
            /*
            {
                for (auto ii = 0; ii < topk; ++ ii) {
                    if (i == ret[ii].second || ret[ii].first < 1e-5) {
                        correct_cnt ++;
                        break;
                    }
                }
            }
            */
        }
        te = std::chrono::high_resolution_clock::now();
        std::cout << "search " << nq << " times costs: " << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms " << std::endl;
        std::cout << "correct query of top" << topk << " is " << correct_cnt << std::endl;

//        delete [] I;
//        delete [] D;
    }



    delete [] xb;
    delete [] xq;
    delete [] sq8_code;

    return 0;
}
