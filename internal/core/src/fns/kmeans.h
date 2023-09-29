// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. 
//Copyright (c) 2023 Baidu, Inc.  All Rights Reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
/**
 * @file    kmeans.h
 * @author  yinjie06(yinjie06@baidu.com)
 * @date    2023/07/25 11:11
 * @brief
 *
 **/
#pragma once
#include <thread>
#include <random>
#include <chrono>
#include <vector>

namespace puck {
enum KMeansCenterInitType {
    RANDOM = 0,
    KMEANS_PLUS_PLUS = 1,
};
//Kmeans聚类的参数
struct KmeansParams {
    int redo;
    int nt;
    int niter;
    KMeansCenterInitType init_type;
    KmeansParams(bool kmeans_pp = false) {
        redo    = 1;
        nt      = std::thread::hardware_concurrency();
        niter   = 30;
        init_type = kmeans_pp ? KMeansCenterInitType::KMEANS_PLUS_PLUS : KMeansCenterInitType::RANDOM;
    }
};
int nearest_center(uint32_t dim, const float* centroids, const size_t centroid_cnt,
                   const float* point, const size_t point_cnt, int* assign, float* dis);
class Kmeans {
public:
    Kmeans(): _rnd(time(0)) {}
    Kmeans(bool kmeans_pp): _params(kmeans_pp), _rnd(time(0)) {}
    ~Kmeans() {}
    float kmeans(uint32_t dim, size_t n, size_t k,
                 const float* v,
                 float* centroids_out, float* dis_out = nullptr,
                 int* assign_out = nullptr);
    KmeansParams& get_params() {
        return _params;
    }

protected:
    void random_init_center(const size_t total_cnt, const size_t sample_cnt,
                            const uint32_t dim,
                            const float* train_dataset, std::vector<size_t>& sample_ids);

    //double drand_r(unsigned int* seed);
    void kmeanspp_init_center(const size_t total_cnt, const size_t sample_cnt,
                              const uint32_t dim,
                              const float* train_dataset, std::vector<size_t>& sample_ids);

    int kmeans_reassign_empty(uint32_t dim, size_t total_cnt, size_t k, float* centroids,
                              int* assign, int* nassign);

    int kmeans_core(uint32_t dim, size_t total_cnt, size_t k, int niter, int nt,
                    float* centroids, const float* v,
                    int* assign, int* nassign,
                    float* dis,
                    double* qerr_out);

private:
    int roulette_selection(std::vector<float>& wheel);

private:
    uint32_t _dim;
    KmeansParams _params;
    std::mt19937 _rnd;
};
}// namespace puck

