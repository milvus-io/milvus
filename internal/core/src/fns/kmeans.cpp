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
 * @file    kmeans.cpp
 * @author  yinjie06(yinjie06@baidu.com)
 * @date    2023/07/25 11:11
 * @brief
 *
 **/

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#ifndef __APPLE__
#include <cblas.h>
#else
#include <Accelerate/Accelerate.h>
#endif
#include <random>
#include "kmeans.h"
#include <iostream>

namespace milvus::puck {

void
Kmeans::random_init_center(const size_t total_cnt,
                           const size_t sample_cnt,
                           const uint32_t dim,
                           const float* train_dataset,
                           std::vector<size_t>& sample_ids) {
    sample_ids.clear();
    std::uniform_int_distribution<> dis(0, total_cnt - 1);
    std::vector<bool> filter(total_cnt, false);

    while (sample_ids.size() < sample_cnt) {
        size_t sample_id = dis(_rnd);

        //去重
        if (filter[sample_id]) {
            continue;
        }

        sample_ids.push_back(sample_id);
        filter[sample_id] = true;
    }
}

int
Kmeans::roulette_selection(std::vector<float>& wheel) {
    float total_val = 0;

    for (auto& val : wheel) {
        total_val += val;
    }

    cblas_sscal(wheel.size(), 1.0 / total_val, wheel.data(), 1);
    std::uniform_real_distribution<double> dis(0, 1.0);
    double rd = dis(_rnd);

    for (auto id = 0; id < wheel.size(); ++id) {
        rd -= wheel[id];

        if (rd < 0) {
            return id;
        }
    }

    return wheel.size() - 1;
}

void
Kmeans::kmeanspp_init_center(const size_t total_cnt,
                             const size_t sample_cnt,
                             const uint32_t dim,
                             const float* train_dataset,
                             std::vector<size_t>& sample_ids) {
    std::vector<float> disbest(total_cnt, std::numeric_limits<float>::max());
    std::vector<float> distmp(total_cnt);
    sample_ids.resize(sample_cnt, 0);
    sample_ids[0] = _rnd() % total_cnt;

    std::vector<float> points_norm(total_cnt, 0);

#pragma omp parallel for schedule(dynamic) num_threads(_params.nt)

    for (size_t j = 0; j < total_cnt; j++) {
        points_norm[j] = cblas_sdot(
            dim, train_dataset + j * dim, 1, train_dataset + j * dim, 1);
    }

    for (size_t i = 1; i < sample_cnt; i++) {
        size_t newsel = sample_ids[i - 1];
        const float* last_center = train_dataset + newsel * dim;

#pragma omp parallel for schedule(dynamic) num_threads(_params.nt)

        for (size_t j = 0; j < total_cnt; j++) {
            float temp =
                points_norm[j] + points_norm[newsel] -
                2.0 *
                    cblas_sdot(dim, train_dataset + j * dim, 1, last_center, 1);

            if (temp < disbest[j]) {
                disbest[j] = temp;
            }
        }

        memcpy(distmp.data(), disbest.data(), total_cnt * sizeof(distmp[0]));
        sample_ids[i] = roulette_selection(distmp);
    }
}

int
Kmeans::kmeans_reassign_empty(uint32_t dim,
                              size_t total_cnt,
                              size_t k,
                              float* centroids,
                              int* assign,
                              int* nassign) {
    std::vector<float> proba_split(k);
    std::vector<float> vepsilon(dim);
    std::normal_distribution<> d_normal(0, _rnd() / ((double)RAND_MAX + 1.0));
#pragma omp parallel for schedule(dynamic) num_threads(_params.nt)

    for (auto c = 0; c < k; c++) {
        proba_split[c] = (nassign[c] < 2 ? 0 : nassign[c] * nassign[c] - 1);
    }

    int nreassign = 0;

    for (auto c = 0; c < k; c++) {
        if (nassign[c] == 0) {
            nreassign++;

            auto j = roulette_selection(proba_split);

            memcpy(centroids + c * dim,
                   centroids + j * dim,
                   dim * sizeof(centroids[0]));
            double s = cblas_snrm2(dim, centroids + j * dim, 1) * 0.0000001;

            for (auto& v : vepsilon) {
                v = d_normal(_rnd);
            }

            cblas_sscal(dim, s, vepsilon.data(), 1);
            cblas_saxpy(dim, 1.0, vepsilon.data(), 1, centroids + j * dim, 1);
            cblas_saxpy(dim, -1.0, vepsilon.data(), 1, centroids + c * dim, 1);

            proba_split[j] = 0;
        }
    }

    return nreassign;
}

float
Kmeans::kmeans(uint32_t dim,
               size_t total_cnt,
               size_t k,
               const float* train_dataset,
               float* centroids_out,
               float* dis_out,
               int* assign_out) {
    if (k == 0 || k >= total_cnt) {
        return -1;
    }

    int nt = std::max(_params.nt, 1);

    std::unique_ptr<float[]> centroids(new float[k * dim]);

    std::unique_ptr<float[]> dis(new float[total_cnt]);
    std::unique_ptr<int[]> assign(new int[total_cnt]);
    std::unique_ptr<int[]> nassign(new int[k]);

    double qerr = std::numeric_limits<double>::max();
    double qerr_best = std::numeric_limits<double>::max();

    std::vector<size_t> selected(k);

    int core_ret = 0;

    for (auto run = 0; run < _params.redo; run++) {
        if (_params.init_type == KMeansCenterInitType::KMEANS_PLUS_PLUS) {
            //数据集太大时候，缩小范围，待开发
            uint32_t nsubset =
                (total_cnt > k * 8 && total_cnt > 8 * 1024) ? k * 8 : total_cnt;
            kmeanspp_init_center(nsubset, k, dim, train_dataset, selected);
        } else {
            random_init_center(total_cnt, k, dim, train_dataset, selected);
        }

        for (auto i = 0; i < k; i++) {
            memcpy(centroids.get() + i * dim,
                   train_dataset + selected[i] * dim,
                   dim * sizeof(centroids[0]));
        }

        core_ret = kmeans_core(dim,
                               total_cnt,
                               k,
                               _params.niter,
                               nt,
                               centroids.get(),
                               train_dataset,
                               assign.get(),
                               nassign.get(),
                               dis.get(),
                               &qerr);

        if (core_ret < 0) {
            return -1;
            break;
        }

        if (qerr < qerr_best) {
            qerr_best = qerr;

            if (centroids_out != nullptr) {
                memcpy(centroids_out,
                       centroids.get(),
                       k * dim * sizeof(*centroids.get()));
            }

            if (dis_out != nullptr) {
                memcpy(dis_out, dis.get(), total_cnt * sizeof(*dis.get()));
            }

            if (assign_out != nullptr) {
                memcpy(assign_out,
                       assign.get(),
                       total_cnt * sizeof(*assign.get()));
            }
        }
    }

    return qerr_best / total_cnt;
}

int
nearest_center(uint32_t dim,
               const float* centroids,
               const size_t centroid_cnt,
               const float* train_dataset,
               const size_t point_cnt,
               int* assign,
               float* dis) {
    std::vector<float> points_norm(point_cnt);
    std::vector<float> centroids_norm(centroid_cnt);
    int nt = std::thread::hardware_concurrency();

#pragma omp parallel for schedule(dynamic) num_threads(nt)

    for (size_t j = 0; j < point_cnt; j++) {
        points_norm[j] = cblas_sdot(
            dim, train_dataset + j * dim, 1, train_dataset + j * dim, 1);
    }

#pragma omp parallel for schedule(dynamic) num_threads(nt)

    for (size_t j = 0; j < centroid_cnt; j++) {
        centroids_norm[j] =
            cblas_sdot(dim, centroids + j * dim, 1, centroids + j * dim, 1);
    }

#pragma omp parallel for schedule(dynamic) num_threads(nt)

    for (size_t j = 0; j < point_cnt; j++) {
        std::pair<float, uint32_t> min_centroid = {
            std::numeric_limits<float>::max(), 0};

        for (size_t c = 0; c < centroid_cnt; c++) {
            float cur_dist = points_norm[j] + centroids_norm[c] -
                             2.0 * cblas_sdot(dim,
                                              train_dataset + j * dim,
                                              1,
                                              centroids + c * dim,
                                              1);

            if (cur_dist < min_centroid.first) {
                min_centroid = {cur_dist, c};
            }
        }

        assign[j] = min_centroid.second;
        dis[j] = min_centroid.first;
    }

    return 0;
}

int
Kmeans::kmeans_core(uint32_t d,
                    size_t n,
                    size_t k,
                    int niter,
                    int nt,
                    float* centroids,
                    const float* v,
                    int* assign,
                    int* nassign,
                    float* dis,
                    double* qerr_out) {
    double qerr = std::numeric_limits<double>::max();
    double qerr_old = std::numeric_limits<double>::max();

    int tot_nreassign = 0;

    auto start_timer = std::chrono::steady_clock::now();
    for (auto iter = 1; iter <= niter; iter++) {
        printf("kmeans::iter : [%d / %d]\n", iter, niter);
        nearest_center(d, centroids, k, v, n, assign, dis);
        memset(nassign, 0, k * sizeof(int));
        {
            memset(centroids, 0, sizeof(centroids[0] * k * d));

            for (auto i = 0; i < n; i++) {
                if (assign[i] < 0 || assign[i] >= k) {
                    return -1;
                }

                nassign[assign[i]]++;
                cblas_saxpy(d, 1.0, v + i * d, 1, centroids + assign[i] * d, 1);
            }

            for (auto i = 0; i < k; i++) {
                cblas_sscal(d, 1.0 / nassign[i], centroids + i * d, 1);
            }
        }

        auto nreassign =
            kmeans_reassign_empty(d, n, k, centroids, assign, nassign);

        tot_nreassign += nreassign;

        if (tot_nreassign > n / 100 && tot_nreassign > 1000) {
            return -1;
        }

        qerr_old = qerr;
        qerr = 0;

        for (auto i = 0; i < n; i++) {
            qerr += dis[i];
        }

        if (std::fabs(qerr_old - qerr) < 1e-6 && nreassign == 0) {
            break;
        }
    }
    auto end_timer = std::chrono::steady_clock::now();
    auto duration = 1.0 *
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_timer - start_timer)
                        .count() /
                    1000.0;
    { std::cout << "kmeans's duration: " << duration << " seconds\n"; }
    *qerr_out = qerr;
    return 0;
}

};  // namespace milvus::puck