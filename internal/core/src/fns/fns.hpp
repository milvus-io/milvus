// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef INTERNAL_CORE_SRC_FNS_FNS_HPP_
#define INTERNAL_CORE_SRC_FNS_FNS_HPP_

#include <memory>
#include <iostream>
#include <unordered_set>
#include <algorithm>
#include <queue>
#include <utility>
#include <vector>

#include "kmeans.h"
#include "kgraph.h"
#include "basicDistance.h"
#include "visitedList.hpp"

#include "common/Types.h"

namespace milvus::fns {

using PriorQ = std::priority_queue<std::pair<distance_t, idx_t>,
                                   std::vector<std::pair<distance_t, idx_t>>>;
using NNDIndexParam = milvus::kgraph::IndexParams;
using graph_t = std::vector<std::vector<idx_t>>;

struct BuildParam {
    size_t kms_cluster_num = 1024;
    size_t filter_pos = 4;
    size_t filter_density = 0;
    size_t filter_ctrl_size = 64;
    size_t filter_max_edges = 2048;
};

template <typename T>
class FNS {
 private:
    const T* data_{nullptr};
    float* kmeans_centroids_{nullptr};
    int* kmeans_labels_{nullptr};
    graph_t kmeans_label_graph_;
    std::vector<idx_t> density_;
    graph_t knn_graph_;
    const size_t data_size_{0};
    const size_t data_dim_{0};
    BuildParam build_param_;
    NNDIndexParam nnd_para_;
    size_t search_k_{0};
    VisitedListPool* visited_list_pool_{nullptr};

 public:
    FNS(const T* base, size_t data_size, size_t data_dim)
        : data_(base), data_size_(data_size), data_dim_(data_dim) {
        visited_list_pool_ = new VisitedListPool(1, data_size_);
    }

    ~FNS() {
        delete visited_list_pool_;
        if (kmeans_centroids_)
            delete[] kmeans_centroids_;
        if (kmeans_labels_)
            delete[] kmeans_labels_;
    }

    void
    setUpBuildPara(const BuildParam& bp) {
        build_param_ = bp;
    }
    void
    setUpNNDPara(const NNDIndexParam& nnd_para) {
        nnd_para_ = nnd_para;
    }

    int
    build() {
        auto kms_return_val = runKmeans();
        auto kms_nnd_val = runNNDescent();
        if (kms_return_val == -1 || kms_nnd_val == -1)
            return -1;
        updateDensity();
        graph_t(build_param_.kms_cluster_num).swap(kmeans_label_graph_);
        for (size_t idx = 0; idx < data_size_; ++idx) {
            auto lb = kmeans_labels_[idx];
            kmeans_label_graph_[lb].emplace_back(idx);
        }
        return 0;
    }

    void
    setSearchK(size_t k) {
        search_k_ = k;
    }

    int
    runNNDescent() {
        // load base data;
        Matrix<T> base_data;
        base_data.load(data_, data_size_, data_dim_);
        MatrixOracle<T, metric::l2sqr> oracle(base_data);
        std::unique_ptr<milvus::kgraph::KGraphConstructor> kg(
            new milvus::kgraph::KGraphConstructor(oracle, nnd_para_));
        int nnd_return_val = kg->build_index();
        if (nnd_return_val != 0) {
            return nnd_return_val;
        }
        auto knn_pool = kg->nhoods;
        std::vector<std::vector<idx_t>>(data_size_).swap(knn_graph_);
        for (size_t i = 0; i < data_size_; ++i) {
            auto& kg_nbhood = knn_graph_[i];
            auto const& pool = knn_pool[i].pool;
            for (auto& elem : pool) {
                kg_nbhood.emplace_back(elem.id);
            }
        }
        return 0;
    }

    float
    runKmeans() {
        if (kmeans_centroids_) {
            delete[] kmeans_centroids_;
            kmeans_centroids_ = nullptr;
        }
        kmeans_centroids_ = new float[build_param_.kms_cluster_num * data_dim_];

        if (kmeans_labels_) {
            delete[] kmeans_labels_;
            kmeans_labels_ = nullptr;
        }
        kmeans_labels_ = new int[data_size_];

        std::unique_ptr<puck::Kmeans> kms(new puck::Kmeans(true));
        auto kms_return_val = kms->kmeans(data_dim_,
                                          data_size_,
                                          build_param_.kms_cluster_num,
                                          data_,
                                          kmeans_centroids_,
                                          nullptr,
                                          kmeans_labels_);
        return kms_return_val;
    }

    inline void
    updateDensity() {
        vector<idx_t>(data_size_, 0).swap(density_);
        for (size_t i = 0; i < data_size_; ++i) {
            for (size_t j = 0; j < build_param_.filter_pos &&
                               j < (size_t)knn_graph_[0].size();
                 ++j) {
                auto nb = knn_graph_[i][j];
                ++density_[nb];
            }
        }
    }

    inline PriorQ
    searchFNS(T* query) {
        PriorQ top_results;
        if (search_k_ <= 0 || search_k_ > data_size_ || query == nullptr) {
            return top_results;
        }
        VisitedList* vl = visited_list_pool_->getFreeVisitedList();
        vl_type* visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;
        std::vector<std::pair<distance_t, idx_t>> pairs_center(
            build_param_.kms_cluster_num);
        for (size_t i = 0; i < build_param_.kms_cluster_num; ++i) {
            auto label = i;
            auto dst = milvus::basicDistance::basicL2(
                query, &kmeans_centroids_[i * data_dim_], data_dim_);
            pairs_center[i].first = -dst;
            pairs_center[i].second = label;
        }
        sort(pairs_center.begin(), pairs_center.end());
        vector<idx_t> low_density_points;
        for (auto& p : pairs_center) {
            auto lb = p.second;
            auto& points = kmeans_label_graph_[lb];
            for (auto p : points) {
                if (density_[p] <= build_param_.filter_density) {
                    low_density_points.emplace_back(p);
                    if (low_density_points.size() >
                        build_param_.filter_max_edges) {
                        break;
                    }
                }
            }
            if (low_density_points.size() > build_param_.filter_max_edges) {
                break;
            }
        }
        std::vector<std::pair<distance_t, idx_t>> pairs(
            low_density_points.size());
        for (auto i = 0; i < low_density_points.size(); ++i) {
            auto nb = low_density_points[i];
            auto dst = milvus::basicDistance::basicL2(
                query, data_ + nb * data_dim_, data_dim_);
            pairs[i].first = -dst;
            pairs[i].second = nb;
        }
        sort(pairs.begin(), pairs.end());
        for (size_t i = 0; i < search_k_; i++) {
            auto& p = pairs[i];
            top_results.push(p);
            visited_array[p.second] = visited_array_tag;
        }
        for (size_t i = 0; i < build_param_.filter_ctrl_size; i++) {
            auto nb = pairs[i].second;
            for (size_t j = 0; j < knn_graph_[0].size(); ++j) {
                size_t nn = (size_t)knn_graph_[nb][j];
                if (visited_array[nn] == visited_array_tag) {
                    continue;
                }
                visited_array[nn] = visited_array_tag;
                auto dst = milvus::basicDistance::basicL2(
                    query, data_ + nn * data_dim_, data_dim_);
                if (dst > -top_results.top().first) {
                    top_results.emplace(-dst, nn);
                }
            }
            while (top_results.size() > search_k_) {
                top_results.pop();
            }
        }
        visited_list_pool_->releaseVisitedList(vl);
        return top_results;
    }

    double
    evaluateRatio(std::vector<std::vector<unsigned>>& gt,
                  graph_t& full_fns,
                  size_t query_size,
                  T* query,
                  size_t checkK = 100) {
        double avg_ratio = 0;

        for (size_t index = 0; index < query_size; ++index) {
            auto q = query + index * data_dim_;
            auto& gt_list = gt[index];
            auto& nn_list = full_fns[index];
            double overall_ratio = 0;
            checkK = std::min(checkK, gt_list.size());
            for (int iter = 0; iter < checkK; ++iter) {
                auto idx = nn_list[iter];
                auto gt_idx = gt_list[iter];
                overall_ratio += milvus::basicDistance::basicL2(
                                     q, data_ + gt_idx * data_dim_, data_dim_) /
                                 milvus::basicDistance::basicL2(
                                     q, data_ + idx * data_dim_, data_dim_);
            }
            avg_ratio += overall_ratio / checkK;
        }
        return avg_ratio / query_size;
    }

    float
    evaluateRecall(std::vector<std::vector<unsigned>>& gt,
                   graph_t& full_fns,
                   size_t checkK = 100) {
        size_t hit = 0;
        size_t checkSz = full_fns.size();
        for (size_t iter = 0; iter < checkSz; ++iter) {
            auto& fns = full_fns[iter];
            auto& gt_list = gt[iter];
            for (auto i = 0; i < checkK; ++i) {
                auto fn = fns[i];
                for (auto j = 0; j < checkK; ++j) {
                    auto nb = gt_list[j];
                    if (fn == nb) {
                        ++hit;
                        break;
                    }
                }
            }
        }
        return 1.0 * hit / (checkSz * checkK);
    }
};
}  // namespace milvus::fns

#endif  // INTERNAL_CORE_SRC_FNS_FNS_HPP_
