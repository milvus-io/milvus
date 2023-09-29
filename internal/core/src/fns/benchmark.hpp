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

#ifndef FNS_H
#define FNS_H

#include <memory>
#include <iostream>
#include <unordered_set>
#include <algorithm>
#include <queue>


#include "kmeans.h"
#include "kgraph.h"

// #include "IO.hpp"
#include "basicDistance.h"
#include "visitedList.hpp"


using namespace milvus;

using PriorQ = std::priority_queue<std::pair<distance_t, idx_t>, std::vector<std::pair<distance_t, idx_t>>>;

struct BuildParam{
    size_t kms_cluster_num = 1024; 
    size_t filter_pos = 4;
    size_t filter_density = 0;
    size_t filter_ctrl_size = 64;
    size_t filter_max_edges = 2048;
};

template <typename T> 
class FNS
{
private:
    const T * Data_;
    float * KmeansCentroids_ = nullptr;
    size_t KmeansClusterNum_;
    int * KmeansLabels_ = nullptr;
    graph_t KmeansLabelGraph_;
    std::vector<idx_t> getDensity_;
    graph_t kgraph;
    const size_t dataSz_;
    const size_t dataDim_;
    const BuildParam & BP_;
    std::vector<idx_t> fullLowDenPoints_;
    size_t searchK_ = 0;
    VisitedListPool *visited_list_pool_;

public:

    FNS(const T * base, size_t data_size, size_t data_dim,  BuildParam & bp):
        Data_(base),
        dataSz_(data_size),
        dataDim_(data_dim),
        BP_(bp)
    {
        visited_list_pool_ = new VisitedListPool(1, dataSz_);
    }


    ~FNS(){
        delete visited_list_pool_;
        delete [] KmeansCentroids_;
        delete [] KmeansLabels_;
    }

    void build(std::string centroids_path, std::string kms_labels_path, std::string knng_path){
 

        runKmeans();
        runNNDescent();
        updateDensity();

        IO::saveBinVecPtr(centroids_path, KmeansCentroids_, BP_.kms_cluster_num, dataDim_);
        std::vector<std::vector<int>> kms_labels(dataSz_, vector<int>(1));
        for (size_t i = 0; i < dataSz_; ++i){
            kms_labels[i][0] = KmeansLabels_[i];
        }
        IO::saveBinVec(kms_labels_path, kms_labels); 

        IO::saveBinVec(knng_path, kgraph);

        graph_t(BP_.kms_cluster_num).swap(KmeansLabelGraph_);
        for (size_t idx = 0; idx < dataSz_; ++idx){
            auto lb = KmeansLabels_[idx];
            KmeansLabelGraph_[lb].emplace_back(idx);
        }
    }




    void load(std::string centroids_path, std::string kms_labels_path, std::string knng_path){
        auto centroids = IO::LoadBinVec<T>(centroids_path);
        auto kms_labels = IO::LoadBinVec<int>(kms_labels_path);
        kgraph = IO::LoadBinVec<idx_t>(knng_path);
        KmeansCentroids_    = new float[BP_.kms_cluster_num * dataDim_]; 
        KmeansLabels_       = new int[dataSz_];
        for (size_t i = 0; i < centroids.size(); ++i){
            memcpy(KmeansCentroids_ + i * dataDim_, centroids[i].data(), dataDim_ * sizeof(T));
        }

        for (size_t i = 0; i < dataSz_; ++i){
            KmeansLabels_[i] = static_cast<int>(kms_labels[i][0]);
        }

        graph_t(BP_.kms_cluster_num).swap(KmeansLabelGraph_);
        for (size_t idx = 0; idx < dataSz_; ++idx){
            auto lb = KmeansLabels_[idx];
            KmeansLabelGraph_[lb].emplace_back(idx);
        }
    
        updateDensity();
    }

    void setSearchK(size_t k){
        searchK_ = k;
    }



    void runNNDescent(){

        IndexParams params;

        params.L = 100; //// K of k-NN Graph  unsigned K = params.L;
        params.R = 16; //32; /// smpN
        params.S = 16; //32;//// smpN
        params.K = 10; //// search K && recall K 
        params.iterations = 9; //iterN 
        params.controls = 0;
        params.reverse = 0;


        // load base data;
        Matrix<T> base_data;
        base_data.load(Data_, dataSz_, dataDim_); 
        MatrixOracle<T, metric::l2sqr> oracle(base_data);

        KGraphConstructor * kg = new KGraphConstructor(oracle, params);


        kg->build_index();

        auto knn_pool = kg->nhoods;

        kgraph.resize(dataSz_); 

        for (size_t i = 0; i < dataSz_; ++i){
            auto & kg_nbhood = kgraph[i]; 
            auto const &pool = knn_pool[i].pool;
            for (auto & elem : pool){
                kg_nbhood.emplace_back(elem.id);
            }
        }

        delete kg;
    }



    void runKmeans(){

        KmeansCentroids_    = new float[BP_.kms_cluster_num * dataDim_]; 
        KmeansLabels_       = new int[dataSz_];

        puck::Kmeans * kms = new puck::Kmeans(true);
        kms->kmeans(dataDim_, dataSz_, BP_.kms_cluster_num, Data_, KmeansCentroids_, nullptr, KmeansLabels_);
        delete kms; 

    }

    inline void updateDensity(){
        vector<idx_t>(dataSz_, 0).swap(getDensity_);
    // #pragma omp parallel for ///TODO: add mutex 
        for (size_t i = 0; i < dataSz_; ++i){
            for (size_t j = 0; j < BP_.filter_pos && j < (size_t)kgraph[0].size(); ++j){
                auto nb = kgraph[i][j];
                ++getDensity_[nb];
            }
        }
    }




    inline PriorQ searchFNS(T * query){
        VisitedList *vl = visited_list_pool_->getFreeVisitedList();
        vl_type *visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        std::vector<std::pair<distance_t, idx_t>> pairs_center(BP_.kms_cluster_num);
        for (size_t i = 0; i < BP_.kms_cluster_num; ++i){
            auto label = i; 
            auto dst = basicDistance::basicL2(query, KmeansCentroids_ + i * dataDim_, dataDim_);
            pairs_center[i].first = -dst;
            pairs_center[i].second = label;
        }
        sort(pairs_center.begin(), pairs_center.end());
        vector<idx_t> low_density_points;

        for (auto & p : pairs_center){
            auto lb = p.second;
            auto & points = KmeansLabelGraph_[lb];
            for (auto p : points){
                if (getDensity_[p] <= BP_.filter_density ){
                    low_density_points.emplace_back(p);
                    if (low_density_points.size() > BP_.filter_max_edges){break;}
                }
            }
            if (low_density_points.size() > BP_.filter_max_edges){break;}
        }

        //// TODO: using prefetch to boost this distance calculation. 
        std::vector<std::pair<distance_t, idx_t>> pairs(low_density_points.size());
        for (auto i = 0; i < low_density_points.size(); ++i){
            auto nb = low_density_points[i];
            auto dst = basicDistance::basicL2(query, Data_ + nb * dataDim_, dataDim_);
            pairs[i].first  = -dst;
            pairs[i].second = nb;
        }
        sort(pairs.begin(), pairs.end());

        PriorQ top_results;


        for (size_t i = 0; i < searchK_ ; i++){
            auto & p = pairs[i];
            top_results.push(p);
            visited_array[p.second] = visited_array_tag;
        }


        for (size_t i = 0; i < BP_.filter_ctrl_size ; i++)
        {
            auto nb = pairs[i].second;
            for (size_t j = 0; j < kgraph[0].size(); ++j)
            {   
                size_t nn = (size_t)kgraph[nb][j];
                if (visited_array[nn] == visited_array_tag)
                {
                    continue;
                }
                visited_array[nn] = visited_array_tag;
                auto dst = basicDistance::basicL2(query, Data_ + nn * dataDim_, dataDim_);
                if (dst > -top_results.top().first){
                    top_results.emplace(-dst, nn);
                }
            }
            while (top_results.size() > searchK_){
                top_results.pop();
            }
        }

        visited_list_pool_->releaseVisitedList(vl);

        return top_results;
    }




    double evaluateRatio(std::vector<std::vector<unsigned>>  & gt, graph_t & full_fns, size_t query_size, T * query, size_t checkK=100){
        double avg_ratio = 0;

        for (size_t index = 0; index < query_size; ++index){
            auto q = query + index * dataDim_;
            auto & gt_list = gt[index];
            auto & nn_list = full_fns[index];
            double overall_ratio = 0;
            checkK = std::min(checkK, gt_list.size());
            for (int iter = 0; iter < checkK; ++iter){
                auto idx = nn_list[iter];
                auto gt_idx = gt_list[iter];
                overall_ratio += basicDistance::basicL2(q, Data_ + gt_idx * dataDim_, dataDim_)
                                / basicDistance::basicL2(q, Data_ + idx * dataDim_, dataDim_);
            }
            avg_ratio += overall_ratio / checkK;
        }
        return avg_ratio / query_size;
    }


    float evaluateRecall(std::vector<std::vector<unsigned>> & gt, graph_t & full_fns, size_t checkK=100){
        size_t hit = 0; 
        size_t checkSz = full_fns.size();
        for (size_t iter = 0; iter < checkSz; ++iter){
            auto & fns = full_fns[iter];
            auto & gt_list = gt[iter];
            for (auto i = 0; i < checkK; ++i){
                auto fn = fns[i];
                for (auto j = 0; j < checkK; ++j){
                    auto nb = gt_list[j];
                    if (fn == nb){
                        ++hit;
                        break;
                    }
                }
            }
        }
        return 1.0 * hit / (checkSz * checkK);
    }
};



#endif