// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <boost/dynamic_bitset.hpp>
#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

#include "Distance.h"
#include "Neighbor.h"
#include "knowhere/common/Config.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {
namespace impl {

using node_t = int64_t;

struct BuildParams {
    size_t search_length;
    size_t out_degree;
    size_t candidate_pool_size;
};

struct SearchParams {
    size_t search_length;
    size_t k;
};

using Graph = std::vector<std::vector<node_t>>;

class NsgIndex {
 public:
    enum Metric_Type {
        Metric_Type_L2 = 0,
        Metric_Type_IP,
    };

    size_t dimension;
    size_t ntotal;        // totabl nb of indexed vectors
    int32_t metric_type;  // enum Metric_Type
    Distance* distance_;

    // float* ori_data_;
    int64_t* ids_;
    Graph nsg;   // final graph
    Graph knng;  // reset after build

    node_t navigation_point;  // offset of node in origin data

    bool is_trained = false;

    /*
     * build and search parameter
     */
    size_t search_length;
    size_t candidate_pool_size;  // search deepth in fullset
    size_t out_degree;

 public:
    explicit NsgIndex(const size_t& dimension, const size_t& n, Metric_Type metric);

    NsgIndex() = default;

    virtual ~NsgIndex();

    void
    SetKnnGraph(Graph& knng);

    void
    Build(size_t nb, float* data, const int64_t* ids, const BuildParams& parameters);

    void
    Search(const float* query,
           float* data,
           const unsigned& nq,
           const unsigned& dim,
           const unsigned& k,
           float* dist,
           int64_t* ids,
           SearchParams& params,
           const faiss::BitsetView bitset);

    int64_t
    GetSize();

    // Not support yet.
    // virtual void Add() = 0;
    // virtual void Add_with_ids() = 0;
    // virtual void Delete() = 0;
    // virtual void Delete_with_ids() = 0;
    // virtual void Rebuild(size_t nb,
    //                     const float *data,
    //                     const int64_t *ids,
    //                     const Parameters &parameters) = 0;
    // virtual void Build(size_t nb,
    //                   const float *data,
    //                   const BuildParam &parameters);

 protected:
    void
    InitNavigationPoint(float* data);

    // link specify
    void
    GetNeighbors(const float* query,
                 float* data,
                 std::vector<Neighbor>& resset,
                 std::vector<Neighbor>& fullset,
                 boost::dynamic_bitset<>& has_calculated_dist);

    // FindUnconnectedNode
    void
    GetNeighbors(const float* query, float* data, std::vector<Neighbor>& resset, std::vector<Neighbor>& fullset);

    // navigation-point
    void
    GetNeighbors(
        const float* query, float* data, std::vector<Neighbor>& resset, Graph& graph, SearchParams* param = nullptr);

    // only for search
    // void
    // GetNeighbors(const float* query, node_t* I, float* D, SearchParams* params);

    void
    Link(float* data);

    void
    SyncPrune(float* data,
              size_t q,
              std::vector<Neighbor>& pool,
              boost::dynamic_bitset<>& has_calculated,
              float* cut_graph_dist);

    void
    SelectEdge(float* data,
               unsigned& cursor,
               std::vector<Neighbor>& sort_pool,
               std::vector<Neighbor>& result,
               bool limit = false);

    void
    InterInsert(float* data, unsigned n, std::vector<std::mutex>& mutex_vec, float* dist);

    void
    CheckConnectivity(float* data);

    void
    DFS(size_t root, boost::dynamic_bitset<>& flags, int64_t& count);

    void
    FindUnconnectedNode(float* data, boost::dynamic_bitset<>& flags, int64_t& root);
};

}  // namespace impl
}  // namespace knowhere
}  // namespace milvus
