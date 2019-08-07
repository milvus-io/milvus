////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstddef>
#include <vector>
#include <mutex>

#include <boost/dynamic_bitset.hpp>
#include "neighbor.h"


namespace zilliz {
namespace knowhere {
namespace algo {


using node_t = int64_t;

enum class MetricType {
    METRIC_INNER_PRODUCT = 0,
    METRIC_L2 = 1,
};

struct BuildParams {
    size_t search_length;
    size_t out_degree;
    size_t candidate_pool_size;
};

struct SearchParams {
    size_t search_length;
};

using Graph = std::vector<std::vector<node_t>>;

class NsgIndex {
 public:
    size_t dimension;
    size_t ntotal; // totabl nb of indexed vectors
    MetricType metric_type; // L2 | IP

    float *ori_data_;
    long *ids_; // TODO: support different type
    Graph nsg; // final graph
    Graph knng; // reset after build

    node_t navigation_point; // offset of node in origin data

    bool is_trained = false;

    /*
     * build and search parameter
     */
    size_t search_length;
    size_t candidate_pool_size; // search deepth in fullset
    size_t out_degree;

 public:
    explicit NsgIndex(const size_t &dimension,
                      const size_t &n,
                      MetricType metric = MetricType::METRIC_L2);

    NsgIndex() = default;

    virtual ~NsgIndex();

    void SetKnnGraph(Graph &knng);

    virtual void Build_with_ids(size_t nb,
                                const float *data,
                                const long *ids,
                                const BuildParams &parameters);

    void Search(const float *query,
                const unsigned &nq,
                const unsigned &dim,
                const unsigned &k,
                float *dist,
                long *ids,
                SearchParams &params);

    // Not support yet.
    //virtual void Add() = 0;
    //virtual void Add_with_ids() = 0;
    //virtual void Delete() = 0;
    //virtual void Delete_with_ids() = 0;
    //virtual void Rebuild(size_t nb,
    //                     const float *data,
    //                     const long *ids,
    //                     const Parameters &parameters) = 0;
    //virtual void Build(size_t nb,
    //                   const float *data,
    //                   const BuildParam &parameters);

 protected:
    virtual void InitNavigationPoint();

    // link specify
    void GetNeighbors(const float *query,
                      std::vector<Neighbor> &resset,
                      std::vector<Neighbor> &fullset,
                      boost::dynamic_bitset<> &has_calculated_dist);

    // FindUnconnectedNode
    void GetNeighbors(const float *query,
                      std::vector<Neighbor> &resset,
                      std::vector<Neighbor> &fullset);

    // search and navigation-point
    void GetNeighbors(const float *query,
                      std::vector<Neighbor> &resset,
                      Graph &graph,
                      SearchParams *param = nullptr);

    void Link();

    void SyncPrune(size_t q,
                   std::vector<Neighbor> &pool,
                   boost::dynamic_bitset<> &has_calculated,
                   float *cut_graph_dist
    );

    void SelectEdge(unsigned &cursor,
                    std::vector<Neighbor> &sort_pool,
                    std::vector<Neighbor> &result,
                    bool limit = false);

    void InterInsert(unsigned n, std::vector<std::mutex> &mutex_vec, float *dist);

    void CheckConnectivity();

    void DFS(size_t root, boost::dynamic_bitset<> &flags, int64_t &count);

    void FindUnconnectedNode(boost::dynamic_bitset<> &flags, int64_t &root);

 private:
    void GetKnnGraphFromFile();
};

}
}
}
