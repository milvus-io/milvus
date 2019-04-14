/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <vector>
#include <unordered_set>
#include <queue>

#include <omp.h>

#include "Index.h"
#include "FaissAssert.h"
#include "utils.h"


namespace faiss {


/** Implementation of the Hierarchical Navigable Small World
 * datastructure.
 *
 * Efficient and robust approximate nearest neighbor search using
 * Hierarchical Navigable Small World graphs
 *
 *  Yu. A. Malkov, D. A. Yashunin, arXiv 2017
 *
 * This implmentation is heavily influenced by the NMSlib
 * implementation by Yury Malkov and Leonid Boystov
 * (https://github.com/searchivarius/nmslib)
 *
 * The HNSW object stores only the neighbor link structure, see
 * IndexHNSW.h for the full index object.
 */


struct VisitedTable;
struct DistanceComputer; // from AuxIndexStructures

struct HNSW {
  /// internal storage of vectors (32 bits: this is expensive)
  typedef int storage_idx_t;

  /// Faiss results are 64-bit
  typedef Index::idx_t idx_t;

  typedef std::pair<float, storage_idx_t> Node;

  /** Heap structure that allows fast
   */
  struct MinimaxHeap {
    int n;
    int k;
    int nvalid;

    std::vector<storage_idx_t> ids;
    std::vector<float> dis;
    typedef faiss::CMax<float, storage_idx_t> HC;

    explicit MinimaxHeap(int n): n(n), k(0), nvalid(0), ids(n), dis(n) {}

    void push(storage_idx_t i, float v);

    float max() const;

    int size() const;

    void clear();

    int pop_min(float *vmin_out = nullptr);

    int count_below(float thresh);
  };


  /// to sort pairs of (id, distance) from nearest to fathest or the reverse
  struct NodeDistCloser {
    float d;
    int id;
    NodeDistCloser(float d, int id): d(d), id(id) {}
    bool operator < (const NodeDistCloser &obj1) const { return d < obj1.d; }
  };

  struct NodeDistFarther {
    float d;
    int id;
    NodeDistFarther(float d, int id): d(d), id(id) {}
    bool operator < (const NodeDistFarther &obj1) const { return d > obj1.d; }
  };


  /// assignment probability to each layer (sum=1)
  std::vector<double> assign_probas;

  /// number of neighbors stored per layer (cumulative), should not
  /// be changed after first add
  std::vector<int> cum_nneighbor_per_level;

  /// level of each vector (base level = 1), size = ntotal
  std::vector<int> levels;

  /// offsets[i] is the offset in the neighbors array where vector i is stored
  /// size ntotal + 1
  std::vector<size_t> offsets;

  /// neighbors[offsets[i]:offsets[i+1]] is the list of neighbors of vector i
  /// for all levels. this is where all storage goes.
  std::vector<storage_idx_t> neighbors;

  /// entry point in the search structure (one of the points with maximum level
  storage_idx_t entry_point;

  faiss::RandomGenerator rng;

  /// maximum level
  int max_level;

  /// expansion factor at construction time
  int efConstruction;

  /// expansion factor at search time
  int efSearch;

  /// during search: do we check whether the next best distance is good enough?
  bool check_relative_distance = true;

  /// number of entry points in levels > 0.
  int upper_beam;

  /// use bounded queue during exploration
  bool search_bounded_queue = true;

  // methods that initialize the tree sizes

  /// initialize the assign_probas and cum_nneighbor_per_level to
  /// have 2*M links on level 0 and M links on levels > 0
  void set_default_probas(int M, float levelMult);

  /// set nb of neighbors for this level (before adding anything)
  void set_nb_neighbors(int level_no, int n);

  // methods that access the tree sizes

  /// nb of neighbors for this level
  int nb_neighbors(int layer_no) const;

  /// cumumlative nb up to (and excluding) this level
  int cum_nb_neighbors(int layer_no) const;

  /// range of entries in the neighbors table of vertex no at layer_no
  void neighbor_range(idx_t no, int layer_no,
                      size_t * begin, size_t * end) const;

  /// only mandatory parameter: nb of neighbors
  explicit HNSW(int M = 32);

  /// pick a random level for a new point
  int random_level();

  /// add n random levels to table (for debugging...)
  void fill_with_random_links(size_t n);

  void add_links_starting_from(DistanceComputer& ptdis,
                               storage_idx_t pt_id,
                               storage_idx_t nearest,
                               float d_nearest,
                               int level,
                               omp_lock_t *locks,
                               VisitedTable &vt);


  /** add point pt_id on all levels <= pt_level and build the link
   * structure for them. */
  void add_with_locks(DistanceComputer& ptdis, int pt_level, int pt_id,
                      std::vector<omp_lock_t>& locks,
                      VisitedTable& vt);

  int search_from_candidates(DistanceComputer& qdis, int k,
                             idx_t *I, float *D,
                             MinimaxHeap& candidates,
                             VisitedTable &vt,
                             int level, int nres_in = 0) const;

  std::priority_queue<Node> search_from_candidate_unbounded(
    const Node& node,
    DistanceComputer& qdis,
    int ef,
    VisitedTable *vt
  ) const;

  /// search interface
  void search(DistanceComputer& qdis, int k,
              idx_t *I, float *D,
              VisitedTable& vt) const;

  void reset();

  void clear_neighbor_tables(int level);
  void print_neighbor_stats(int level) const;

  int prepare_level_tab(size_t n, bool preset_levels = false);

  static void shrink_neighbor_list(
    DistanceComputer& qdis,
    std::priority_queue<NodeDistFarther>& input,
    std::vector<NodeDistFarther>& output,
    int max_size);

};


/**************************************************************
 * Auxiliary structures
 **************************************************************/

/// set implementation optimized for fast access.
struct VisitedTable {
  std::vector<uint8_t> visited;
  int visno;

  explicit VisitedTable(int size)
    : visited(size), visno(1) {}

  /// set flog #no to true
  void set(int no) {
    visited[no] = visno;
  }

  /// get flag #no
  bool get(int no) const {
    return visited[no] == visno;
  }

  /// reset all flags to false
  void advance() {
    visno++;
    if (visno == 250) {
      // 250 rather than 255 because sometimes we use visno and visno+1
      memset(visited.data(), 0, sizeof(visited[0]) * visited.size());
      visno = 1;
    }
  }
};


struct HNSWStats {
  size_t n1, n2, n3;
  size_t ndis;
  size_t nreorder;
  bool view;

  HNSWStats() {
    reset();
  }

  void reset() {
    n1 = n2 = n3 = 0;
    ndis = 0;
    nreorder = 0;
    view = false;
  }
};

// global var that collects them all
extern HNSWStats hnsw_stats;


}  // namespace faiss
