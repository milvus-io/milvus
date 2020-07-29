/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/RHNSW.h>

#include <string>

#include <faiss/impl/AuxIndexStructures.h>

namespace faiss {


/**************************************************************
 * hnsw structure implementation
 **************************************************************/

int* RHNSW::get_neighbor_link(idx_t no, int layer_no) const
{
  return layer_no ? (int*)(level0_links + no * level0_link_size) : (int*)(linkLists[no] + (layer_no - 1) * link_size);
}

unsigned short int RHNSW::get_neighbors_num(int *p) const {
  return *((unsigned short int*)p);
}

void RHNSW::set_neighbors_num(int *p, unsigned short int num) const {
  *((unsigned short int*)(p)) = *((unsigned short int *)(&num));
}

RHNSW::RHNSW(int M) : M(M), rng(12345) {
  max_level = -1;
  entry_point = -1;
  efSearch = 16;
  efConstruction = 40;
  upper_beam = 1;
  level0_link_size = sizeof(int) * ((M << 1) | 1);
  link_size = sizeof(int) * (M + 1);
  level0_links = nullptr;
  linkLists = nullptr;
}

RHNSW::~RHNSW() {
  free(level0_links);
  for (auto i = 0; i < levels.size(); ++ i) {
    if (levels[i])
      free(linkLists[i]);
  }
  free(linkLists);
}

int RHNSW::random_level(double arg)
{
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    double r = -log(distribution(level_generator)) * arg;
  return (int)r;
}

void RHNSW::reset() {
  max_level = -1;
  entry_point = -1;
  levels.clear();
  free(level0_links);
  for (auto i = 0; i < levels.size(); ++ i) {
    if (levels[i])
      free(linkLists[i]);
  }
  free(linkLists);
  level0_links = nullptr;
  linkLists = nullptr;
}


/*
void hnsw::print_neighbor_stats(int level) const
{
  FAISS_THROW_IF_NOT (level < cum_nneighbor_per_level.size());
  printf("stats on level %d, max %d neighbors per vertex:\n",
         level, nb_neighbors(level));
  size_t tot_neigh = 0, tot_common = 0, tot_reciprocal = 0, n_node = 0;
#pragma omp parallel for reduction(+: tot_neigh) reduction(+: tot_common) \
  reduction(+: tot_reciprocal) reduction(+: n_node)
  for (int i = 0; i < levels.size(); i++) {
    if (levels[i] > level) {
      n_node++;
      size_t begin, end;
      neighbor_range(i, level, &begin, &end);
      std::unordered_set<int> neighset;
      for (size_t j = begin; j < end; j++) {
        if (neighbors [j] < 0) break;
        neighset.insert(neighbors[j]);
      }
      int n_neigh = neighset.size();
      int n_common = 0;
      int n_reciprocal = 0;
      for (size_t j = begin; j < end; j++) {
        storage_idx_t i2 = neighbors[j];
        if (i2 < 0) break;
        FAISS_ASSERT(i2 != i);
        size_t begin2, end2;
        neighbor_range(i2, level, &begin2, &end2);
        for (size_t j2 = begin2; j2 < end2; j2++) {
          storage_idx_t i3 = neighbors[j2];
          if (i3 < 0) break;
          if (i3 == i) {
            n_reciprocal++;
            continue;
          }
          if (neighset.count(i3)) {
            neighset.erase(i3);
            n_common++;
          }
        }
      }
      tot_neigh += n_neigh;
      tot_common += n_common;
      tot_reciprocal += n_reciprocal;
    }
  }
  float normalizer = n_node;
  printf("   nb of nodes at that level %ld\n", n_node);
  printf("   neighbors per node: %.2f (%ld)\n",
         tot_neigh / normalizer, tot_neigh);
  printf("   nb of reciprocal neighbors: %.2f\n", tot_reciprocal / normalizer);
  printf("   nb of neighbors that are also neighbor-of-neighbors: %.2f (%ld)\n",
         tot_common / normalizer, tot_common);
}
*/

int RHNSW::prepare_level_tab(size_t n, bool preset_levels)
{
//  size_t n0 = offsets.size() - 1;
  size_t n0 = levels.size();

  if (preset_levels) {
    FAISS_ASSERT (n0 + n == levels.size());
  } else {
    FAISS_ASSERT (n0 == levels.size());
    double random_level_arg = 1 / log(double(M));
    levels.resize(n0 + n);
    for (int i = 0; i < n; i++) {
      int pt_level = random_level(random_level_arg);
      levels.push_back(pt_level);
    }
  }

  char *level0_links_new = (char*)malloc((n0 + n) * level0_link_size);
  if (level0_links_new == nullptr) {
    throw std::runtime_error("No enough memory 4 level0_links!");
  }
  printf("level0_links_new addr: %p has malloc %d bytes space, level0_link_size = %d\n", level0_links_new, level0_link_size * (n0 + n), level0_link_size);
  memcpy(level0_links_new, level0_links, n0 * level0_link_size);
  free(level0_links);
  level0_links = level0_links_new;

  char **linkLists_new = (char **)malloc(sizeof(void*) * (n0 + n));
  if (linkLists_new == nullptr) {
    throw std::runtime_error("No enough memory 4 level0_links_new!");
  }
  printf("linkLists_new addr: %p has malloc %d bytes space, link_size = %d\n", linkLists_new, sizeof(void*) * (n0 + n), link_size);
  memcpy(linkLists_new, linkLists, n0 * sizeof(void*));
  free(linkLists);
  linkLists = linkLists_new;

  int max_level = 0;
  int debug_space = 0;
  for (int i = 0; i < n; i++) {
    int pt_level = levels[i + n0];
    if (pt_level > max_level) max_level = pt_level;
    if (pt_level) {
      linkLists[n0 + i] = (char*) malloc(link_size * pt_level + 1);
      if (linkLists[n0 + i] == nullptr) {
        throw std::runtime_error("No enough memory 4 linkLists!");
      }
      memset(linkLists[n0 + i], 0, link_size * pt_level + 1);
    }
  }

  return max_level;
}


/** Enumerate vertices from farthest to nearest from query, keep a
 * neighbor only if there is no previous neighbor that is closer to
 * that vertex than the query.
 */
void RHNSW::shrink_neighbor_list(
  DistanceComputer& qdis,
  std::priority_queue<NodeDistFarther>& input,
  std::vector<NodeDistFarther>& output,
  int max_size)
{
  while (input.size() > 0) {
    NodeDistFarther v1 = input.top();
    input.pop();
    float dist_v1_q = v1.d;

    bool good = true;
    for (NodeDistFarther v2 : output) {
      float dist_v1_v2 = qdis.symmetric_dis(v2.id, v1.id);

      if (dist_v1_v2 < dist_v1_q) {
        good = false;
        break;
      }
    }

    if (good) {
      output.push_back(v1);
      if (output.size() >= max_size) {
        return;
      }
    }
  }
}


namespace {


using storage_idx_t = RHNSW::storage_idx_t;
using NodeDistCloser = RHNSW::NodeDistCloser;
using NodeDistFarther = RHNSW::NodeDistFarther;


/**************************************************************
 * Addition subroutines
 **************************************************************/


/// remove neighbors from the list to make it smaller than max_size
void shrink_neighbor_list(
  DistanceComputer& qdis,
  std::priority_queue<NodeDistCloser>& resultSet1,
  int max_size)
{
    if (resultSet1.size() < max_size) {
        return;
    }
    std::priority_queue<NodeDistFarther> resultSet;
    std::vector<NodeDistFarther> returnlist;

    while (resultSet1.size() > 0) {
        resultSet.emplace(resultSet1.top().d, resultSet1.top().id);
        resultSet1.pop();
    }

    RHNSW::shrink_neighbor_list(qdis, resultSet, returnlist, max_size);

    for (NodeDistFarther curen2 : returnlist) {
        resultSet1.emplace(curen2.d, curen2.id);
    }

}


/// add a link between two elements, possibly shrinking the list
/// of links to make room for it.
void add_link(RHNSW& hnsw,
              DistanceComputer& qdis,
              storage_idx_t src, storage_idx_t dest,
              int level)
{
  int curr_M = level ? hnsw.M : hnsw.M << 1;
  int *src_level_link = hnsw.get_neighbor_link(src, level);
  int *p_neighbors = src_level_link + 1;
  auto neighbor_num = hnsw.get_neighbors_num(src_level_link);
  if (neighbor_num < curr_M) {
    p_neighbors[++ neighbor_num] = dest;
    hnsw.set_neighbors_num(src_level_link, neighbor_num);
    return;
  }
  // otherwise we let them fight out which to keep

  // copy to resultSet...
  std::priority_queue<NodeDistCloser> resultSet;
  resultSet.emplace(qdis.symmetric_dis(src, dest), dest);
  for (auto i = 0; i < neighbor_num; ++ i) {
    resultSet.emplace(qdis.symmetric_dis(src, p_neighbors[i]), p_neighbors[i]);
  }

  shrink_neighbor_list(qdis, resultSet, neighbor_num);

  // ...and back
  neighbor_num = 0;
  while (resultSet.size()) {
    p_neighbors[neighbor_num ++] = resultSet.top().id;
    resultSet.pop();
  }
  hnsw.set_neighbors_num(src_level_link, neighbor_num);
}

/// search neighbors on a single level, starting from an entry point
void search_neighbors_to_add(
  RHNSW& hnsw,
  DistanceComputer& qdis,
  std::priority_queue<NodeDistCloser>& results,
  int entry_point,
  float d_entry_point,
  int level,
  VisitedTable &vt)
{
  // top is nearest candidate
  std::priority_queue<NodeDistFarther> candidates;

  NodeDistFarther ev(d_entry_point, entry_point);
  candidates.push(ev);
  results.emplace(d_entry_point, entry_point);
  vt.set(entry_point);

  while (!candidates.empty()) {
    // get nearest
    const NodeDistFarther &currEv = candidates.top();

    if (currEv.d > results.top().d) {
      break;
    }
    int currNode = currEv.id;
    candidates.pop();

    // loop over neighbors
    int *cur_links = hnsw.get_neighbor_link(currNode, level);
    int *cur_neighbors = cur_links + 1;
    auto cur_neighbor_num = hnsw.get_neighbors_num(cur_links);
    for(auto i = 0; i < cur_neighbor_num; ++ i) {
      storage_idx_t nodeId = cur_neighbors[i];
      if (nodeId < 0) break;
      if (vt.get(nodeId)) continue;
      vt.set(nodeId);

      float dis = qdis(nodeId);
      NodeDistFarther evE1(dis, nodeId);

      if (results.size() < hnsw.efConstruction ||
          results.top().d > dis) {

        results.emplace(dis, nodeId);
        candidates.emplace(dis, nodeId);
        if (results.size() > hnsw.efConstruction) {
          results.pop();
        }
      }
    }
  }
  vt.advance();
}


/**************************************************************
 * Searching subroutines
 **************************************************************/

/// greedily update a nearest vector at a given level
void greedy_update_nearest(const RHNSW& hnsw,
                           DistanceComputer& qdis,
                           int level,
                           storage_idx_t& nearest,
                           float& d_nearest)
{
  for(;;) {
    storage_idx_t prev_nearest = nearest;

    int *cur_links = hnsw.get_neighbor_link(nearest, level);
    int *cur_neighbors = cur_links + 1;
    auto cur_neighbor_num = hnsw.get_neighbors_num(cur_links);
    for(auto i = 0; i < cur_neighbor_num; ++ i) {
      storage_idx_t v = cur_neighbors[i];
      if (v < 0) break;
      float dis = qdis(v);
      if (dis < d_nearest) {
        nearest = v;
        d_nearest = dis;
      }
    }
    if (nearest == prev_nearest) {
      return;
    }
  }
}


}  // namespace


/// Finds neighbors and builds links with them, starting from an entry
/// point. The own neighbor list is assumed to be locked.
void RHNSW::add_links_starting_from(DistanceComputer& ptdis,
                                   storage_idx_t pt_id,
                                   storage_idx_t nearest,
                                   float d_nearest,
                                   int level,
                                   omp_lock_t *locks,
                                   VisitedTable &vt)
{
  std::priority_queue<NodeDistCloser> link_targets;

  search_neighbors_to_add(*this, ptdis, link_targets, nearest, d_nearest,
                          level, vt);

  // but we can afford only this many neighbors
  int curr_M = level ? M : M << 1;

  ::faiss::shrink_neighbor_list(ptdis, link_targets, curr_M);

  while (!link_targets.empty()) {
    int other_id = link_targets.top().id;

    omp_set_lock(&locks[other_id]);
    add_link(*this, ptdis, other_id, pt_id, level);
    omp_unset_lock(&locks[other_id]);

    add_link(*this, ptdis, pt_id, other_id, level);

    link_targets.pop();
  }
}


/**************************************************************
 * Building, parallel
 **************************************************************/

void RHNSW::add_with_locks(DistanceComputer& ptdis, int pt_level, int pt_id,
                          std::vector<omp_lock_t>& locks,
                          VisitedTable& vt)
{
  //  greedy search on upper levels

  storage_idx_t nearest;
#pragma omp critical
  {
    nearest = entry_point;

    if (nearest == -1) {
      max_level = pt_level;
      entry_point = pt_id;
    }
  }

  if (nearest < 0) {
    return;
  }

  omp_set_lock(&locks[pt_id]);

  int level = max_level; // level at which we start adding neighbors
  float d_nearest = ptdis(nearest);

  for(; level > pt_level; level--) {
    greedy_update_nearest(*this, ptdis, level, nearest, d_nearest);
  }

  for(; level >= 0; level--) {
    add_links_starting_from(ptdis, pt_id, nearest, d_nearest,
                            level, locks.data(), vt);
  }

  omp_unset_lock(&locks[pt_id]);

  if (pt_level > max_level) {
    max_level = pt_level;
    entry_point = pt_id;
  }
}


/** Do a BFS on the candidates list */

int RHNSW::search_from_candidates(
  DistanceComputer& qdis, int k,
  idx_t *I, float *D,
  MinimaxHeap& candidates,
  VisitedTable& vt,
  int level, int nres_in) const
{
  int nres = nres_in;
  int ndis = 0;
  for (int i = 0; i < candidates.size(); i++) {
    idx_t v1 = candidates.ids[i];
    float d = candidates.dis[i];
    FAISS_ASSERT(v1 >= 0);
    if (nres < k) {
      faiss::maxheap_push(++nres, D, I, d, v1);
    } else if (d < D[0]) {
      faiss::maxheap_pop(nres--, D, I);
      faiss::maxheap_push(++nres, D, I, d, v1);
    }
    vt.set(v1);
  }

  bool do_dis_check = check_relative_distance;
  int nstep = 0;

  while (candidates.size() > 0) {
    float d0 = 0;
    int v0 = candidates.pop_min(&d0);

    if (do_dis_check) {
      // tricky stopping condition: there are more that ef
      // distances that are processed already that are smaller
      // than d0

      int n_dis_below = candidates.count_below(d0);
      if(n_dis_below >= efSearch) {
        break;
      }
    }

    int *cur_links = get_neighbor_link(v0, level);
    int *cur_neighbors = cur_links + 1;
    auto cur_neighbor_num = get_neighbors_num(cur_links);

    for (auto j = 0; j < cur_neighbor_num; ++ j) {
      int v1 = cur_neighbors[j];
      if (v1 < 0) break;
      if (vt.get(v1)) {
        continue;
      }
      vt.set(v1);
      ndis++;
      float d = qdis(v1);
      if (nres < k) {
        faiss::maxheap_push(++nres, D, I, d, v1);
      } else if (d < D[0]) {
        faiss::maxheap_pop(nres--, D, I);
        faiss::maxheap_push(++nres, D, I, d, v1);
      }
      candidates.push(v1, d);
    }

    nstep++;
    if (!do_dis_check && nstep > efSearch) {
      break;
    }
  }

  if (level == 0) {
#pragma omp critical
    {
      rhnsw_stats.n1 ++;
      if (candidates.size() == 0) {
        rhnsw_stats.n2 ++;
      }
      rhnsw_stats.n3 += ndis;
    }
  }

  return nres;
}


/**************************************************************
 * Searching
 **************************************************************/

std::priority_queue<RHNSW::Node> RHNSW::search_from_candidate_unbounded(
  const Node& node,
  DistanceComputer& qdis,
  int ef,
  VisitedTable *vt) const
{
  int ndis = 0;
  std::priority_queue<Node> top_candidates;
  std::priority_queue<Node, std::vector<Node>, std::greater<Node>> candidates;

  top_candidates.push(node);
  candidates.push(node);

  vt->set(node.second);

  while (!candidates.empty()) {
    float d0;
    storage_idx_t v0;
    std::tie(d0, v0) = candidates.top();

    if (d0 > top_candidates.top().first) {
      break;
    }

    candidates.pop();

    int *cur_links = get_neighbor_link(v0, 0);
    int *cur_neighbors = cur_links + 1;
    auto cur_neighbor_num = get_neighbors_num(cur_links);

    for (auto j = 0; j < cur_neighbor_num; ++ j) {
      int v1 = cur_neighbors[j];

      if (v1 < 0) {
        break;
      }
      if (vt->get(v1)) {
        continue;
      }

      vt->set(v1);

      float d1 = qdis(v1);
      ++ndis;

      if (top_candidates.top().first > d1 || top_candidates.size() < ef) {
        candidates.emplace(d1, v1);
        top_candidates.emplace(d1, v1);

        if (top_candidates.size() > ef) {
          top_candidates.pop();
        }
      }
    }
  }

#pragma omp critical
  {
    ++rhnsw_stats.n1;
    if (candidates.size() == 0) {
      ++rhnsw_stats.n2;
    }
    rhnsw_stats.n3 += ndis;
  }

  return top_candidates;
}

void RHNSW::search(DistanceComputer& qdis, int k,
                  idx_t *I, float *D,
                  VisitedTable& vt) const
{
  if (upper_beam == 1) {

    //  greedy search on upper levels
    storage_idx_t nearest = entry_point;
    float d_nearest = qdis(nearest);

    for(int level = max_level; level >= 1; level--) {
      greedy_update_nearest(*this, qdis, level, nearest, d_nearest);
    }

    int ef = std::max(efSearch, k);
    if (search_bounded_queue) {
      MinimaxHeap candidates(ef);

      candidates.push(nearest, d_nearest);

      search_from_candidates(qdis, k, I, D, candidates, vt, 0);
    } else {
      std::priority_queue<Node> top_candidates =
        search_from_candidate_unbounded(Node(d_nearest, nearest),
                                        qdis, ef, &vt);

      while (top_candidates.size() > k) {
        top_candidates.pop();
      }

      int nres = 0;
      while (!top_candidates.empty()) {
        float d;
        storage_idx_t label;
        std::tie(d, label) = top_candidates.top();
        faiss::maxheap_push(++nres, D, I, d, label);
        top_candidates.pop();
      }
    }

    vt.advance();

  } else {
    int candidates_size = upper_beam;
    MinimaxHeap candidates(candidates_size);

    std::vector<idx_t> I_to_next(candidates_size);
    std::vector<float> D_to_next(candidates_size);

    int nres = 1;
    I_to_next[0] = entry_point;
    D_to_next[0] = qdis(entry_point);

    for(int level = max_level; level >= 0; level--) {

      // copy I, D -> candidates

      candidates.clear();

      for (int i = 0; i < nres; i++) {
        candidates.push(I_to_next[i], D_to_next[i]);
      }

      if (level == 0) {
        nres = search_from_candidates(qdis, k, I, D, candidates, vt, 0);
      } else  {
        nres = search_from_candidates(
          qdis, candidates_size,
          I_to_next.data(), D_to_next.data(),
          candidates, vt, level
        );
      }
      vt.advance();
    }
  }
}


void RHNSW::MinimaxHeap::push(storage_idx_t i, float v) {
  if (k == n) {
    if (v >= dis[0]) return;
    faiss::heap_pop<HC> (k--, dis.data(), ids.data());
    --nvalid;
  }
  faiss::heap_push<HC> (++k, dis.data(), ids.data(), v, i);
  ++nvalid;
}

float RHNSW::MinimaxHeap::max() const {
  return dis[0];
}

int RHNSW::MinimaxHeap::size() const {
  return nvalid;
}

void RHNSW::MinimaxHeap::clear() {
  nvalid = k = 0;
}

int RHNSW::MinimaxHeap::pop_min(float *vmin_out) {
  assert(k > 0);
  // returns min. This is an O(n) operation
  int i = k - 1;
  while (i >= 0) {
    if (ids[i] != -1) break;
    i--;
  }
  if (i == -1) return -1;
  int imin = i;
  float vmin = dis[i];
  i--;
  while(i >= 0) {
    if (ids[i] != -1 && dis[i] < vmin) {
      vmin = dis[i];
      imin = i;
    }
    i--;
  }
  if (vmin_out) *vmin_out = vmin;
  int ret = ids[imin];
  ids[imin] = -1;
  --nvalid;

  return ret;
}

int RHNSW::MinimaxHeap::count_below(float thresh) {
  int n_below = 0;
  for(int i = 0; i < k; i++) {
    if (dis[i] < thresh) {
      n_below++;
    }
  }

  return n_below;
}


}  // namespace faiss
