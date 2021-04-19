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

RHNSW::RHNSW(int M) : M(M), rng(12345) {
  level_generator.seed(100);
  max_level = -1;
  entry_point = -1;
  efSearch = 16;
  efConstruction = 40;
  upper_beam = 1;
  level0_link_size = sizeof(int) * ((M << 1) | 1);
  link_size = sizeof(int) * (M + 1);
  level0_links = nullptr;
  linkLists = nullptr;
  level_constant = 1 / log(1.0 * M);
  visited_list_pool = nullptr;
}

void RHNSW::init(int ntotal) {
  level_generator.seed(100);
  if (visited_list_pool) delete visited_list_pool;
  visited_list_pool = new VisitedListPool(1, ntotal);
  std::vector<std::mutex>(ntotal).swap(link_list_locks);
}

RHNSW::~RHNSW() {
  free(level0_links);
  for (auto i = 0; i < levels.size(); ++ i) {
    if (levels[i])
      free(linkLists[i]);
  }
  free(linkLists);
  delete visited_list_pool;
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
  level_constant = 1 / log(1.0 * M);
}

int RHNSW::prepare_level_tab(size_t n, bool preset_levels)
{
  size_t n0 = levels.size();

  std::vector<int> level_stats(n);
  if (preset_levels) {
    FAISS_ASSERT (n0 + n == levels.size());
  } else {
    FAISS_ASSERT (n0 == levels.size());
    for (int i = 0; i < n; i++) {
      int pt_level = random_level(level_constant);
      levels.push_back(pt_level);
    }
  }

  char *level0_links_new = (char*)malloc((n0 + n) * level0_link_size);
  if (level0_links_new == nullptr) {
    throw std::runtime_error("No enough memory 4 level0_links!");
  }
  memset(level0_links_new, 0, (n0 + n) * level0_link_size);
  if (level0_links) {
    memcpy(level0_links_new, level0_links, n0 * level0_link_size);
    free(level0_links);
  }
  level0_links = level0_links_new;

  char **linkLists_new = (char **)malloc(sizeof(void*) * (n0 + n));
  if (linkLists_new == nullptr) {
    throw std::runtime_error("No enough memory 4 level0_links_new!");
  }
  if (linkLists) {
    memcpy(linkLists_new, linkLists, n0 * sizeof(void*));
    free(linkLists);
  }
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
    if (max_level >= level_stats.size()) {
      level_stats.resize(max_level + 1);
    }
    level_stats[pt_level] ++;
  }

//  printf("level stats:\n");
//  for (int i = 0; i <= max_level; ++ i)
//    printf("level %d: %d points\n", i, level_stats[i]);
//  printf("\n");
  std::vector<std::mutex>(n0 + n).swap(link_list_locks);
  if (visited_list_pool) delete visited_list_pool;
  visited_list_pool = new VisitedListPool(1, n0 + n);

  return max_level;
}


/**************************************************************
 * new implementation of hnsw ispired by hnswlib
 * by cmli@zilliz   July 30, 2020
 **************************************************************/
using Node = faiss::RHNSW::Node;
using CompareByFirst = faiss::RHNSW::CompareByFirst;
void RHNSW::addPoint(DistanceComputer& ptdis, int pt_level, int pt_id) {

  std::unique_lock<std::mutex> lock_el(link_list_locks[pt_id]);
  std::unique_lock<std::mutex> temp_lock(global);
  int maxlevel_copy = max_level;
  if (pt_level <= maxlevel_copy)
    temp_lock.unlock();
  int currObj = entry_point;
  int ep_copy = entry_point;

  if (currObj != -1) {
    if (pt_level < maxlevel_copy) {
      float curdist = ptdis(currObj);
      for (int lev = maxlevel_copy; lev > pt_level; lev --) {
        bool changed = true;
        while (changed) {
          changed = false;
          std::unique_lock<std::mutex> lk(link_list_locks[currObj]);
          int *curObj_link = get_neighbor_link(currObj, lev);
          auto curObj_nei_num = get_neighbors_num(curObj_link);
          for (auto i = 1; i <= curObj_nei_num; ++ i) {
            int cand = curObj_link[i];
            if (cand < 0 || cand > levels.size())
              throw std::runtime_error("cand error when addPoint");
            float d = ptdis(cand);
            if (d < curdist) {
              curdist = d;
              currObj = cand;
              changed = true;
            }
          }
        }
      }
    }

    for (int lev = std::min(pt_level, maxlevel_copy); lev >= 0; -- lev) {
      if (lev > maxlevel_copy || lev < 0)
        throw std::runtime_error("Level error");

      std::priority_queue<Node, std::vector<Node>, CompareByFirst> top_candidates = search_layer(ptdis, pt_id, currObj, lev);
      currObj = top_candidates.top().second;
      make_connection(ptdis, pt_id, top_candidates, lev);
    }
  } else {
    entry_point = 0;
    max_level = pt_level;
  }

  if (pt_level > maxlevel_copy) {
    entry_point = pt_id;
    max_level = pt_level;
  }

}

std::priority_queue<Node, std::vector<Node>, CompareByFirst>
RHNSW::search_layer(DistanceComputer& ptdis,
                    storage_idx_t pt_id,
                    storage_idx_t nearest,
                    int level) {
  VisitedList *vl = visited_list_pool->getFreeVisitedList();
  vl_type *visited_array = vl->mass;
  vl_type visited_array_tag = vl->curV;

  std::priority_queue<Node, std::vector<Node>, CompareByFirst> top_candidates;
  std::priority_queue<Node, std::vector<Node>, CompareByFirst> candidate_set;

  float d_nearest = ptdis(nearest);
  float lb = d_nearest;
  top_candidates.emplace(d_nearest, nearest);
  candidate_set.emplace(-d_nearest, nearest);
  visited_array[nearest] = visited_array_tag;

  while (!candidate_set.empty()) {
    Node currNode = candidate_set.top();
    if ((-currNode.first) > lb)
      break;
    candidate_set.pop();
    int cur_id = currNode.second;
    std::unique_lock<std::mutex> lk(link_list_locks[cur_id]);
    int *cur_link = get_neighbor_link(cur_id, level);
    auto cur_neighbor_num = get_neighbors_num(cur_link);

    for (auto i = 1; i <= cur_neighbor_num; ++ i) {
      int candidate_id = cur_link[i];
      if (visited_array[candidate_id] == visited_array_tag) continue;
      visited_array[candidate_id] = visited_array_tag;
      float dcand = ptdis(candidate_id);
      if (top_candidates.size() < efConstruction || lb > dcand) {
        candidate_set.emplace(-dcand, candidate_id);
        top_candidates.emplace(dcand, candidate_id);
        if (top_candidates.size() > efConstruction)
          top_candidates.pop();
        if (!top_candidates.empty())
          lb = top_candidates.top().first;
      }
    }
  }
  visited_list_pool->releaseVisitedList(vl);
  return top_candidates;
}

std::priority_queue<Node, std::vector<Node>, CompareByFirst>
RHNSW::search_base_layer(DistanceComputer& ptdis,
                         storage_idx_t nearest,
                         storage_idx_t ef,
                         float d_nearest,
                         ConcurrentBitsetPtr bitset) const {
  VisitedList *vl = visited_list_pool->getFreeVisitedList();
  vl_type *visited_array = vl->mass;
  vl_type visited_array_tag = vl->curV;

  std::priority_queue<Node, std::vector<Node>, CompareByFirst> top_candidates;
  std::priority_queue<Node, std::vector<Node>, CompareByFirst> candidate_set;

  float lb;
  if (bitset == nullptr || !bitset->test((faiss::ConcurrentBitset::id_type_t)(nearest))) {
    lb = d_nearest;
    top_candidates.emplace(d_nearest, nearest);
    candidate_set.emplace(-d_nearest, nearest);
  } else {
    lb = std::numeric_limits<float>::max();
    candidate_set.emplace(-lb, nearest);
  }
  visited_array[nearest] = visited_array_tag;

  while (!candidate_set.empty()) {
    Node currNode = candidate_set.top();
    if ((-currNode.first) > lb)
      break;
    candidate_set.pop();
    int cur_id = currNode.second;
    int *cur_link = get_neighbor_link(cur_id, 0);
    auto cur_neighbor_num = get_neighbors_num(cur_link);
    for (auto i = 1; i <= cur_neighbor_num; ++ i) {
      int candidate_id = cur_link[i];
      if (visited_array[candidate_id] != visited_array_tag) {
        visited_array[candidate_id] = visited_array_tag;
        float dcand = ptdis(candidate_id);
        if (top_candidates.size() < ef || lb > dcand) {
          candidate_set.emplace(-dcand, candidate_id);
          if (bitset == nullptr || !bitset->test((faiss::ConcurrentBitset::id_type_t)(candidate_id)))
            top_candidates.emplace(dcand, candidate_id);
          if (top_candidates.size() > ef)
            top_candidates.pop();
          if (!top_candidates.empty())
            lb = top_candidates.top().first;
        }
      }
    }
  }
  visited_list_pool->releaseVisitedList(vl);
  return top_candidates;
}

void
RHNSW::make_connection(DistanceComputer& ptdis,
                       storage_idx_t pt_id,
                       std::priority_queue<Node, std::vector<Node>, CompareByFirst> &cand,
                       int level) {
  int maxM = level ? M : M << 1;
  int *selectedNeighbors = (int*)malloc(sizeof(int) * maxM);
  int selectedNeighborsNum = 0;
  prune_neighbors(ptdis, cand, maxM, selectedNeighbors, selectedNeighborsNum);
  if (selectedNeighborsNum > maxM)
    throw std::runtime_error("Wrong size of candidates returned by prune_neighbors!");

  int *cur_link = get_neighbor_link(pt_id, level);
  if (*cur_link)
    throw std::runtime_error("The newly inserted element should have blank link");

  set_neighbors_num(cur_link, selectedNeighborsNum);
  for (auto i = 1; i <= selectedNeighborsNum; ++ i) {
    if (cur_link[i])
      throw std::runtime_error("Possible memory corruption.");
    if (level > levels[selectedNeighbors[i - 1]])
      throw std::runtime_error("Trying to make a link on a non-exisitent level.");
    cur_link[i] = selectedNeighbors[i - 1];
  }

  for (auto i = 0; i < selectedNeighborsNum; ++ i) {
    std::unique_lock<std::mutex> lk(link_list_locks[selectedNeighbors[i]]);

    int *selected_link = get_neighbor_link(selectedNeighbors[i], level);
    auto selected_neighbor_num = get_neighbors_num(selected_link);
    if (selected_neighbor_num > maxM)
      throw std::runtime_error("Bad value of selected_neighbor_num.");
    if (selectedNeighbors[i] == pt_id)
      throw std::runtime_error("Trying to connect an element to itself.");
    if (level > levels[selectedNeighbors[i]])
      throw std::runtime_error("Trying to make a link on a non-exisitent level.");
    if (selected_neighbor_num < maxM) {
      selected_link[selected_neighbor_num + 1] = pt_id;
      set_neighbors_num(selected_link, selected_neighbor_num + 1);
    } else {
      double d_max = ptdis(selectedNeighbors[i]);
      std::priority_queue<Node, std::vector<Node>, CompareByFirst> candi;
      candi.emplace(d_max, pt_id);
      for (auto j = 1; j <= selected_neighbor_num; ++ j)
        candi.emplace(ptdis.symmetric_dis(selectedNeighbors[i], selected_link[j]), selected_link[j]);
      int indx = 0;
      prune_neighbors(ptdis, candi, maxM, selected_link + 1, indx);
      set_neighbors_num(selected_link, indx);
    }
  }

  free(selectedNeighbors);
}

void RHNSW::prune_neighbors(DistanceComputer& ptdis,
                            std::priority_queue<Node, std::vector<Node>, CompareByFirst> &cand,
                            const int maxM, int *ret, int &ret_len) {
  if (cand.size() < maxM) {
    while (!cand.empty()) {
      ret[ret_len ++] = cand.top().second;
      cand.pop();
    }
    return;
  }
  std::priority_queue<Node> closest;

  while (!cand.empty()) {
    closest.emplace(-cand.top().first, cand.top().second);
    cand.pop();
  }

  while (closest.size()) {
    if (ret_len >= maxM)
      break;
    Node curr = closest.top();
    float dist_to_query = -curr.first;
    closest.pop();
    bool good = true;
    for (auto i = 0; i < ret_len; ++ i) {
      float cur_dist = ptdis.symmetric_dis(curr.second, ret[i]);
      if (cur_dist < dist_to_query) {
        good = false;
        break;
      }
    }
    if (good) {
      ret[ret_len ++] = curr.second;
    }
  }
}

void RHNSW::searchKnn(DistanceComputer& qdis, int k,
            idx_t *I, float *D,
            ConcurrentBitsetPtr bitset) const {
  if (levels.size() == 0)
    return;
  int ep = entry_point;
  float dist = qdis(ep);

  for (auto i = max_level; i > 0; -- i) {
    bool good = true;
    while (good) {
      good = false;
      int *ep_link = get_neighbor_link(ep, i);
      auto ep_neighbors_cnt = get_neighbors_num(ep_link);
      for (auto j = 1; j <= ep_neighbors_cnt; ++ j) {
        int cand = ep_link[j];
        if (cand < 0 || cand > levels.size())
          throw std::runtime_error("cand error");
        float d = qdis(cand);
        if (d < dist) {
          dist = d;
          ep = cand;
          good = true;
        }
      }
    }
  }
  std::priority_queue<Node, std::vector<Node>, CompareByFirst> top_candidates = search_base_layer(qdis, ep, std::max(efSearch, k), dist, bitset);
  while (top_candidates.size() > k)
    top_candidates.pop();
  int i = 0;
  while (!top_candidates.empty()) {
    I[i] = top_candidates.top().second;
    D[i] = top_candidates.top().first;
    i ++;
    top_candidates.pop();
  }
}

size_t RHNSW::cal_size() {
  size_t ret = 0;
  ret += sizeof(*this);
  ret += visited_list_pool->GetSize();
  ret += link_list_locks.size() * sizeof(std::mutex);
  ret += levels.size() * sizeof(int);
  ret += levels.size() * level0_link_size;
  ret += levels.size() * sizeof(void*);
  for (auto i = 0; i < levels.size(); ++ i) {
    ret += levels[i] ? link_size * levels[i] : 0;
  }
  return ret;
}

}  // namespace faiss
