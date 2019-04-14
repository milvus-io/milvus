/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexBinaryHNSW.h"


#include <memory>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio>
#include <cmath>
#include <omp.h>

#include <unordered_set>
#include <queue>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>

#include "utils.h"
#include "Heap.h"
#include "FaissAssert.h"
#include "IndexBinaryFlat.h"
#include "hamming.h"
#include "AuxIndexStructures.h"

namespace faiss {


/**************************************************************
 * add / search blocks of descriptors
 **************************************************************/

namespace {


void hnsw_add_vertices(IndexBinaryHNSW& index_hnsw,
                       size_t n0,
                       size_t n, const uint8_t *x,
                       bool verbose,
                       bool preset_levels = false) {
  HNSW& hnsw = index_hnsw.hnsw;
  size_t ntotal = n0 + n;
  double t0 = getmillisecs();
  if (verbose) {
    printf("hnsw_add_vertices: adding %ld elements on top of %ld "
           "(preset_levels=%d)\n",
           n, n0, int(preset_levels));
  }

  int max_level = hnsw.prepare_level_tab(n, preset_levels);

  if (verbose) {
    printf("  max_level = %d\n", max_level);
  }

  std::vector<omp_lock_t> locks(ntotal);
  for(int i = 0; i < ntotal; i++) {
    omp_init_lock(&locks[i]);
  }

  // add vectors from highest to lowest level
  std::vector<int> hist;
  std::vector<int> order(n);

  { // make buckets with vectors of the same level

    // build histogram
    for (int i = 0; i < n; i++) {
      HNSW::storage_idx_t pt_id = i + n0;
      int pt_level = hnsw.levels[pt_id] - 1;
      while (pt_level >= hist.size()) {
        hist.push_back(0);
      }
      hist[pt_level] ++;
    }

    // accumulate
    std::vector<int> offsets(hist.size() + 1, 0);
    for (int i = 0; i < hist.size() - 1; i++) {
      offsets[i + 1] = offsets[i] + hist[i];
    }

    // bucket sort
    for (int i = 0; i < n; i++) {
      HNSW::storage_idx_t pt_id = i + n0;
      int pt_level = hnsw.levels[pt_id] - 1;
      order[offsets[pt_level]++] = pt_id;
    }
  }

  { // perform add
    RandomGenerator rng2(789);

    int i1 = n;

    for (int pt_level = hist.size() - 1; pt_level >= 0; pt_level--) {
      int i0 = i1 - hist[pt_level];

      if (verbose) {
        printf("Adding %d elements at level %d\n",
               i1 - i0, pt_level);
      }

      // random permutation to get rid of dataset order bias
      for (int j = i0; j < i1; j++) {
        std::swap(order[j], order[j + rng2.rand_int(i1 - j)]);
      }

#pragma omp parallel
      {
        VisitedTable vt (ntotal);

        std::unique_ptr<DistanceComputer> dis(
          index_hnsw.get_distance_computer()
        );
        int prev_display = verbose && omp_get_thread_num() == 0 ? 0 : -1;

#pragma omp  for schedule(dynamic)
        for (int i = i0; i < i1; i++) {
          HNSW::storage_idx_t pt_id = order[i];
          dis->set_query((float *)(x + (pt_id - n0) * index_hnsw.code_size));

          hnsw.add_with_locks(*dis, pt_level, pt_id, locks, vt);

          if (prev_display >= 0 && i - i0 > prev_display + 10000) {
            prev_display = i - i0;
            printf("  %d / %d\r", i - i0, i1 - i0);
            fflush(stdout);
          }
        }
      }
      i1 = i0;
    }
    FAISS_ASSERT(i1 == 0);
  }
  if (verbose) {
    printf("Done in %.3f ms\n", getmillisecs() - t0);
  }

  for(int i = 0; i < ntotal; i++)
    omp_destroy_lock(&locks[i]);
}


} // anonymous namespace


/**************************************************************
 * IndexBinaryHNSW implementation
 **************************************************************/

IndexBinaryHNSW::IndexBinaryHNSW()
{
  is_trained = true;
}

IndexBinaryHNSW::IndexBinaryHNSW(int d, int M)
    : IndexBinary(d),
      hnsw(M),
      own_fields(true),
      storage(new IndexBinaryFlat(d))
{
  is_trained = true;
}

IndexBinaryHNSW::IndexBinaryHNSW(IndexBinary *storage, int M)
    : IndexBinary(storage->d),
      hnsw(M),
      own_fields(false),
      storage(storage)
{
  is_trained = true;
}

IndexBinaryHNSW::~IndexBinaryHNSW() {
  if (own_fields) {
    delete storage;
  }
}

void IndexBinaryHNSW::train(idx_t n, const uint8_t *x)
{
  // hnsw structure does not require training
  storage->train(n, x);
  is_trained = true;
}

void IndexBinaryHNSW::search(idx_t n, const uint8_t *x, idx_t k,
                             int32_t *distances, idx_t *labels) const
{
#pragma omp parallel
  {
    VisitedTable vt(ntotal);
    std::unique_ptr<DistanceComputer> dis(get_distance_computer());

#pragma omp for
    for(idx_t i = 0; i < n; i++) {
      idx_t *idxi = labels + i * k;
      float *simi = (float *)(distances + i * k);

      dis->set_query((float *)(x + i * code_size));

      maxheap_heapify(k, simi, idxi);
      hnsw.search(*dis, k, idxi, simi, vt);
      maxheap_reorder(k, simi, idxi);
    }
  }

#pragma omp parallel for
  for (int i = 0; i < n * k; ++i) {
    distances[i] = std::round(((float *)distances)[i]);
  }
}


void IndexBinaryHNSW::add(idx_t n, const uint8_t *x)
{
  FAISS_THROW_IF_NOT(is_trained);
  int n0 = ntotal;
  storage->add(n, x);
  ntotal = storage->ntotal;

  hnsw_add_vertices(*this, n0, n, x, verbose,
                    hnsw.levels.size() == ntotal);
}

void IndexBinaryHNSW::reset()
{
  hnsw.reset();
  storage->reset();
  ntotal = 0;
}

void IndexBinaryHNSW::reconstruct(idx_t key, uint8_t *recons) const
{
  storage->reconstruct(key, recons);
}


namespace {


template<class HammingComputer>
struct FlatHammingDis : DistanceComputer {
  const int code_size;
  const uint8_t *b;
  size_t ndis;
  HammingComputer hc;

  float operator () (idx_t i) override {
    ndis++;
    return hc.hamming(b + i * code_size);
  }

  float symmetric_dis(idx_t i, idx_t j) override {
    return HammingComputerDefault(b + j * code_size, code_size)
      .hamming(b + i * code_size);
  }


  explicit FlatHammingDis(const IndexBinaryFlat& storage)
      : code_size(storage.code_size),
        b(storage.xb.data()),
        ndis(0),
        hc() {}

  // NOTE: Pointers are cast from float in order to reuse the floating-point
  //   DistanceComputer.
  void set_query(const float *x) override {
    hc.set((uint8_t *)x, code_size);
  }

  ~FlatHammingDis() override {
#pragma omp critical
    {
      hnsw_stats.ndis += ndis;
    }
  }
};


}  // namespace


DistanceComputer *IndexBinaryHNSW::get_distance_computer() const {
  IndexBinaryFlat *flat_storage = dynamic_cast<IndexBinaryFlat *>(storage);

  FAISS_ASSERT(flat_storage != nullptr);

  switch(code_size) {
    case 4:
      return new FlatHammingDis<HammingComputer4>(*flat_storage);
    case 8:
      return new FlatHammingDis<HammingComputer8>(*flat_storage);
    case 16:
      return new FlatHammingDis<HammingComputer16>(*flat_storage);
    case 20:
      return new FlatHammingDis<HammingComputer20>(*flat_storage);
    case 32:
      return new FlatHammingDis<HammingComputer32>(*flat_storage);
    case 64:
      return new FlatHammingDis<HammingComputer64>(*flat_storage);
    default:
      if (code_size % 8 == 0) {
        return new FlatHammingDis<HammingComputerM8>(*flat_storage);
      } else if (code_size % 4 == 0) {
        return new FlatHammingDis<HammingComputerM4>(*flat_storage);
      }
  }

  return new FlatHammingDis<HammingComputerDefault>(*flat_storage);
}


} // namespace faiss
