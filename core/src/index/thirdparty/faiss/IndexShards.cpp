/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexShards.h>

#include <cstdio>
#include <functional>

#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/Heap.h>
#include <faiss/utils/WorkerThread.h>

namespace faiss {

// subroutines
namespace {

typedef Index::idx_t idx_t;


// add translation to all valid labels
void translate_labels (long n, idx_t *labels, long translation)
{
    if (translation == 0) return;
    for (long i = 0; i < n; i++) {
        if(labels[i] < 0) continue;
        labels[i] += translation;
    }
}


/** merge result tables from several shards.
 * @param all_distances  size nshard * n * k
 * @param all_labels     idem
 * @param translartions  label translations to apply, size nshard
 */

template <class IndexClass, class C>
void
merge_tables(long n, long k, long nshard,
             typename IndexClass::distance_t *distances,
             idx_t *labels,
             const std::vector<typename IndexClass::distance_t>& all_distances,
             const std::vector<idx_t>& all_labels,
             const std::vector<long>& translations) {
  if (k == 0) {
    return;
  }
  using distance_t = typename IndexClass::distance_t;

  long stride = n * k;
#pragma omp parallel
  {
    std::vector<int> buf (2 * nshard);
    int * pointer = buf.data();
    int * shard_ids = pointer + nshard;
    std::vector<distance_t> buf2 (nshard);
    distance_t * heap_vals = buf2.data();
#pragma omp for
    for (long i = 0; i < n; i++) {
      // the heap maps values to the shard where they are
      // produced.
      const distance_t *D_in = all_distances.data() + i * k;
      const idx_t *I_in = all_labels.data() + i * k;
      int heap_size = 0;

      for (long s = 0; s < nshard; s++) {
        pointer[s] = 0;
        if (I_in[stride * s] >= 0) {
          heap_push<C> (++heap_size, heap_vals, shard_ids,
                        D_in[stride * s], s);
        }
      }

      distance_t *D = distances + i * k;
      idx_t *I = labels + i * k;

      for (int j = 0; j < k; j++) {
        if (heap_size == 0) {
          I[j] = -1;
          D[j] = C::neutral();
        } else {
          // pop best element
          int s = shard_ids[0];
          int & p = pointer[s];
          D[j] = heap_vals[0];
          I[j] = I_in[stride * s + p] + translations[s];

          heap_pop<C> (heap_size--, heap_vals, shard_ids);
          p++;
          if (p < k && I_in[stride * s + p] >= 0) {
            heap_push<C> (++heap_size, heap_vals, shard_ids,
                          D_in[stride * s + p], s);
          }
        }
      }
    }
  }
}

} // anonymous namespace

template <typename IndexT>
IndexShardsTemplate<IndexT>::IndexShardsTemplate(idx_t d,
                                                 bool threaded,
                                                 bool successive_ids)
    : ThreadedIndex<IndexT>(d, threaded),
      successive_ids(successive_ids) {
}

template <typename IndexT>
IndexShardsTemplate<IndexT>::IndexShardsTemplate(int d,
                                                 bool threaded,
                                                 bool successive_ids)
    : ThreadedIndex<IndexT>(d, threaded),
      successive_ids(successive_ids) {
}

template <typename IndexT>
IndexShardsTemplate<IndexT>::IndexShardsTemplate(bool threaded,
                                                 bool successive_ids)
    : ThreadedIndex<IndexT>(threaded),
      successive_ids(successive_ids) {
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::onAfterAddIndex(IndexT* index /* unused */) {
  sync_with_shard_indexes();
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::onAfterRemoveIndex(IndexT* index /* unused */) {
  sync_with_shard_indexes();
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::sync_with_shard_indexes() {
  if (!this->count()) {
    this->is_trained = false;
    this->ntotal = 0;

    return;
  }

  auto firstIndex = this->at(0);
  this->metric_type = firstIndex->metric_type;
  this->is_trained = firstIndex->is_trained;
  this->ntotal = firstIndex->ntotal;

  for (int i = 1; i < this->count(); ++i) {
    auto index = this->at(i);
    FAISS_THROW_IF_NOT(this->metric_type == index->metric_type);
    FAISS_THROW_IF_NOT(this->d == index->d);

    this->ntotal += index->ntotal;
  }
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::train(idx_t n,
                                   const component_t *x) {
  auto fn =
    [n, x](int no, IndexT *index) {
      if (index->verbose) {
        printf("begin train shard %d on %ld points\n", no, n);
      }

      index->train(n, x);

      if (index->verbose) {
        printf("end train shard %d\n", no);
      }
    };

  this->runOnIndex(fn);
  sync_with_shard_indexes();
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::add(idx_t n,
                                 const component_t *x) {
  add_with_ids(n, x, nullptr);
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::add_with_ids(idx_t n,
                                          const component_t * x,
                                          const idx_t *xids) {

  FAISS_THROW_IF_NOT_MSG(!(successive_ids && xids),
                         "It makes no sense to pass in ids and "
                         "request them to be shifted");

  if (successive_ids) {
    FAISS_THROW_IF_NOT_MSG(!xids,
                           "It makes no sense to pass in ids and "
                           "request them to be shifted");
    FAISS_THROW_IF_NOT_MSG(this->ntotal == 0,
                           "when adding to IndexShards with sucessive_ids, "
                           "only add() in a single pass is supported");
  }

  idx_t nshard = this->count();
  const idx_t *ids = xids;

  std::vector<idx_t> aids;

  if (!ids && !successive_ids) {
    aids.resize(n);

    for (idx_t i = 0; i < n; i++) {
      aids[i] = this->ntotal + i;
    }

    ids = aids.data();
  }

  size_t components_per_vec =
    sizeof(component_t) == 1 ? (this->d + 7) / 8 : this->d;

  auto fn =
    [n, ids, x, nshard, components_per_vec](int no, IndexT *index) {
      idx_t i0 = (idx_t) no * n / nshard;
      idx_t i1 = ((idx_t) no + 1) * n / nshard;
      auto x0 = x + i0 * components_per_vec;

      if (index->verbose) {
        printf ("begin add shard %d on %ld points\n", no, n);
      }

      if (ids) {
        index->add_with_ids (i1 - i0, x0, ids + i0);
      } else {
        index->add (i1 - i0, x0);
      }

      if (index->verbose) {
        printf ("end add shard %d on %ld points\n", no, i1 - i0);
      }
    };

  this->runOnIndex(fn);

  // This is safe to do here because the current thread controls execution in
  // all threads, and nothing else is happening
  this->ntotal += n;
}

template <typename IndexT>
void
IndexShardsTemplate<IndexT>::search(idx_t n,
                                    const component_t *x,
                                    idx_t k,
                                    distance_t *distances,
                                    idx_t *labels) const {
  long nshard = this->count();

  std::vector<distance_t> all_distances(nshard * k * n);
  std::vector<idx_t> all_labels(nshard * k * n);

  auto fn =
    [n, k, x, &all_distances, &all_labels](int no, const IndexT *index) {
      if (index->verbose) {
        printf ("begin query shard %d on %ld points\n", no, n);
      }

      index->search (n, x, k,
                     all_distances.data() + no * k * n,
                     all_labels.data() + no * k * n);

      if (index->verbose) {
        printf ("end query shard %d\n", no);
      }
    };

  this->runOnIndex(fn);

  std::vector<long> translations(nshard, 0);

  // Because we just called runOnIndex above, it is safe to access the sub-index
  // ntotal here
  if (successive_ids) {
    translations[0] = 0;

    for (int s = 0; s + 1 < nshard; s++) {
      translations[s + 1] = translations[s] + this->at(s)->ntotal;
    }
  }

  if (this->metric_type == METRIC_L2) {
    merge_tables<IndexT, CMin<distance_t, int>>(
      n, k, nshard, distances, labels,
      all_distances, all_labels, translations);
  } else {
    merge_tables<IndexT, CMax<distance_t, int>>(
      n, k, nshard, distances, labels,
      all_distances, all_labels, translations);
  }
}

// explicit instanciations
template struct IndexShardsTemplate<Index>;
template struct IndexShardsTemplate<IndexBinary>;

} // namespace faiss
