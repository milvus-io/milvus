/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/IndexReplicas.h>
#include <faiss/impl/FaissAssert.h>

namespace faiss {

template <typename IndexT>
IndexReplicasTemplate<IndexT>::IndexReplicasTemplate(bool threaded)
    : ThreadedIndex<IndexT>(threaded) {
}

template <typename IndexT>
IndexReplicasTemplate<IndexT>::IndexReplicasTemplate(idx_t d, bool threaded)
    : ThreadedIndex<IndexT>(d, threaded) {
}

template <typename IndexT>
IndexReplicasTemplate<IndexT>::IndexReplicasTemplate(int d, bool threaded)
    : ThreadedIndex<IndexT>(d, threaded) {
}

template <typename IndexT>
void
IndexReplicasTemplate<IndexT>::onAfterAddIndex(IndexT* index) {
  // Make sure that the parameters are the same for all prior indices, unless
  // we're the first index to be added
  if (this->count() > 0 && this->at(0) != index) {
    auto existing = this->at(0);

    FAISS_THROW_IF_NOT_FMT(index->ntotal == existing->ntotal,
                           "IndexReplicas: newly added index does "
                           "not have same number of vectors as prior index; "
                           "prior index has %ld vectors, new index has %ld",
                           existing->ntotal, index->ntotal);

    FAISS_THROW_IF_NOT_MSG(index->is_trained == existing->is_trained,
                           "IndexReplicas: newly added index does "
                           "not have same train status as prior index");
  } else {
    // Set our parameters based on the first index we're adding
    // (dimension is handled in ThreadedIndex)
    this->ntotal = index->ntotal;
    this->verbose = index->verbose;
    this->is_trained = index->is_trained;
    this->metric_type = index->metric_type;
  }
}

template <typename IndexT>
void
IndexReplicasTemplate<IndexT>::train(idx_t n, const component_t* x) {
  this->runOnIndex([n, x](int, IndexT* index){ index->train(n, x); });
}

template <typename IndexT>
void
IndexReplicasTemplate<IndexT>::add(idx_t n, const component_t* x) {
  this->runOnIndex([n, x](int, IndexT* index){ index->add(n, x); });
  this->ntotal += n;
}

template <typename IndexT>
void
IndexReplicasTemplate<IndexT>::reconstruct(idx_t n, component_t* x) const {
  FAISS_THROW_IF_NOT_MSG(this->count() > 0, "no replicas in index");

  // Just pass to the first replica
  this->at(0)->reconstruct(n, x);
}

template <typename IndexT>
void
IndexReplicasTemplate<IndexT>::search(idx_t n,
                                      const component_t* x,
                                      idx_t k,
                                      distance_t* distances,
                                      idx_t* labels) const {
  FAISS_THROW_IF_NOT_MSG(this->count() > 0, "no replicas in index");

  if (n == 0) {
    return;
  }

  auto dim = this->d;
  size_t componentsPerVec =
    sizeof(component_t) == 1 ? (dim + 7) / 8 : dim;

  // Partition the query by the number of indices we have
  faiss::Index::idx_t queriesPerIndex =
    (faiss::Index::idx_t) (n + this->count() - 1) /
    (faiss::Index::idx_t) this->count();
  FAISS_ASSERT(n / queriesPerIndex <= this->count());

  auto fn =
    [queriesPerIndex, componentsPerVec,
     n, x, k, distances, labels](int i, const IndexT* index) {
      faiss::Index::idx_t base = (faiss::Index::idx_t) i * queriesPerIndex;

      if (base < n) {
        auto numForIndex = std::min(queriesPerIndex, n - base);

        index->search(numForIndex,
                      x + base * componentsPerVec,
                      k,
                      distances + base * k,
                      labels + base * k);
      }
    };

  this->runOnIndex(fn);
}

// explicit instantiations
template struct IndexReplicasTemplate<Index>;
template struct IndexReplicasTemplate<IndexBinary>;

} // namespace
