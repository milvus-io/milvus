/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "IndexReplicas.h"
#include "FaissAssert.h"

namespace faiss {

template<class IndexClass>
IndexReplicasTemplate<IndexClass>::IndexReplicasTemplate()
    : own_fields(false) {
}

template<class IndexClass>
IndexReplicasTemplate<IndexClass>::~IndexReplicasTemplate() {
  if (own_fields) {
    for (auto& index : this->indices_)
      delete index.first;
  }
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::addIndex(IndexClass* index) {
  // Make sure that the parameters are the same for all prior indices
  if (!indices_.empty()) {
    auto& existing = indices_.front().first;

    FAISS_THROW_IF_NOT_FMT(index->d == existing->d,
                           "IndexReplicas::addIndex: dimension mismatch for "
                           "newly added index; prior index has dim %d, "
                           "new index has %d",
                           existing->d, index->d);

    FAISS_THROW_IF_NOT_FMT(index->ntotal == existing->ntotal,
                           "IndexReplicas::addIndex: newly added index does "
                           "not have same number of vectors as prior index; "
                           "prior index has %ld vectors, new index has %ld",
                           existing->ntotal, index->ntotal);

    FAISS_THROW_IF_NOT_MSG(index->metric_type == existing->metric_type,
                           "IndexReplicas::addIndex: newly added index is "
                           "of different metric type than old index");
  } else {
    // Set our parameters
    // FIXME: this is a little bit weird
    this->d = index->d;
    this->ntotal = index->ntotal;
    this->verbose = index->verbose;
    this->is_trained = index->is_trained;
    this->metric_type = index->metric_type;
  }

  this->indices_.emplace_back(
    std::make_pair(index,
                   std::unique_ptr<WorkerThread>(new WorkerThread)));
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::removeIndex(IndexClass* index) {
  for (auto it = this->indices_.begin(); it != indices_.end(); ++it) {
    if (it->first == index) {
      // This is our index; stop the worker thread before removing it,
      // to ensure that it has finished before function exit
      it->second->stop();
      it->second->waitForThreadExit();

      this->indices_.erase(it);
      return;
    }
  }

  // could not find our index
  FAISS_THROW_MSG("IndexReplicas::removeIndex: index not found");
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::runOnIndex(std::function<void(IndexClass*)> f) {
  FAISS_THROW_IF_NOT_MSG(!indices_.empty(), "no replicas in index");

  std::vector<std::future<bool>> v;

  for (auto& index : this->indices_) {
    auto indexPtr = index.first;
    v.emplace_back(index.second->add([indexPtr, f](){ f(indexPtr); }));
  }

  // Blocking wait for completion
  for (auto& func : v) {
    func.get();
  }
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::reset() {
  runOnIndex([](IndexClass* index){ index->reset(); });
  this->ntotal = 0;
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::train(idx_t n, const component_t* x) {
  runOnIndex([n, x](IndexClass* index){ index->train(n, x); });
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::add(idx_t n, const component_t* x) {
  runOnIndex([n, x](IndexClass* index){ index->add(n, x); });
  this->ntotal += n;
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::reconstruct(idx_t n, component_t* x) const {
  FAISS_THROW_IF_NOT_MSG(!indices_.empty(), "no replicas in index");
  indices_[0].first->reconstruct (n, x);
}

template<class IndexClass>
void IndexReplicasTemplate<IndexClass>::search(
              idx_t n,
              const component_t* x,
              idx_t k,
              distance_t* distances,
              idx_t* labels) const {
  FAISS_THROW_IF_NOT_MSG(!indices_.empty(), "no replicas in index");

  if (n == 0) {
    return;
  }

  auto dim = indices_.front().first->d;

  std::vector<std::future<bool>> v;

  // Partition the query by the number of indices we have
  auto queriesPerIndex =
    (faiss::Index::idx_t) (n + indices_.size() - 1) / indices_.size();
  FAISS_ASSERT(n / queriesPerIndex <= indices_.size());

  for (faiss::Index::idx_t i = 0; i < indices_.size(); ++i) {
    auto base = i * queriesPerIndex;
    if (base >= n) {
      break;
    }

    auto numForIndex = std::min(queriesPerIndex, n - base);
    size_t components_per_vec = sizeof(component_t) == 1 ? (dim + 7) / 8 : dim;
    auto queryStart = x + base * components_per_vec;
    auto distancesStart = distances + base * k;
    auto labelsStart = labels + base * k;

    auto indexPtr = indices_[i].first;
    auto fn =
      [indexPtr, numForIndex, queryStart, k, distancesStart, labelsStart]() {
        indexPtr->search(numForIndex, queryStart,
                         k, distancesStart, labelsStart);
      };

    v.emplace_back(indices_[i].second->add(std::move(fn)));
  }

  // Blocking wait for completion
  for (auto& f : v) {
    f.get();
  }
}

// explicit instanciations
template struct IndexReplicasTemplate<Index>;
template struct IndexReplicasTemplate<IndexBinary>;


} // namespace
