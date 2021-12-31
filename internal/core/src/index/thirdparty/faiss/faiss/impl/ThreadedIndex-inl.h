/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/impl/FaissAssert.h>
#include <exception>
#include <iostream>

namespace faiss {

template <typename IndexT>
ThreadedIndex<IndexT>::ThreadedIndex(bool threaded)
    // 0 is default dimension
    : ThreadedIndex(0, threaded) {
}

template <typename IndexT>
ThreadedIndex<IndexT>::ThreadedIndex(int d, bool threaded)
    : IndexT(d),
      own_fields(false),
      isThreaded_(threaded) {
  }

template <typename IndexT>
ThreadedIndex<IndexT>::~ThreadedIndex() {
  for (auto& p : indices_) {
    if (isThreaded_) {
      // should have worker thread
      FAISS_ASSERT((bool) p.second);

      // This will also flush all pending work
      p.second->stop();
      p.second->waitForThreadExit();
    } else {
      // should not have worker thread
      FAISS_ASSERT(!(bool) p.second);
    }

    if (own_fields) {
      delete p.first;
    }
  }
}

template <typename IndexT>
void ThreadedIndex<IndexT>::addIndex(IndexT* index) {
  // We inherit the dimension from the first index added to us if we don't have
  // a set dimension
  if (indices_.empty() && this->d == 0) {
    this->d = index->d;
  }

  // The new index must match our set dimension
  FAISS_THROW_IF_NOT_FMT(this->d == index->d,
                         "addIndex: dimension mismatch for "
                         "newly added index; expecting dim %d, "
                         "new index has dim %d",
                         this->d, index->d);

  if (!indices_.empty()) {
    auto& existing = indices_.front().first;

    FAISS_THROW_IF_NOT_MSG(index->metric_type == existing->metric_type,
                           "addIndex: newly added index is "
                           "of different metric type than old index");

    // Make sure this index is not duplicated
    for (auto& p : indices_) {
      FAISS_THROW_IF_NOT_MSG(p.first != index,
                             "addIndex: attempting to add index "
                             "that is already in the collection");
    }
  }

  indices_.emplace_back(
    std::make_pair(
      index,
      std::unique_ptr<WorkerThread>(isThreaded_ ?
                                    new WorkerThread : nullptr)));

  onAfterAddIndex(index);
}

template <typename IndexT>
void ThreadedIndex<IndexT>::removeIndex(IndexT* index) {
  for (auto it = indices_.begin(); it != indices_.end(); ++it) {
    if (it->first == index) {
      // This is our index; stop the worker thread before removing it,
      // to ensure that it has finished before function exit
      if (isThreaded_) {
        // should have worker thread
        FAISS_ASSERT((bool) it->second);
        it->second->stop();
        it->second->waitForThreadExit();
      } else {
        // should not have worker thread
        FAISS_ASSERT(!(bool) it->second);
      }

      indices_.erase(it);
      onAfterRemoveIndex(index);

      if (own_fields) {
        delete index;
      }

      return;
    }
  }

  // could not find our index
  FAISS_THROW_MSG("IndexReplicas::removeIndex: index not found");
}

template <typename IndexT>
void ThreadedIndex<IndexT>::runOnIndex(std::function<void(int, IndexT*)> f) {
  if (isThreaded_) {
    std::vector<std::future<bool>> v;

    for (int i = 0; i < this->indices_.size(); ++i) {
      auto& p = this->indices_[i];
      auto indexPtr = p.first;
      v.emplace_back(p.second->add([f, i, indexPtr](){ f(i, indexPtr); }));
    }

    waitAndHandleFutures(v);
  } else {
    // Multiple exceptions may be thrown; gather them as we encounter them,
    // while letting everything else run to completion
    std::vector<std::pair<int, std::exception_ptr>> exceptions;

    for (int i = 0; i < this->indices_.size(); ++i) {
      auto& p = this->indices_[i];
      try {
        f(i, p.first);
      } catch (...) {
        exceptions.emplace_back(std::make_pair(i, std::current_exception()));
      }
    }

    handleExceptions(exceptions);
  }
}

template <typename IndexT>
void ThreadedIndex<IndexT>::runOnIndex(
  std::function<void(int, const IndexT*)> f) const {
  const_cast<ThreadedIndex<IndexT>*>(this)->runOnIndex(
    [f](int i, IndexT* idx){ f(i, idx); });
}

template <typename IndexT>
void ThreadedIndex<IndexT>::reset() {
  runOnIndex([](int, IndexT* index){ index->reset(); });
  this->ntotal = 0;
  this->is_trained = false;
}

template <typename IndexT>
void
ThreadedIndex<IndexT>::onAfterAddIndex(IndexT* index) {
}

template <typename IndexT>
void
ThreadedIndex<IndexT>::onAfterRemoveIndex(IndexT* index) {
}

template <typename IndexT>
void
ThreadedIndex<IndexT>::waitAndHandleFutures(std::vector<std::future<bool>>& v) {
  // Blocking wait for completion for all of the indices, capturing any
  // exceptions that are generated
  std::vector<std::pair<int, std::exception_ptr>> exceptions;

  for (int i = 0; i < v.size(); ++i) {
    auto& fut = v[i];

    try {
      fut.get();
    } catch (...) {
      exceptions.emplace_back(std::make_pair(i, std::current_exception()));
    }
  }

  handleExceptions(exceptions);
}

} // namespace
