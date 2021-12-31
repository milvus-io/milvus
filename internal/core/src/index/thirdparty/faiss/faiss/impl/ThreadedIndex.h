/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/Index.h>
#include <faiss/IndexBinary.h>
#include <faiss/utils/WorkerThread.h>
#include <memory>
#include <vector>

namespace faiss {

/// A holder of indices in a collection of threads
/// The interface to this class itself is not thread safe
template <typename IndexT>
class ThreadedIndex : public IndexT {
 public:
  explicit ThreadedIndex(bool threaded);
  explicit ThreadedIndex(int d, bool threaded);

  ~ThreadedIndex() override;

  /// override an index that is managed by ourselves.
  /// WARNING: once an index is added, it becomes unsafe to touch it from any
  /// other thread than that on which is managing it, until we are shut
  /// down. Use runOnIndex to perform work on it instead.
  void addIndex(IndexT* index);

  /// Remove an index that is managed by ourselves.
  /// This will flush all pending work on that index, and then shut
  /// down its managing thread, and will remove the index.
  void removeIndex(IndexT* index);

  /// Run a function on all indices, in the thread that the index is
  /// managed in.
  /// Function arguments are (index in collection, index pointer)
  void runOnIndex(std::function<void(int, IndexT*)> f);
  void runOnIndex(std::function<void(int, const IndexT*)> f) const;

  /// faiss::Index API
  /// All indices receive the same call
  void reset() override;

  /// Returns the number of sub-indices
  int count() const { return indices_.size(); }

  /// Returns the i-th sub-index
  IndexT* at(int i) { return indices_[i].first; }

  /// Returns the i-th sub-index (const version)
  const IndexT* at(int i) const { return indices_[i].first; }

  /// Whether or not we are responsible for deleting our contained indices
  bool own_fields;

 protected:
  /// Called just after an index is added
  virtual void onAfterAddIndex(IndexT* index);

  /// Called just after an index is removed
  virtual void onAfterRemoveIndex(IndexT* index);

protected:
  static void waitAndHandleFutures(std::vector<std::future<bool>>& v);

  /// Collection of Index instances, with their managing worker thread if any
  std::vector<std::pair<IndexT*, std::unique_ptr<WorkerThread>>> indices_;

  /// Is this index multi-threaded?
  bool isThreaded_;
};

} // namespace

#include <faiss/impl/ThreadedIndex-inl.h>
