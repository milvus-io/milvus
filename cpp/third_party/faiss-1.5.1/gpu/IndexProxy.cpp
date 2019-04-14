/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "IndexProxy.h"
#include "../FaissAssert.h"

#include "../Clustering.h"
#include "GpuIndexFlat.h"
#include "StandardGpuResources.h"
#include <cstring>

namespace faiss { namespace gpu {

IndexProxy::IndexProxy():own_fields(false) {
}

IndexProxy::~IndexProxy() {
    if (own_fields) {
        for (auto& index : indices_)
           delete index.first;
    }
}

void
IndexProxy::addIndex(faiss::Index* index) {
  // Make sure that the parameters are the same for all prior indices
  if (!indices_.empty()) {
    auto& existing = indices_.front().first;

    if (index->d != existing->d) {
      FAISS_ASSERT(false);
      return;
    }

    if (index->ntotal != existing->ntotal) {
      FAISS_ASSERT(false);
      return;
    }

    if (index->metric_type != existing->metric_type) {
      FAISS_ASSERT(false);
      return;
    }
} else {
    // Set our parameters
    // FIXME: this is a little bit weird
    this->d = index->d;
    this->ntotal = index->ntotal;
    this->verbose = index->verbose;
    this->is_trained = index->is_trained;
    this->metric_type = index->metric_type;
  }

  indices_.emplace_back(
    std::make_pair(index,
                   std::unique_ptr<WorkerThread>(new WorkerThread)));
}

void
IndexProxy::removeIndex(faiss::Index* index) {
  for (auto it = indices_.begin(); it != indices_.end(); ++it) {
    if (it->first == index) {
      // This is our index; stop the worker thread before removing it,
      // to ensure that it has finished before function exit
      it->second->stop();
      it->second->waitForThreadExit();

      indices_.erase(it);
      return;
    }
  }

  // index not found
  FAISS_ASSERT(false);
}

void
IndexProxy::runOnIndex(std::function<void(faiss::Index*)> f) {
  std::vector<std::future<bool>> v;

  for (auto& index : indices_) {
    auto indexPtr = index.first;
    v.emplace_back(index.second->add([indexPtr, f](){ f(indexPtr); }));
  }

  // Blocking wait for completion
  for (auto& func : v) {
    func.get();
  }
}

void
IndexProxy::reset() {
  runOnIndex([](faiss::Index* index){ index->reset(); });
  ntotal = 0;
}

void
IndexProxy::train(Index::idx_t n, const float* x) {
  runOnIndex([n, x](faiss::Index* index){ index->train(n, x); });
}

void
IndexProxy::add(Index::idx_t n, const float* x) {
  runOnIndex([n, x](faiss::Index* index){ index->add(n, x); });
  ntotal += n;
}

void
IndexProxy::reconstruct(Index::idx_t n, float* x) const {
    FAISS_ASSERT (count() > 0);
    indices_[0].first->reconstruct (n, x);
}


void
IndexProxy::search(faiss::Index::idx_t n,
                        const float* x,
                        faiss::Index::idx_t k,
                        float* distances,
                        faiss::Index::idx_t* labels) const {
  FAISS_ASSERT(!indices_.empty());
  if (n == 0) {
    return;
  }

  auto dim = indices_.front().first->d;

  std::vector<std::future<bool>> v;

  // Partition the query by the number of indices we have
  auto queriesPerIndex =
    (faiss::Index::idx_t) (n + indices_.size() - 1) / indices_.size();
  FAISS_ASSERT(n / queriesPerIndex <= indices_.size());

  for (int i = 0; i < indices_.size(); ++i) {
    auto base = i * queriesPerIndex;
    if (base >= n) {
      break;
    }

    auto numForIndex = std::min(queriesPerIndex, n - base);
    auto queryStart = x + base * dim;
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



//
// GPU clustering implementation
//

float kmeans_clustering_gpu (int ngpu, size_t d, size_t n, size_t k,
                             const float *x,
                             float *centroids,
                             bool useFloat16,
                             bool storeTransposed)
{
    Clustering clus (d, k);
    // display logs if > 16Gflop per iteration
    clus.verbose = d * n * k > (1L << 34);
    FAISS_ASSERT(ngpu >= 1);

    std::vector<std::unique_ptr<StandardGpuResources> > res;
    std::vector<std::unique_ptr<GpuIndexFlatL2> > sub_indices;
    for(int dev_no = 0; dev_no < ngpu; dev_no++) {
        res.emplace_back(new StandardGpuResources());


        GpuIndexFlatConfig config;
        config.device = dev_no;
        config.useFloat16 = useFloat16;
        config.storeTransposed = storeTransposed;

        sub_indices.emplace_back(
          new GpuIndexFlatL2(res.back().get(), d, config));
    }

    IndexProxy proxy;
    Index *index;
    if (ngpu == 1) {
        index = sub_indices[0].get();
    } else {
        for(int dev_no = 0; dev_no < ngpu; dev_no++) {
            proxy.addIndex(sub_indices[dev_no].get());
        }
        index = &proxy;
    }
    clus.train (n, x, *index);

    memcpy(centroids, clus.centroids.data(), sizeof(*centroids) * d * k);
    return clus.obj.back();

}




} } // namespace
