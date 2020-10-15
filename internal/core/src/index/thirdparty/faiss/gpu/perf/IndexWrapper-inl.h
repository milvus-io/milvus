/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/impl/FaissAssert.h>

namespace faiss { namespace gpu {

template <typename GpuIndex>
IndexWrapper<GpuIndex>::IndexWrapper(
  int numGpus,
  std::function<std::unique_ptr<GpuIndex>(GpuResources*, int)> init) {
  FAISS_ASSERT(numGpus <= faiss::gpu::getNumDevices());
  for (int i = 0; i < numGpus; ++i) {
    auto res = std::unique_ptr<faiss::gpu::StandardGpuResources>(
      new StandardGpuResources);

    subIndex.emplace_back(init(res.get(), i));
    resources.emplace_back(std::move(res));
  }

  if (numGpus > 1) {
    // create proxy
    replicaIndex =
      std::unique_ptr<faiss::IndexReplicas>(new faiss::IndexReplicas);

    for (auto& index : subIndex) {
      replicaIndex->addIndex(index.get());
    }
  }
}

template <typename GpuIndex>
faiss::Index*
IndexWrapper<GpuIndex>::getIndex() {
  if ((bool) replicaIndex) {
    return replicaIndex.get();
  } else {
    FAISS_ASSERT(!subIndex.empty());
    return subIndex.front().get();
  }
}

template <typename GpuIndex>
void
IndexWrapper<GpuIndex>::runOnIndices(std::function<void(GpuIndex*)> f) {

  if ((bool) replicaIndex) {
    replicaIndex->runOnIndex(
      [f](int, faiss::Index* index) {
        f(dynamic_cast<GpuIndex*>(index));
      });
  } else {
    FAISS_ASSERT(!subIndex.empty());
    f(subIndex.front().get());
  }
}

template <typename GpuIndex>
void
IndexWrapper<GpuIndex>::setNumProbes(int nprobe) {
  runOnIndices([nprobe](GpuIndex* index) {
      index->setNumProbes(nprobe);
    });
}

} }
