/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/IndexReplicas.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <functional>
#include <memory>
#include <vector>

namespace faiss { namespace gpu {

// If we want to run multi-GPU, create a proxy to wrap the indices.
// If we don't want multi-GPU, don't involve the proxy, so it doesn't
// affect the timings.
template <typename GpuIndex>
struct IndexWrapper {
  std::vector<std::unique_ptr<faiss::gpu::StandardGpuResources>> resources;
  std::vector<std::unique_ptr<GpuIndex>> subIndex;
  std::unique_ptr<faiss::IndexReplicas> replicaIndex;

  IndexWrapper(
    int numGpus,
    std::function<std::unique_ptr<GpuIndex>(GpuResources*, int)> init);
  faiss::Index* getIndex();

  void runOnIndices(std::function<void(GpuIndex*)> f);
  void setNumProbes(int nprobe);
};

} }

#include <faiss/gpu/perf/IndexWrapper-inl.h>
