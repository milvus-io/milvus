/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "../Index.h"
#include "../AutoTune.h"
#include "GpuClonerOptions.h"
#include "GpuIndex.h"
#include "GpuIndicesOptions.h"

namespace faiss { namespace gpu {

class GpuResources;

// to support auto-tuning we need cloning to/from CPU

/// converts any GPU index inside gpu_index to a CPU index
faiss::Index * index_gpu_to_cpu(const faiss::Index *gpu_index);

/// converts any CPU index that can be converted to GPU
faiss::Index * index_cpu_to_gpu(
       GpuResources* resources, int device,
       const faiss::Index *index,
       const GpuClonerOptions *options = nullptr);

faiss::Index * index_cpu_to_gpu_multiple(
       std::vector<GpuResources*> & resources,
       std::vector<int> &devices,
       const faiss::Index *index,
       const GpuMultipleClonerOptions *options = nullptr);

/// parameter space and setters for GPU indexes
struct GpuParameterSpace: faiss::ParameterSpace {
    /// initialize with reasonable parameters for the index
    void initialize (const faiss::Index * index) override;

    /// set a combination of parameters on an index
    void set_index_parameter (
          faiss::Index * index, const std::string & name,
          double val) const override;
};

} } // namespace
