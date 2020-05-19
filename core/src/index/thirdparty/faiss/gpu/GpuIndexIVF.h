/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/GpuIndex.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndicesOptions.h>
#include <faiss/Clustering.h>

namespace faiss { struct IndexIVF; }

namespace faiss { namespace gpu {

class GpuIndexFlat;
class GpuResources;

struct GpuIndexIVFConfig : public GpuIndexConfig {
  inline GpuIndexIVFConfig()
      : indicesOptions(INDICES_64_BIT) {
  }

  /// Index storage options for the GPU
  IndicesOptions indicesOptions;

  /// Configuration for the coarse quantizer object
  GpuIndexFlatConfig flatConfig;
};

class GpuIndexIVF : public GpuIndex {
 public:
  GpuIndexIVF(GpuResources* resources,
              int dims,
              faiss::MetricType metric,
              float metricArg,
              int nlist,
              GpuIndexIVFConfig config = GpuIndexIVFConfig());

  ~GpuIndexIVF() override;

 private:
  /// Shared initialization functions
  void init_();

 public:
  /// Copy what we need from the CPU equivalent
  void copyFrom(const faiss::IndexIVF* index);

  /// Copy what we have to the CPU equivalent
  void copyTo(faiss::IndexIVF* index) const;

  /// Returns the number of inverted lists we're managing
  int getNumLists() const;

  /// Return the quantizer we're using
  GpuIndexFlat* getQuantizer();

  /// Sets the number of list probes per query
  void setNumProbes(int nprobe);

  /// Returns our current number of list probes per query
  int getNumProbes() const;

 protected:
  bool addImplRequiresIDs_() const override;
  void trainQuantizer_(faiss::Index::idx_t n, const float* x);

 public:
  /// Exposing this like the CPU version for manipulation
  ClusteringParameters cp;

  /// Exposing this like the CPU version for query
  int nlist;

  /// Exposing this like the CPU version for manipulation
  int nprobe;

  /// Exposeing this like the CPU version for query
  GpuIndexFlat* quantizer;

 protected:
  GpuIndexIVFConfig ivfConfig_;
};

} } // namespace
