/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "../IndexBinaryFlat.h"
#include "GpuIndex.h"

namespace faiss { namespace gpu {

class BinaryFlatIndex;
class GpuResources;

struct GpuIndexBinaryFlatConfig : public GpuIndexConfig {
};

/// A GPU version of IndexBinaryFlat for brute-force comparison of bit vectors
/// via Hamming distance
class GpuIndexBinaryFlat : public IndexBinary {
 public:
  /// Construct from a pre-existing faiss::IndexBinaryFlat instance, copying
  /// data over to the given GPU
  GpuIndexBinaryFlat(GpuResources* resources,
                     const faiss::IndexBinaryFlat* index,
                     GpuIndexBinaryFlatConfig config =
                     GpuIndexBinaryFlatConfig());

  /// Construct an empty instance that can be added to
  GpuIndexBinaryFlat(GpuResources* resources,
                     int dims,
                     GpuIndexBinaryFlatConfig config =
                     GpuIndexBinaryFlatConfig());

  ~GpuIndexBinaryFlat() override;

  /// Initialize ourselves from the given CPU index; will overwrite
  /// all data in ourselves
  void copyFrom(const faiss::IndexBinaryFlat* index);

  /// Copy ourselves to the given CPU index; will overwrite all data
  /// in the index instance
  void copyTo(faiss::IndexBinaryFlat* index) const;

  void add(faiss::IndexBinary::idx_t n,
           const uint8_t* x) override;

  void reset() override;

  void search(faiss::IndexBinary::idx_t n,
              const uint8_t* x,
              faiss::IndexBinary::idx_t k,
              int32_t* distances,
              faiss::IndexBinary::idx_t* labels) const override;

  void reconstruct(faiss::IndexBinary::idx_t key,
                   uint8_t* recons) const override;

 protected:
  /// Called from search when the input data is on the CPU;
  /// potentially allows for pinned memory usage
  void searchFromCpuPaged_(int n,
                           const uint8_t* x,
                           int k,
                           int32_t* outDistancesData,
                           int* outIndicesData) const;

  void searchNonPaged_(int n,
                       const uint8_t* x,
                       int k,
                       int32_t* outDistancesData,
                       int* outIndicesData) const;

 protected:
  /// Manages streans, cuBLAS handles and scratch memory for devices
  GpuResources* resources_;

  /// Configuration options
  GpuIndexBinaryFlatConfig config_;

  /// Holds our GPU data containing the list of vectors; is managed via raw
  /// pointer so as to allow non-CUDA compilers to see this header
  BinaryFlatIndex* data_;
};

} } // namespace gpu
