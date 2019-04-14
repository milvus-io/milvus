/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "IVFBase.cuh"

namespace faiss { namespace gpu {

class IVFFlat : public IVFBase {
 public:
  /// Construct from a quantizer that has elemen
  IVFFlat(GpuResources* resources,
          /// We do not own this reference
          FlatIndex* quantizer,
          bool l2Distance,
          bool useFloat16,
          IndicesOptions indicesOptions,
          MemorySpace space);

  ~IVFFlat() override;

  /// Add vectors to a specific list; the input data can be on the
  /// host or on our current device
  void addCodeVectorsFromCpu(int listId,
                             const float* vecs,
                             const long* indices,
                             size_t numVecs);

  /// Adds the given vectors to this index.
  /// The input data must be on our current device.
  /// Returns the number of vectors successfully added. Vectors may
  /// not be able to be added because they contain NaNs.
  int classifyAndAddVectors(Tensor<float, 2, true>& vecs,
                            Tensor<long, 1, true>& indices);

  /// Find the approximate k nearest neigbors for `queries` against
  /// our database
  void query(Tensor<float, 2, true>& queries,
             int nprobe,
             int k,
             Tensor<float, 2, true>& outDistances,
             Tensor<long, 2, true>& outIndices);

  /// Return the vectors of a particular list back to the CPU
  std::vector<float> getListVectors(int listId) const;

 private:
  /// Returns the size of our stored vectors, in bytes
  size_t getVectorMemorySize() const;

 private:
  /// Calculating L2 distance or inner product?
  const bool l2Distance_;

  /// Do we store data internally as float16 (versus float32)?
  const bool useFloat16_;
};

} } // namespace
