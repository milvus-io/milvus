/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/impl/IVFBase.cuh>
#include <faiss/gpu/impl/GpuScalarQuantizer.cuh>

namespace faiss { namespace gpu {

class IVFFlat : public IVFBase {
 public:
  /// Construct from a quantizer that has elemen
  IVFFlat(GpuResources* resources,
          /// We do not own this reference
          FlatIndex* quantizer,
          faiss::MetricType metric,
          float metricArg,
          bool useResidual,
          /// Optional ScalarQuantizer
          faiss::ScalarQuantizer* scalarQ,
          IndicesOptions indicesOptions,
          MemorySpace space);

  ~IVFFlat() override;

  /// Add vectors to a specific list; the input data can be on the
  /// host or on our current device
  void addCodeVectorsFromCpu(int listId,
                             const unsigned char* vecs,
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

 private:
  /// Returns the size of our stored vectors, in bytes
  size_t getVectorMemorySize() const;

 private:
  /// Do we encode the residual from a coarse quantizer or not?
  bool useResidual_;

  /// Scalar quantizer for encoded vectors, if any
  std::unique_ptr<GpuScalarQuantizer> scalarQ_;
};

} } // namespace
