/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../utils/DeviceTensor.cuh"
#include "../utils/DeviceVector.cuh"
#include "../utils/MemorySpace.h"

namespace faiss { namespace gpu {

class GpuResources;

/// Holder of GPU resources for a particular flat index
class BinaryFlatIndex {
 public:
  BinaryFlatIndex(GpuResources* res,
                  int dim,
                  MemorySpace space);

  /// Returns the number of vectors we contain
  int getSize() const;

  int getDim() const;

  /// Reserve storage that can contain at least this many vectors
  void reserve(size_t numVecs, cudaStream_t stream);

  /// Returns a reference to our vectors currently in use
  Tensor<unsigned char, 2, true>& getVectorsRef();

  void query(Tensor<unsigned char, 2, true>& vecs,
             int k,
             Tensor<int, 2, true>& outDistances,
             Tensor<int, 2, true>& outIndices);

  /// Add vectors to ourselves; the pointer passed can be on the host
  /// or the device
  void add(const unsigned char* data, int numVecs, cudaStream_t stream);

  /// Free all storage
  void reset();

 private:
  /// Collection of GPU resources that we use
  GpuResources* resources_;

  /// Dimensionality of our vectors
  const int dim_;

  /// Memory space for our allocations
  MemorySpace space_;

  /// How many vectors we have
  int num_;

  /// The underlying expandable storage
  DeviceVector<char> rawData_;

  /// Vectors currently in rawData_
  DeviceTensor<unsigned char, 2, true> vectors_;
};

} } // namespace
