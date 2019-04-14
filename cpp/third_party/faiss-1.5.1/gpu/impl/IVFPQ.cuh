/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "IVFBase.cuh"
#include "../utils/Float16.cuh"

namespace faiss { namespace gpu {

/// Implementing class for IVFPQ on the GPU
class IVFPQ : public IVFBase {
 public:
  IVFPQ(GpuResources* resources,
        /// We do not own this reference
        FlatIndex* quantizer,
        int numSubQuantizers,
        int bitsPerSubQuantizer,
        float* pqCentroidData,
        IndicesOptions indicesOptions,
        bool useFloat16LookupTables,
        MemorySpace space);

  /// Returns true if we support PQ in this size
  static bool isSupportedPQCodeLength(int size);

  /// For no precomputed codes, is this a supported sub-dimension
  /// size?
  /// FIXME: get MM implementation working again
  static bool isSupportedNoPrecomputedSubDimSize(int dims);

  ~IVFPQ() override;

  /// Enable or disable pre-computed codes
  void setPrecomputedCodes(bool enable);

  /// Adds a set of codes and indices to a list; the data can be
  /// resident on either the host or the device
  void addCodeVectorsFromCpu(int listId,
                             const void* codes,
                             const long* indices,
                             size_t numVecs);

  /// Calcuates the residual and quantizes the vectors, adding them to
  /// this index
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

  /// Return the list codes of a particular list back to the CPU
  std::vector<unsigned char> getListCodes(int listId) const;

  /// Returns our set of sub-quantizers of the form
  /// (sub q)(code id)(sub dim)
  Tensor<float, 3, true> getPQCentroids();

 private:
  /// Sets the current product quantizer centroids; the data can be
  /// resident on either the host or the device. It will be transposed
  /// into our preferred data layout
  /// Data must be a row-major, 3-d array of size
  /// (numSubQuantizers, numSubQuantizerCodes, dim / numSubQuantizers)
  void setPQCentroids_(float* data);

  /// Calculate precomputed residual distance information
  void precomputeCodes_();

  /// Runs kernels for scanning inverted lists with precomputed codes
  void runPQPrecomputedCodes_(Tensor<float, 2, true>& queries,
                              DeviceTensor<float, 2, true>& coarseDistances,
                              DeviceTensor<int, 2, true>& coarseIndices,
                              int k,
                              Tensor<float, 2, true>& outDistances,
                              Tensor<long, 2, true>& outIndices);

  /// Runs kernels for scanning inverted lists without precomputed codes
  void runPQNoPrecomputedCodes_(Tensor<float, 2, true>& queries,
                                DeviceTensor<float, 2, true>& coarseDistances,
                                DeviceTensor<int, 2, true>& coarseIndices,
                                int k,
                                Tensor<float, 2, true>& outDistances,
                                Tensor<long, 2, true>& outIndices);

 private:
  /// Number of sub-quantizers per vector
  const int numSubQuantizers_;

  /// Number of bits per sub-quantizer
  const int bitsPerSubQuantizer_;

  /// Number of per sub-quantizer codes (2^bits)
  const int numSubQuantizerCodes_;

  /// Number of dimensions per each sub-quantizer
  const int dimPerSubQuantizer_;

  /// Do we maintain precomputed terms and lookup tables in float16
  /// form?
  const bool useFloat16LookupTables_;

  /// On the GPU, we prefer different PQ centroid data layouts for
  /// different purposes.
  ///
  /// (sub q)(sub dim)(code id)
  DeviceTensor<float, 3, true> pqCentroidsInnermostCode_;

  /// (sub q)(code id)(sub dim)
  DeviceTensor<float, 3, true> pqCentroidsMiddleCode_;

  /// Are precomputed codes enabled? (additional factoring and
  /// precomputation of the residual distance, to reduce query-time work)
  bool precomputedCodes_;

  /// Precomputed term 2 in float form
  /// (centroid id)(sub q)(code id)
  DeviceTensor<float, 3, true> precomputedCode_;

#ifdef FAISS_USE_FLOAT16
  /// Precomputed term 2 in half form
  DeviceTensor<half, 3, true> precomputedCodeHalf_;
#endif
};

} } // namespace
