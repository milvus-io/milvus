/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "GpuIndex.h"

namespace faiss {

struct IndexFlat;
struct IndexFlatL2;
struct IndexFlatIP;

}

namespace faiss { namespace gpu {

struct FlatIndex;

struct GpuIndexFlatConfig : public GpuIndexConfig {
  inline GpuIndexFlatConfig()
      : useFloat16(false),
        useFloat16Accumulator(false),
        storeTransposed(false) {
  }

  /// Whether or not data is stored as float16
  bool useFloat16;

  /// Whether or not all math is performed in float16, if useFloat16 is
  /// specified. If true, we use cublasHgemm, supported only on CC
  /// 5.3+. Otherwise, we use cublasSgemmEx.
  bool useFloat16Accumulator;

  /// Whether or not data is stored (transparently) in a transposed
  /// layout, enabling use of the NN GEMM call, which is ~10% faster.
  /// This will improve the speed of the flat index, but will
  /// substantially slow down any add() calls made, as all data must
  /// be transposed, and will increase storage requirements (we store
  /// data in both transposed and non-transposed layouts).
  bool storeTransposed;
};

/// Wrapper around the GPU implementation that looks like
/// faiss::IndexFlat; copies over centroid data from a given
/// faiss::IndexFlat
class GpuIndexFlat : public GpuIndex {
 public:
  /// Construct from a pre-existing faiss::IndexFlat instance, copying
  /// data over to the given GPU
  GpuIndexFlat(GpuResources* resources,
               const faiss::IndexFlat* index,
               GpuIndexFlatConfig config = GpuIndexFlatConfig());

  /// Construct an empty instance that can be added to
  GpuIndexFlat(GpuResources* resources,
               int dims,
               faiss::MetricType metric,
               GpuIndexFlatConfig config = GpuIndexFlatConfig());

  ~GpuIndexFlat() override;

  /// Initialize ourselves from the given CPU index; will overwrite
  /// all data in ourselves
  void copyFrom(const faiss::IndexFlat* index);

  /// Copy ourselves to the given CPU index; will overwrite all data
  /// in the index instance
  void copyTo(faiss::IndexFlat* index) const;

  /// Returns the number of vectors we contain
  size_t getNumVecs() const;

  /// Clears all vectors from this index
  void reset() override;

  /// This index is not trained, so this does nothing
  void train(Index::idx_t n, const float* x) override;

  /// Overrides to avoid excessive copies
  void add(faiss::Index::idx_t, const float* x) override;

  /// Reconstruction methods; prefer the batch reconstruct as it will
  /// be more efficient
  void reconstruct(faiss::Index::idx_t key, float* out) const override;

  /// Batch reconstruction method
  void reconstruct_n(
      faiss::Index::idx_t i0,
      faiss::Index::idx_t num,
      float* out) const override;

  /// For internal access
  inline FlatIndex* getGpuData() { return data_; }

 protected:
  /// Flat index does not require IDs as there is no storage available for them
  bool addImplRequiresIDs_() const override;

  /// Called from GpuIndex for add
  void addImpl_(int n,
                const float* x,
                const Index::idx_t* ids) override;

  /// Called from GpuIndex for search
  void searchImpl_(int n,
                   const float* x,
                   int k,
                   float* distances,
                   faiss::Index::idx_t* labels) const override;

 private:
  /// Checks user settings for consistency
  void verifySettings_() const;

 protected:
  /// Our config object
  const GpuIndexFlatConfig config_;

  /// Holds our GPU data containing the list of vectors; is managed via raw
  /// pointer so as to allow non-CUDA compilers to see this header
  FlatIndex* data_;
};

/// Wrapper around the GPU implementation that looks like
/// faiss::IndexFlatL2; copies over centroid data from a given
/// faiss::IndexFlat
class GpuIndexFlatL2 : public GpuIndexFlat {
 public:
  /// Construct from a pre-existing faiss::IndexFlatL2 instance, copying
  /// data over to the given GPU
  GpuIndexFlatL2(GpuResources* resources,
                 faiss::IndexFlatL2* index,
                 GpuIndexFlatConfig config = GpuIndexFlatConfig());

  /// Construct an empty instance that can be added to
  GpuIndexFlatL2(GpuResources* resources,
                 int dims,
                 GpuIndexFlatConfig config = GpuIndexFlatConfig());

  /// Initialize ourselves from the given CPU index; will overwrite
  /// all data in ourselves
  void copyFrom(faiss::IndexFlatL2* index);

  /// Copy ourselves to the given CPU index; will overwrite all data
  /// in the index instance
  void copyTo(faiss::IndexFlatL2* index);
};

/// Wrapper around the GPU implementation that looks like
/// faiss::IndexFlatIP; copies over centroid data from a given
/// faiss::IndexFlat
class GpuIndexFlatIP : public GpuIndexFlat {
 public:
  /// Construct from a pre-existing faiss::IndexFlatIP instance, copying
  /// data over to the given GPU
  GpuIndexFlatIP(GpuResources* resources,
                 faiss::IndexFlatIP* index,
                 GpuIndexFlatConfig config = GpuIndexFlatConfig());

  /// Construct an empty instance that can be added to
  GpuIndexFlatIP(GpuResources* resources,
                 int dims,
                 GpuIndexFlatConfig config = GpuIndexFlatConfig());

  /// Initialize ourselves from the given CPU index; will overwrite
  /// all data in ourselves
  void copyFrom(faiss::IndexFlatIP* index);

  /// Copy ourselves to the given CPU index; will overwrite all data
  /// in the index instance
  void copyTo(faiss::IndexFlatIP* index);
};

} } // namespace
