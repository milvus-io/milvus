/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/IndexScalarQuantizer.h>

namespace faiss { namespace gpu {

class IVFFlat;
class GpuIndexFlat;

struct GpuIndexIVFScalarQuantizerConfig : public GpuIndexIVFConfig {
};

/// Wrapper around the GPU implementation that looks like
/// faiss::IndexIVFScalarQuantizer
class GpuIndexIVFScalarQuantizer : public GpuIndexIVF {
 public:
  /// Construct from a pre-existing faiss::IndexIVFScalarQuantizer instance,
  /// copying data over to the given GPU, if the input index is trained.
  GpuIndexIVFScalarQuantizer(
    GpuResources* resources,
    const faiss::IndexIVFScalarQuantizer* index,
    GpuIndexIVFScalarQuantizerConfig config =
    GpuIndexIVFScalarQuantizerConfig());

  /// Constructs a new instance with an empty flat quantizer; the user
  /// provides the number of lists desired.
  GpuIndexIVFScalarQuantizer(
    GpuResources* resources,
    int dims,
    int nlist,
    faiss::ScalarQuantizer::QuantizerType qtype,
    faiss::MetricType metric = MetricType::METRIC_L2,
    bool encodeResidual = true,
    GpuIndexIVFScalarQuantizerConfig config =
    GpuIndexIVFScalarQuantizerConfig());

  ~GpuIndexIVFScalarQuantizer() override;

  /// Reserve GPU memory in our inverted lists for this number of vectors
  void reserveMemory(size_t numVecs);

  /// Initialize ourselves from the given CPU index; will overwrite
  /// all data in ourselves
  void copyFrom(const faiss::IndexIVFScalarQuantizer* index);

  /// Copy ourselves to the given CPU index; will overwrite all data
  /// in the index instance
  void copyTo(faiss::IndexIVFScalarQuantizer* index) const;

  /// After adding vectors, one can call this to reclaim device memory
  /// to exactly the amount needed. Returns space reclaimed in bytes
  size_t reclaimMemory();

  void reset() override;

  void train(Index::idx_t n, const float* x) override;

 protected:
  /// Called from GpuIndex for add/add_with_ids
  void addImpl_(int n,
                const float* x,
                const Index::idx_t* ids) override;

  /// Called from GpuIndex for search
  void searchImpl_(int n,
                   const float* x,
                   int k,
                   float* distances,
                   Index::idx_t* labels) const override;

  /// Called from train to handle SQ residual training
  void trainResiduals_(Index::idx_t n, const float* x);

 public:
  /// Exposed like the CPU version
  faiss::ScalarQuantizer sq;

  /// Exposed like the CPU version
  bool by_residual;

 private:
  GpuIndexIVFScalarQuantizerConfig ivfSQConfig_;

  /// Desired inverted list memory reservation
  size_t reserveMemoryVecs_;

  /// Instance that we own; contains the inverted list
  IVFFlat* index_;
};

} } // namespace
