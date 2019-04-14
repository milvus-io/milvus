/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "GpuIndexIVFFlat.h"
#include "../IndexFlat.h"
#include "../IndexIVFFlat.h"
#include "GpuIndexFlat.h"
#include "GpuResources.h"
#include "impl/IVFFlat.cuh"
#include "utils/CopyUtils.cuh"
#include "utils/DeviceUtils.h"
#include "utils/Float16.cuh"

#include <limits>

namespace faiss { namespace gpu {

GpuIndexIVFFlat::GpuIndexIVFFlat(GpuResources* resources,
                                 const faiss::IndexIVFFlat* index,
                                 GpuIndexIVFFlatConfig config) :
    GpuIndexIVF(resources,
                index->d,
                index->metric_type,
                index->nlist,
                config),
    ivfFlatConfig_(config),
    reserveMemoryVecs_(0),
    index_(nullptr) {
#ifndef FAISS_USE_FLOAT16
  FAISS_THROW_IF_NOT_MSG(!ivfFlatConfig_.useFloat16IVFStorage,
                     "float16 unsupported; need CUDA SDK >= 7.5");
#endif

  copyFrom(index);
}

GpuIndexIVFFlat::GpuIndexIVFFlat(GpuResources* resources,
                                 int dims,
                                 int nlist,
                                 faiss::MetricType metric,
                                 GpuIndexIVFFlatConfig config) :
    GpuIndexIVF(resources, dims, metric, nlist, config),
    ivfFlatConfig_(config),
    reserveMemoryVecs_(0),
    index_(nullptr) {

  // faiss::Index params
  this->is_trained = false;

#ifndef FAISS_USE_FLOAT16
  FAISS_THROW_IF_NOT_MSG(!ivfFlatConfig_.useFloat16IVFStorage,
                     "float16 unsupported; need CUDA SDK >= 7.5");
#endif

  // We haven't trained ourselves, so don't construct the IVFFlat
  // index yet
}

GpuIndexIVFFlat::~GpuIndexIVFFlat() {
  delete index_;
}

void
GpuIndexIVFFlat::reserveMemory(size_t numVecs) {
  reserveMemoryVecs_ = numVecs;
  if (index_) {
    index_->reserveMemory(numVecs);
  }
}

void
GpuIndexIVFFlat::copyFrom(const faiss::IndexIVFFlat* index) {
  DeviceScope scope(device_);

  GpuIndexIVF::copyFrom(index);

  // Clear out our old data
  delete index_;
  index_ = nullptr;

  // The other index might not be trained
  if (!index->is_trained) {
    return;
  }

  // Otherwise, we can populate ourselves from the other index
  this->is_trained = true;

  // Copy our lists as well
  index_ = new IVFFlat(resources_,
                       quantizer_->getGpuData(),
                       index->metric_type == faiss::METRIC_L2,
                       ivfFlatConfig_.useFloat16IVFStorage,
                       ivfFlatConfig_.indicesOptions,
                       memorySpace_);
  InvertedLists *ivf = index->invlists;

  for (size_t i = 0; i < ivf->nlist; ++i) {
    auto numVecs = ivf->list_size(i);

    // GPU index can only support max int entries per list
    FAISS_THROW_IF_NOT_FMT(numVecs <=
                       (size_t) std::numeric_limits<int>::max(),
                       "GPU inverted list can only support "
                       "%zu entries; %zu found",
                       (size_t) std::numeric_limits<int>::max(),
                       numVecs);

    index_->addCodeVectorsFromCpu(
             i, (const float*)(ivf->get_codes(i)),
             ivf->get_ids(i), numVecs);
  }
}

void
GpuIndexIVFFlat::copyTo(faiss::IndexIVFFlat* index) const {
  DeviceScope scope(device_);

  // We must have the indices in order to copy to ourselves
  FAISS_THROW_IF_NOT_MSG(ivfFlatConfig_.indicesOptions != INDICES_IVF,
                     "Cannot copy to CPU as GPU index doesn't retain "
                     "indices (INDICES_IVF)");

  GpuIndexIVF::copyTo(index);
  index->code_size = this->d * sizeof(float);

  InvertedLists *ivf = new ArrayInvertedLists(
      nlist_, index->code_size);

  index->replace_invlists(ivf, true);

  // Copy the inverted lists
  if (index_) {
    for (int i = 0; i < nlist_; ++i) {
      ivf->add_entries (
              i, index_->getListIndices(i).size(),
              index_->getListIndices(i).data(),
              (const uint8_t*)index_->getListVectors(i).data());
    }
  }
}

size_t
GpuIndexIVFFlat::reclaimMemory() {
  if (index_) {
    DeviceScope scope(device_);

    return index_->reclaimMemory();
  }

  return 0;
}

void
GpuIndexIVFFlat::reset() {
  if (index_) {
    DeviceScope scope(device_);

    index_->reset();
    this->ntotal = 0;
  } else {
    FAISS_ASSERT(this->ntotal == 0);
  }
}

void
GpuIndexIVFFlat::train(Index::idx_t n, const float* x) {
  DeviceScope scope(device_);

  if (this->is_trained) {
    FAISS_ASSERT(quantizer_->is_trained);
    FAISS_ASSERT(quantizer_->ntotal == nlist_);
    FAISS_ASSERT(index_);
    return;
  }

  FAISS_ASSERT(!index_);

  trainQuantizer_(n, x);

  // The quantizer is now trained; construct the IVF index
  index_ = new IVFFlat(resources_,
                       quantizer_->getGpuData(),
                       this->metric_type == faiss::METRIC_L2,
                       ivfFlatConfig_.useFloat16IVFStorage,
                       ivfFlatConfig_.indicesOptions,
                       memorySpace_);

  if (reserveMemoryVecs_) {
    index_->reserveMemory(reserveMemoryVecs_);
  }

  this->is_trained = true;
}

void
GpuIndexIVFFlat::addImpl_(int n,
                          const float* x,
                          const Index::idx_t* xids) {
  // Device is already set in GpuIndex::add
  FAISS_ASSERT(index_);
  FAISS_ASSERT(n > 0);

  // Data is already resident on the GPU
  Tensor<float, 2, true> data(const_cast<float*>(x), {n, (int) this->d});

  static_assert(sizeof(long) == sizeof(Index::idx_t), "size mismatch");
  Tensor<long, 1, true> labels(const_cast<long*>(xids), {n});

  // Not all vectors may be able to be added (some may contain NaNs etc)
  index_->classifyAndAddVectors(data, labels);

  // but keep the ntotal based on the total number of vectors that we attempted
  // to add
  ntotal += n;
}

void
GpuIndexIVFFlat::searchImpl_(int n,
                             const float* x,
                             int k,
                             float* distances,
                             Index::idx_t* labels) const {
  // Device is already set in GpuIndex::search
  FAISS_ASSERT(index_);
  FAISS_ASSERT(n > 0);

  // Data is already resident on the GPU
  Tensor<float, 2, true> queries(const_cast<float*>(x), {n, (int) this->d});
  Tensor<float, 2, true> outDistances(distances, {n, k});

  static_assert(sizeof(long) == sizeof(Index::idx_t), "size mismatch");
  Tensor<long, 2, true> outLabels(const_cast<long*>(labels), {n, k});

  index_->query(queries, nprobe_, k, outDistances, outLabels);
}


} } // namespace
