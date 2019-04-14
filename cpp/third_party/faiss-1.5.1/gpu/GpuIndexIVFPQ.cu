/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "GpuIndexIVFPQ.h"
#include "../IndexFlat.h"
#include "../IndexIVFPQ.h"
#include "../ProductQuantizer.h"
#include "GpuIndexFlat.h"
#include "GpuResources.h"
#include "impl/IVFPQ.cuh"
#include "utils/CopyUtils.cuh"
#include "utils/DeviceUtils.h"

#include <limits>

namespace faiss { namespace gpu {

GpuIndexIVFPQ::GpuIndexIVFPQ(GpuResources* resources,
                             const faiss::IndexIVFPQ* index,
                             GpuIndexIVFPQConfig config) :
    GpuIndexIVF(resources,
                index->d,
                index->metric_type,
                index->nlist,
                config),
    ivfpqConfig_(config),
    subQuantizers_(0),
    bitsPerCode_(0),
    reserveMemoryVecs_(0),
    index_(nullptr) {
#ifndef FAISS_USE_FLOAT16
  FAISS_ASSERT(!ivfpqConfig_.useFloat16LookupTables);
#endif

  copyFrom(index);
}

GpuIndexIVFPQ::GpuIndexIVFPQ(GpuResources* resources,
                             int dims,
                             int nlist,
                             int subQuantizers,
                             int bitsPerCode,
                             faiss::MetricType metric,
                             GpuIndexIVFPQConfig config) :
    GpuIndexIVF(resources,
                dims,
                metric,
                nlist,
                config),
    ivfpqConfig_(config),
    subQuantizers_(subQuantizers),
    bitsPerCode_(bitsPerCode),
    reserveMemoryVecs_(0),
    index_(nullptr) {
#ifndef FAISS_USE_FLOAT16
  FAISS_ASSERT(!config.useFloat16LookupTables);
#endif

  verifySettings_();

  // FIXME make IP work fully
  FAISS_ASSERT(this->metric_type == faiss::METRIC_L2);

  // We haven't trained ourselves, so don't construct the PQ index yet
  this->is_trained = false;
}

GpuIndexIVFPQ::~GpuIndexIVFPQ() {
  delete index_;
}

void
GpuIndexIVFPQ::copyFrom(const faiss::IndexIVFPQ* index) {
  DeviceScope scope(device_);

  // FIXME: support this
  FAISS_THROW_IF_NOT_MSG(index->metric_type == faiss::METRIC_L2,
                     "inner product unsupported");
  GpuIndexIVF::copyFrom(index);

  // Clear out our old data
  delete index_;
  index_ = nullptr;

  subQuantizers_ = index->pq.M;
  bitsPerCode_ = index->pq.nbits;

  // We only support this
  FAISS_ASSERT(index->pq.byte_per_idx == 1);
  FAISS_ASSERT(index->by_residual);
  FAISS_ASSERT(index->polysemous_ht == 0);

  verifySettings_();

  // The other index might not be trained
  if (!index->is_trained) {
    return;
  }

  // Otherwise, we can populate ourselves from the other index
  this->is_trained = true;

  // Copy our lists as well
  // The product quantizer must have data in it
  FAISS_ASSERT(index->pq.centroids.size() > 0);
  index_ = new IVFPQ(resources_,
                     quantizer_->getGpuData(),
                     subQuantizers_,
                     bitsPerCode_,
                     (float*) index->pq.centroids.data(),
                     ivfpqConfig_.indicesOptions,
                     ivfpqConfig_.useFloat16LookupTables,
                     memorySpace_);
  // Doesn't make sense to reserve memory here
  index_->setPrecomputedCodes(ivfpqConfig_.usePrecomputedTables);

  // Copy database vectors, if any
  const InvertedLists *ivf = index->invlists;
  size_t nlist = ivf ? ivf->nlist : 0;
  for (size_t i = 0; i < nlist; ++i) {
    size_t list_size = ivf->list_size(i);

    // GPU index can only support max int entries per list
    FAISS_THROW_IF_NOT_FMT(list_size <=
                       (size_t) std::numeric_limits<int>::max(),
                       "GPU inverted list can only support "
                       "%zu entries; %zu found",
                       (size_t) std::numeric_limits<int>::max(),
                       list_size);

    index_->addCodeVectorsFromCpu(
                       i, ivf->get_codes(i), ivf->get_ids(i), list_size);
  }
}

void
GpuIndexIVFPQ::copyTo(faiss::IndexIVFPQ* index) const {
  DeviceScope scope(device_);

  // We must have the indices in order to copy to ourselves
  FAISS_THROW_IF_NOT_MSG(ivfpqConfig_.indicesOptions != INDICES_IVF,
                     "Cannot copy to CPU as GPU index doesn't retain "
                     "indices (INDICES_IVF)");

  GpuIndexIVF::copyTo(index);

  //
  // IndexIVFPQ information
  //
  index->by_residual = true;
  index->use_precomputed_table = 0;
  index->code_size = subQuantizers_;
  index->pq = faiss::ProductQuantizer(this->d, subQuantizers_, bitsPerCode_);

  index->do_polysemous_training = false;
  index->polysemous_training = nullptr;

  index->scan_table_threshold = 0;
  index->max_codes = 0;
  index->polysemous_ht = 0;
  index->precomputed_table.clear();

  InvertedLists *ivf = new ArrayInvertedLists(
      nlist_, index->code_size);

  index->replace_invlists(ivf, true);

  if (index_) {
    // Copy the inverted lists
    for (int i = 0; i < nlist_; ++i) {
      auto ids = getListIndices(i);
      auto codes = getListCodes(i);
      index->invlists->add_entries (i, ids.size(), ids.data(), codes.data());
    }

    // Copy PQ centroids
    auto devPQCentroids = index_->getPQCentroids();
    index->pq.centroids.resize(devPQCentroids.numElements());

    fromDevice<float, 3>(devPQCentroids,
                         index->pq.centroids.data(),
                         resources_->getDefaultStream(device_));

    if (ivfpqConfig_.usePrecomputedTables) {
      index->precompute_table();
    }
  }
}

void
GpuIndexIVFPQ::reserveMemory(size_t numVecs) {
  reserveMemoryVecs_ = numVecs;
  if (index_) {
    DeviceScope scope(device_);
    index_->reserveMemory(numVecs);
  }
}

void
GpuIndexIVFPQ::setPrecomputedCodes(bool enable) {
  ivfpqConfig_.usePrecomputedTables = enable;
  if (index_) {
    DeviceScope scope(device_);
    index_->setPrecomputedCodes(enable);
  }

  verifySettings_();
}

bool
GpuIndexIVFPQ::getPrecomputedCodes() const {
  return ivfpqConfig_.usePrecomputedTables;
}

int
GpuIndexIVFPQ::getNumSubQuantizers() const {
  return subQuantizers_;
}

int
GpuIndexIVFPQ::getBitsPerCode() const {
  return bitsPerCode_;
}

int
GpuIndexIVFPQ::getCentroidsPerSubQuantizer() const {
  return utils::pow2(bitsPerCode_);
}

size_t
GpuIndexIVFPQ::reclaimMemory() {
  if (index_) {
    DeviceScope scope(device_);
    return index_->reclaimMemory();
  }

  return 0;
}

void
GpuIndexIVFPQ::reset() {
  if (index_) {
    DeviceScope scope(device_);

    index_->reset();
    this->ntotal = 0;
  } else {
    FAISS_ASSERT(this->ntotal == 0);
  }
}

void
GpuIndexIVFPQ::trainResidualQuantizer_(Index::idx_t n, const float* x) {
  // Code largely copied from faiss::IndexIVFPQ
  // FIXME: GPUize more of this
  n = std::min(n, (Index::idx_t) (1 << bitsPerCode_) * 64);

  if (this->verbose) {
    printf("computing residuals\n");
  }

  std::vector<Index::idx_t> assign(n);
  quantizer_->assign (n, x, assign.data());

  std::vector<float> residuals(n * d);

  for (idx_t i = 0; i < n; i++) {
    quantizer_->compute_residual(x + i * d, &residuals[i * d], assign[i]);
  }

  if (this->verbose) {
    printf("training %d x %d product quantizer on %ld vectors in %dD\n",
           subQuantizers_, getCentroidsPerSubQuantizer(), n, this->d);
  }

  // Just use the CPU product quantizer to determine sub-centroids
  faiss::ProductQuantizer pq(this->d, subQuantizers_, bitsPerCode_);
  pq.verbose = this->verbose;
  pq.train(n, residuals.data());

  index_ = new IVFPQ(resources_,
                     quantizer_->getGpuData(),
                     subQuantizers_,
                     bitsPerCode_,
                     pq.centroids.data(),
                     ivfpqConfig_.indicesOptions,
                     ivfpqConfig_.useFloat16LookupTables,
                     memorySpace_);
  if (reserveMemoryVecs_) {
    index_->reserveMemory(reserveMemoryVecs_);
  }

  index_->setPrecomputedCodes(ivfpqConfig_.usePrecomputedTables);
}

void
GpuIndexIVFPQ::train(Index::idx_t n, const float* x) {
  DeviceScope scope(device_);

  if (this->is_trained) {
    FAISS_ASSERT(quantizer_->is_trained);
    FAISS_ASSERT(quantizer_->ntotal == nlist_);
    FAISS_ASSERT(index_);
    return;
  }

  FAISS_ASSERT(!index_);

  trainQuantizer_(n, x);
  trainResidualQuantizer_(n, x);

  FAISS_ASSERT(index_);

  this->is_trained = true;
}

void
GpuIndexIVFPQ::addImpl_(int n,
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
GpuIndexIVFPQ::searchImpl_(int n,
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

int
GpuIndexIVFPQ::getListLength(int listId) const {
  FAISS_ASSERT(index_);
  return index_->getListLength(listId);
}

std::vector<unsigned char>
GpuIndexIVFPQ::getListCodes(int listId) const {
  FAISS_ASSERT(index_);
  DeviceScope scope(device_);

  return index_->getListCodes(listId);
}

std::vector<long>
GpuIndexIVFPQ::getListIndices(int listId) const {
  FAISS_ASSERT(index_);
  DeviceScope scope(device_);

  return index_->getListIndices(listId);
}

void
GpuIndexIVFPQ::verifySettings_() const {
  // Our implementation has these restrictions:

  // Must have some number of lists
  FAISS_THROW_IF_NOT_MSG(nlist_ > 0, "nlist must be >0");

  // up to a single byte per code
  FAISS_THROW_IF_NOT_FMT(bitsPerCode_ <= 8,
                     "Bits per code must be <= 8 (passed %d)", bitsPerCode_);

  // Sub-quantizers must evenly divide dimensions available
  FAISS_THROW_IF_NOT_FMT(this->d % subQuantizers_ == 0,
                     "Number of sub-quantizers (%d) must be an "
                     "even divisor of the number of dimensions (%d)",
                     subQuantizers_, this->d);

  // The number of bytes per encoded vector must be one we support
  FAISS_THROW_IF_NOT_FMT(IVFPQ::isSupportedPQCodeLength(subQuantizers_),
                     "Number of bytes per encoded vector / sub-quantizers (%d) "
                     "is not supported",
                     subQuantizers_);

  // We must have enough shared memory on the current device to store
  // our lookup distances
  int lookupTableSize = sizeof(float);
#ifdef FAISS_USE_FLOAT16
  if (ivfpqConfig_.useFloat16LookupTables) {
    lookupTableSize = sizeof(half);
  }
#endif

  // 64 bytes per code is only supported with usage of float16, at 2^8
  // codes per subquantizer
  size_t requiredSmemSize =
    lookupTableSize * subQuantizers_ * utils::pow2(bitsPerCode_);
  size_t smemPerBlock = getMaxSharedMemPerBlock(device_);

  FAISS_THROW_IF_NOT_FMT(requiredSmemSize
                     <= getMaxSharedMemPerBlock(device_),
                     "Device %d has %zu bytes of shared memory, while "
                     "%d bits per code and %d sub-quantizers requires %zu "
                     "bytes. Consider useFloat16LookupTables and/or "
                     "reduce parameters",
                     device_, smemPerBlock, bitsPerCode_, subQuantizers_,
                     requiredSmemSize);

  // If precomputed codes are disabled, we have an extra limitation in
  // terms of the number of dimensions per subquantizer
  FAISS_THROW_IF_NOT_FMT(ivfpqConfig_.usePrecomputedTables ||
                     IVFPQ::isSupportedNoPrecomputedSubDimSize(
                       this->d / subQuantizers_),
                     "Number of dimensions per sub-quantizer (%d) "
                     "is not currently supported without precomputed codes. "
                     "Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims "
                     "per sub-quantizer are currently supported with no "
                     "precomputed codes. "
                     "Precomputed codes supports any number of dimensions, but "
                     "will involve memory overheads.",
                     this->d / subQuantizers_);

  // TODO: fully implement METRIC_INNER_PRODUCT
  FAISS_THROW_IF_NOT_MSG(this->metric_type == faiss::METRIC_L2,
                     "METRIC_INNER_PRODUCT is currently unsupported");
}

} } // namespace
