/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "GpuIndexIVF.h"
#include "../FaissAssert.h"
#include "../IndexFlat.h"
#include "../IndexIVF.h"
#include "GpuIndexFlat.h"
#include "utils/DeviceUtils.h"
#include "utils/Float16.cuh"

namespace faiss { namespace gpu {

GpuIndexIVF::GpuIndexIVF(GpuResources* resources,
                         int dims,
                         faiss::MetricType metric,
                         int nlist,
                         GpuIndexIVFConfig config) :
    GpuIndex(resources, dims, metric, config),
    ivfConfig_(std::move(config)),
    nlist_(nlist),
    nprobe_(1),
    quantizer_(nullptr) {
#ifndef FAISS_USE_FLOAT16
  FAISS_THROW_IF_NOT_MSG(!ivfConfig_.flatConfig.useFloat16 &&
                         !ivfConfig_.flatConfig.useFloat16Accumulator,
                         "float16 unsupported; need CUDA SDK >= 7.5");
#endif

  init_();
}

void
GpuIndexIVF::init_() {
  FAISS_ASSERT(nlist_ > 0);

  // Spherical by default if the metric is inner_product
  if (this->metric_type == faiss::METRIC_INNER_PRODUCT) {
    this->cp.spherical = true;
  }

  // here we set a low # iterations because this is typically used
  // for large clusterings
  this->cp.niter = 10;
  this->cp.verbose = this->verbose;

  if (!quantizer_) {
    // Construct an empty quantizer
    GpuIndexFlatConfig config = ivfConfig_.flatConfig;
    // FIXME: inherit our same device
    config.device = device_;

    if (this->metric_type == faiss::METRIC_L2) {
      quantizer_ = new GpuIndexFlatL2(resources_, this->d, config);
    } else if (this->metric_type == faiss::METRIC_INNER_PRODUCT) {
      quantizer_ = new GpuIndexFlatIP(resources_, this->d, config);
    } else {
      // unknown metric type
      FAISS_ASSERT_MSG(false, "unknown metric type");
    }
  }
}

GpuIndexIVF::~GpuIndexIVF() {
  delete quantizer_;
}

GpuIndexFlat*
GpuIndexIVF::getQuantizer() {
  return quantizer_;
}

void
GpuIndexIVF::copyFrom(const faiss::IndexIVF* index) {
  DeviceScope scope(device_);

  this->d = index->d;
  this->metric_type = index->metric_type;

  FAISS_ASSERT(index->nlist > 0);
  FAISS_THROW_IF_NOT_FMT(index->nlist <=
                     (faiss::Index::idx_t) std::numeric_limits<int>::max(),
                     "GPU index only supports %zu inverted lists",
                     (size_t) std::numeric_limits<int>::max());
  nlist_ = index->nlist;

  FAISS_THROW_IF_NOT_FMT(index->nprobe > 0 &&
                         index->nprobe <= getMaxKSelection(),
                         "GPU index only supports nprobe <= %zu; passed %zu",
                         (size_t) getMaxKSelection(),
                         index->nprobe);
  nprobe_ = index->nprobe;

  // The metric type may have changed as well, so we might have to
  // change our quantizer
  delete quantizer_;
  quantizer_ = nullptr;

  // Construct an empty quantizer
  GpuIndexFlatConfig config = ivfConfig_.flatConfig;
  // FIXME: inherit our same device
  config.device = device_;

  if (index->metric_type == faiss::METRIC_L2) {
    // FIXME: 2 different float16 options?
    quantizer_ = new GpuIndexFlatL2(resources_, this->d, config);
  } else if (index->metric_type == faiss::METRIC_INNER_PRODUCT) {
    // FIXME: 2 different float16 options?
    quantizer_ = new GpuIndexFlatIP(resources_, this->d, config);
  } else {
    // unknown metric type
    FAISS_ASSERT(false);
  }

  if (!index->is_trained) {
    this->is_trained = false;
    this->ntotal = 0;
    return;
  }

  // Otherwise, we can populate ourselves from the other index
  this->is_trained = true;

  // ntotal can exceed max int, but the number of vectors per inverted
  // list cannot exceed this. We check this in the subclasses.
  this->ntotal = index->ntotal;

  // Since we're trained, the quantizer must have data
  FAISS_ASSERT(index->quantizer->ntotal > 0);

  if (index->metric_type == faiss::METRIC_L2) {
    auto q = dynamic_cast<faiss::IndexFlatL2*>(index->quantizer);
    FAISS_ASSERT(q);

    quantizer_->copyFrom(q);
  } else if (index->metric_type == faiss::METRIC_INNER_PRODUCT) {
    auto q = dynamic_cast<faiss::IndexFlatIP*>(index->quantizer);
    FAISS_ASSERT(q);

    quantizer_->copyFrom(q);
  } else {
    // unknown metric type
    FAISS_ASSERT(false);
  }
}

void
GpuIndexIVF::copyTo(faiss::IndexIVF* index) const {
  DeviceScope scope(device_);

  //
  // Index information
  //
  index->ntotal = this->ntotal;
  index->d = this->d;
  index->metric_type = this->metric_type;
  index->is_trained = this->is_trained;

  //
  // IndexIVF information
  //
  index->nlist = nlist_;
  index->nprobe = nprobe_;

  // Construct and copy the appropriate quantizer
  faiss::IndexFlat* q = nullptr;

  if (this->metric_type == faiss::METRIC_L2) {
    q = new faiss::IndexFlatL2(this->d);

  } else if (this->metric_type == faiss::METRIC_INNER_PRODUCT) {
    q = new faiss::IndexFlatIP(this->d);

  } else {
    // unknown metric type
    FAISS_ASSERT(false);
  }

  FAISS_ASSERT(quantizer_);
  quantizer_->copyTo(q);

  if (index->own_fields) {
    delete index->quantizer;
  }

  index->quantizer = q;
  index->quantizer_trains_alone = 0;
  index->own_fields = true;
  index->cp = this->cp;
  index->maintain_direct_map = false;
  index->direct_map.clear();
}

int
GpuIndexIVF::getNumLists() const {
  return nlist_;
}

void
GpuIndexIVF::setNumProbes(int nprobe) {
  FAISS_THROW_IF_NOT_FMT(nprobe > 0 && nprobe <= getMaxKSelection(),
                         "GPU index only supports nprobe <= %d; passed %d",
                         getMaxKSelection(),
                         nprobe);
  nprobe_ = nprobe;
}

int
GpuIndexIVF::getNumProbes() const {
  return nprobe_;
}

bool
GpuIndexIVF::addImplRequiresIDs_() const {
  // All IVF indices have storage for IDs
  return true;
}

void
GpuIndexIVF::trainQuantizer_(faiss::Index::idx_t n, const float* x) {
  if (n == 0) {
    // nothing to do
    return;
  }

  if (quantizer_->is_trained && (quantizer_->ntotal == nlist_)) {
    if (this->verbose) {
      printf ("IVF quantizer does not need training.\n");
    }

    return;
  }

  if (this->verbose) {
    printf ("Training IVF quantizer on %ld vectors in %dD\n", n, d);
  }

  DeviceScope scope(device_);

  // leverage the CPU-side k-means code, which works for the GPU
  // flat index as well
  quantizer_->reset();
  Clustering clus(this->d, nlist_, this->cp);
  clus.verbose = verbose;
  clus.train(n, x, *quantizer_);
  quantizer_->is_trained = true;

  FAISS_ASSERT(quantizer_->ntotal == nlist_);
}

} } // namespace
