/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "BinaryFlatIndex.cuh"
#include "BinaryDistance.cuh"
#include "../utils/DeviceUtils.h"
#include "../GpuResources.h"

namespace faiss { namespace gpu {

BinaryFlatIndex::BinaryFlatIndex(GpuResources* res,
                                 int dim,
                                 MemorySpace space) :
    resources_(res),
    dim_(dim),
    space_(space),
    num_(0),
    rawData_(space) {
  FAISS_ASSERT(dim % 8 == 0);
}

/// Returns the number of vectors we contain
int BinaryFlatIndex::getSize() const {
  return vectors_.getSize(0);
}

int BinaryFlatIndex::getDim() const {
  return vectors_.getSize(1) * 8;
}

void
BinaryFlatIndex::reserve(size_t numVecs, cudaStream_t stream) {
  rawData_.reserve(numVecs * (dim_ / 8) * sizeof(unsigned int), stream);
}

Tensor<unsigned char, 2, true>&
BinaryFlatIndex::getVectorsRef() {
  return vectors_;
}

void
BinaryFlatIndex::query(Tensor<unsigned char, 2, true>& input,
                       int k,
                       Tensor<int, 2, true>& outDistances,
                       Tensor<int, 2, true>& outIndices) {
  auto stream = resources_->getDefaultStreamCurrentDevice();

  runBinaryDistance(vectors_,
                    input,
                    outDistances,
                    outIndices,
                    k,
                    stream);
}

void
BinaryFlatIndex::add(const unsigned char* data,
                     int numVecs,
                     cudaStream_t stream) {
  if (numVecs == 0) {
    return;
  }

  rawData_.append((char*) data,
                  (size_t) (dim_ / 8) * numVecs * sizeof(unsigned char),
                  stream,
                  true /* reserve exactly */);

  num_ += numVecs;

  DeviceTensor<unsigned char, 2, true> vectors(
    (unsigned char*) rawData_.data(), {(int) num_, (dim_ / 8)}, space_);
  vectors_ = std::move(vectors);
}

void
BinaryFlatIndex::reset() {
  rawData_.clear();
  vectors_ = std::move(DeviceTensor<unsigned char, 2, true>());
  num_ = 0;
}

} }
