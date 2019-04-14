/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "FlatIndex.cuh"
#include "Distance.cuh"
#include "L2Norm.cuh"
#include "../utils/CopyUtils.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/Transpose.cuh"

namespace faiss { namespace gpu {

FlatIndex::FlatIndex(GpuResources* res,
                     int dim,
                     bool l2Distance,
                     bool useFloat16,
                     bool useFloat16Accumulator,
                     bool storeTransposed,
                     MemorySpace space) :
    resources_(res),
    dim_(dim),
    useFloat16_(useFloat16),
    useFloat16Accumulator_(useFloat16Accumulator),
    storeTransposed_(storeTransposed),
    l2Distance_(l2Distance),
    space_(space),
    num_(0),
    rawData_(space) {
#ifndef FAISS_USE_FLOAT16
  FAISS_ASSERT(!useFloat16_);
#endif
}

bool
FlatIndex::getUseFloat16() const {
  return useFloat16_;
}

/// Returns the number of vectors we contain
int FlatIndex::getSize() const {
#ifdef FAISS_USE_FLOAT16
  if (useFloat16_) {
    return vectorsHalf_.getSize(0);
  }
#endif

  return vectors_.getSize(0);
}

int FlatIndex::getDim() const {
#ifdef FAISS_USE_FLOAT16
  if (useFloat16_) {
    return vectorsHalf_.getSize(1);
  }
#endif

  return vectors_.getSize(1);
}

void
FlatIndex::reserve(size_t numVecs, cudaStream_t stream) {
  if (useFloat16_) {
#ifdef FAISS_USE_FLOAT16
    rawData_.reserve(numVecs * dim_ * sizeof(half), stream);
#endif
  } else {
    rawData_.reserve(numVecs * dim_ * sizeof(float), stream);
  }
}

Tensor<float, 2, true>&
FlatIndex::getVectorsFloat32Ref() {
  return vectors_;
}

#ifdef FAISS_USE_FLOAT16
Tensor<half, 2, true>&
FlatIndex::getVectorsFloat16Ref() {
  return vectorsHalf_;
}
#endif

DeviceTensor<float, 2, true>
FlatIndex::getVectorsFloat32Copy(cudaStream_t stream) {
  return getVectorsFloat32Copy(0, num_, stream);
}

DeviceTensor<float, 2, true>
FlatIndex::getVectorsFloat32Copy(int from, int num, cudaStream_t stream) {
  DeviceTensor<float, 2, true> vecFloat32({num, dim_}, space_);

  if (useFloat16_) {
#ifdef FAISS_USE_FLOAT16
    runConvertToFloat32(vecFloat32.data(),
                        vectorsHalf_[from].data(),
                        num * dim_, stream);
#endif
  } else {
    vectors_.copyTo(vecFloat32, stream);
  }

  return vecFloat32;
}

void
FlatIndex::query(Tensor<float, 2, true>& input,
                 int k,
                 Tensor<float, 2, true>& outDistances,
                 Tensor<int, 2, true>& outIndices,
                 bool exactDistance) {
  auto stream = resources_->getDefaultStreamCurrentDevice();
  auto& mem = resources_->getMemoryManagerCurrentDevice();

  if (useFloat16_) {
    // We need to convert to float16
#ifdef FAISS_USE_FLOAT16
    auto inputHalf = toHalf<2>(resources_, stream, input);

    DeviceTensor<half, 2, true> outDistancesHalf(
      mem, {outDistances.getSize(0), outDistances.getSize(1)}, stream);

    query(inputHalf, k, outDistancesHalf, outIndices, exactDistance);

    if (exactDistance) {
      // Convert outDistances back
      fromHalf<2>(stream, outDistancesHalf, outDistances);
    }
#endif
  } else {
    if (l2Distance_) {
      runL2Distance(resources_,
                    vectors_,
                    storeTransposed_ ? &vectorsTransposed_ : nullptr,
                    &norms_,
                    input,
                    k,
                    outDistances,
                    outIndices,
                    // FIXME
                    !exactDistance);
    } else {
      runIPDistance(resources_,
                    vectors_,
                    storeTransposed_ ? &vectorsTransposed_ : nullptr,
                    input,
                    k,
                    outDistances,
                    outIndices);
    }
  }
}

#ifdef FAISS_USE_FLOAT16
void
FlatIndex::query(Tensor<half, 2, true>& input,
                 int k,
                 Tensor<half, 2, true>& outDistances,
                 Tensor<int, 2, true>& outIndices,
                 bool exactDistance) {
  FAISS_ASSERT(useFloat16_);

  if (l2Distance_) {
    runL2Distance(resources_,
                  vectorsHalf_,
                  storeTransposed_ ? &vectorsHalfTransposed_ : nullptr,
                  &normsHalf_,
                  input,
                  k,
                  outDistances,
                  outIndices,
                  useFloat16Accumulator_,
                  // FIXME
                  !exactDistance);
  } else {
    runIPDistance(resources_,
                  vectorsHalf_,
                  storeTransposed_ ? &vectorsHalfTransposed_ : nullptr,
                  input,
                  k,
                  outDistances,
                  outIndices,
                  useFloat16Accumulator_);
  }
}
#endif

void
FlatIndex::add(const float* data, int numVecs, cudaStream_t stream) {
  if (numVecs == 0) {
    return;
  }

  if (useFloat16_) {
#ifdef FAISS_USE_FLOAT16
    // Make sure that `data` is on our device; we'll run the
    // conversion on our device
    auto devData = toDevice<float, 2>(resources_,
                                      getCurrentDevice(),
                                      (float*) data,
                                      stream,
                                      {numVecs, dim_});

    auto devDataHalf = toHalf<2>(resources_, stream, devData);

    rawData_.append((char*) devDataHalf.data(),
                    devDataHalf.getSizeInBytes(),
                    stream,
                    true /* reserve exactly */);
#endif
  } else {
    rawData_.append((char*) data,
                    (size_t) dim_ * numVecs * sizeof(float),
                    stream,
                    true /* reserve exactly */);
  }

  num_ += numVecs;

  if (useFloat16_) {
#ifdef FAISS_USE_FLOAT16
    DeviceTensor<half, 2, true> vectorsHalf(
      (half*) rawData_.data(), {(int) num_, dim_}, space_);
    vectorsHalf_ = std::move(vectorsHalf);
#endif
  } else {
    DeviceTensor<float, 2, true> vectors(
      (float*) rawData_.data(), {(int) num_, dim_}, space_);
    vectors_ = std::move(vectors);
  }

  if (storeTransposed_) {
    if (useFloat16_) {
#ifdef FAISS_USE_FLOAT16
      vectorsHalfTransposed_ =
        std::move(DeviceTensor<half, 2, true>({dim_, (int) num_}, space_));
      runTransposeAny(vectorsHalf_, 0, 1, vectorsHalfTransposed_, stream);
#endif
    } else {
      vectorsTransposed_ =
        std::move(DeviceTensor<float, 2, true>({dim_, (int) num_}, space_));
      runTransposeAny(vectors_, 0, 1, vectorsTransposed_, stream);
    }
  }

  if (l2Distance_) {
    // Precompute L2 norms of our database
    if (useFloat16_) {
#ifdef FAISS_USE_FLOAT16
      DeviceTensor<half, 1, true> normsHalf({(int) num_}, space_);
      runL2Norm(vectorsHalf_, normsHalf, true, stream);
      normsHalf_ = std::move(normsHalf);
#endif
    } else {
      DeviceTensor<float, 1, true> norms({(int) num_}, space_);
      runL2Norm(vectors_, norms, true, stream);
      norms_ = std::move(norms);
    }
  }
}

void
FlatIndex::reset() {
  rawData_.clear();
  vectors_ = std::move(DeviceTensor<float, 2, true>());
  norms_ = std::move(DeviceTensor<float, 1, true>());
  num_ = 0;
}

} }
