/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "Timer.h"
#include "DeviceUtils.h"
#include "../../FaissAssert.h"

namespace faiss { namespace gpu {

KernelTimer::KernelTimer(cudaStream_t stream)
    : startEvent_(0),
      stopEvent_(0),
      stream_(stream),
      valid_(true) {
  CUDA_VERIFY(cudaEventCreate(&startEvent_));
  CUDA_VERIFY(cudaEventCreate(&stopEvent_));

  CUDA_VERIFY(cudaEventRecord(startEvent_, stream_));
}

KernelTimer::~KernelTimer() {
  CUDA_VERIFY(cudaEventDestroy(startEvent_));
  CUDA_VERIFY(cudaEventDestroy(stopEvent_));
}

float
KernelTimer::elapsedMilliseconds() {
  FAISS_ASSERT(valid_);

  CUDA_VERIFY(cudaEventRecord(stopEvent_, stream_));
  CUDA_VERIFY(cudaEventSynchronize(stopEvent_));

  auto time = 0.0f;
  CUDA_VERIFY(cudaEventElapsedTime(&time, startEvent_, stopEvent_));
  valid_ = false;

  return time;
}

CpuTimer::CpuTimer() {
  clock_gettime(CLOCK_REALTIME, &start_);
}

float
CpuTimer::elapsedMilliseconds() {
  struct timespec end;
  clock_gettime(CLOCK_REALTIME, &end);

  auto diffS = end.tv_sec - start_.tv_sec;
  auto diffNs = end.tv_nsec - start_.tv_nsec;

  return 1000.0f * (float) diffS + ((float) diffNs) / 1000000.0f;
}

} } // namespace
