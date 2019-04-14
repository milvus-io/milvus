/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda_runtime.h>
#include <time.h>

namespace faiss { namespace gpu {

/// Utility class for timing execution of a kernel
class KernelTimer {
 public:
  /// Constructor starts the timer and adds an event into the current
  /// device stream
  KernelTimer(cudaStream_t stream = 0);

  /// Destructor releases event resources
  ~KernelTimer();

  /// Adds a stop event then synchronizes on the stop event to get the
  /// actual GPU-side kernel timings for any kernels launched in the
  /// current stream. Returns the number of milliseconds elapsed.
  /// Can only be called once.
  float elapsedMilliseconds();

 private:
  cudaEvent_t startEvent_;
  cudaEvent_t stopEvent_;
  cudaStream_t stream_;
  bool valid_;
};

/// CPU wallclock elapsed timer
class CpuTimer {
 public:
  /// Creates and starts a new timer
  CpuTimer();

  /// Returns elapsed time in milliseconds
  float elapsedMilliseconds();

 private:
  struct timespec start_;
};

} } // namespace
