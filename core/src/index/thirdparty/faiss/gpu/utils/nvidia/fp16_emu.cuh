/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// from Nvidia cuDNN library samples; modified to compile within faiss

#pragma once

namespace faiss { namespace gpu {

typedef struct __align__(2) {
   unsigned short x;
} half1;

half1 cpu_float2half_rn(float f);

float cpu_half2float(half1 h);

} } // namespace
