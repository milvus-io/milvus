/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// from Nvidia cuDNN library samples; modified to compile within faiss

#include <faiss/gpu/utils/nvidia/fp16_emu.cuh>

namespace faiss { namespace gpu {

half1 cpu_float2half_rn(float f)
{
    half1 ret;
    ret.x = 0;
    
    /* remove API implementation */

    return ret;
}


float cpu_half2float(half1 h)
{
    /* remove API implementation */

    return 0.0;
}

} } // namespace
