/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "WarpSelectImpl.cuh"

namespace faiss { namespace gpu {

#ifdef FAISS_USE_FLOAT16
WARP_SELECT_IMPL(half, true, 128, 3);
WARP_SELECT_IMPL(half, false, 128, 3);
#endif

} } // namespace
