/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/gpu/utils/warpselect/WarpSelectImpl.cuh>

namespace faiss { namespace gpu {

#ifdef FAISS_USE_FLOAT16
WARP_SELECT_IMPL(half, false, 1024, 8);
#endif

} } // namespace
