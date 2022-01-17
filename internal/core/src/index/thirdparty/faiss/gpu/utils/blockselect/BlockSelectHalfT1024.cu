/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/gpu/utils/blockselect/BlockSelectImpl.cuh>

namespace faiss { namespace gpu {

#ifdef FAISS_USE_FLOAT16
BLOCK_SELECT_IMPL(half, true, 1024, 8);
#endif

} } // namespace
