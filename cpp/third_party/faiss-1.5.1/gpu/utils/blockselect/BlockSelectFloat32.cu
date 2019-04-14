/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "BlockSelectImpl.cuh"

namespace faiss { namespace gpu {

BLOCK_SELECT_IMPL(float, true, 32, 2);
BLOCK_SELECT_IMPL(float, false, 32, 2);

} } // namespace
