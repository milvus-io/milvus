/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "GpuClonerOptions.h"

namespace faiss { namespace gpu {

GpuClonerOptions::GpuClonerOptions()
    : indicesOptions(INDICES_64_BIT),
      useFloat16CoarseQuantizer(false),
      useFloat16(false),
      usePrecomputed(true),
      reserveVecs(0),
      storeTransposed(false),
      verbose(false) {
}

GpuMultipleClonerOptions::GpuMultipleClonerOptions()
    : shard(false),
      shard_type(1)
{
}

} } // namespace
