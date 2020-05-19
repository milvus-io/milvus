/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/gpu/GpuClonerOptions.h>

namespace faiss { namespace gpu {

GpuClonerOptions::GpuClonerOptions()
    : indicesOptions(INDICES_64_BIT),
      useFloat16CoarseQuantizer(false),
      useFloat16(false),
      usePrecomputed(false),
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
