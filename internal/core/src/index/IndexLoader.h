// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "index/contracts/IIndex.h"

namespace milvus {
class OpContext;
}  // namespace milvus

namespace milvus::index {

struct IndexCacheConfig;

/**
 * @brief Family-internal contract for prepared state that can create index owners.
 *
 * A concrete family's Create entry owns family validation and metadata parsing.
 * The returned loader owns that family's prepared state; this interface
 * exposes no storage-format properties or partially completed source-opening
 * steps.
 * External loading consumers use LoaderEntry::Load instead of this interface.
 * @note Load consumes the prepared source once. A returned Index owns its cache
 * and reload source independently.
 */
class IndexLoader {
 public:
    virtual ~IndexLoader() = default;

    /**
     * @brief Consume the prepared source and create an index with its storage cache.
     * @param context Borrowed for this call only; nullptr uses default priority
     * and no request cancellation.
     * @param cache Required storage cache policy, borrowed for this call.
     * Construction consumes the prepared source, including on failure;
     * do not reuse the loader afterward.
     * @return An index owning its cache and reload source.
     * @note Synchronous warmup may perform I/O before returning. Recreate the
     * loader to retry construction after failure.
     */
    virtual IndexPtr
    Load(milvus::OpContext* context, const IndexCacheConfig& cache) = 0;
};

}  // namespace milvus::index
