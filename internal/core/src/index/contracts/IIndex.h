// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <optional>

#include "index/contracts/query/IIndexReaderBase.h"

namespace milvus {
class OpContext;
}

namespace milvus::index {

/** @brief Known facts of one version; an unknown count is distinct from zero. */
struct IndexMetadata {
    ReaderCaps caps;
    Domain domain{Domain::Row};
    DataType value_type{DataType::NONE};
    std::optional<int64_t> count;
};

/**
 * @brief Long-lived owner of an index version, independent of query contexts.
 * @note Resources may be directly owned or managed by a backend cache. This
 * contract does not impose a cell layout or expose query operations.
 */
class IIndex {
 public:
    virtual ~IIndex() = default;

    /** @return Already-known facts, without loading index data. */
    virtual IndexMetadata
    Metadata() const = 0;

    /**
     * @brief Create a request view without opening files or pinning data.
     * @param op_ctx Borrowed until the reader and its deferred operations finish;
     * nullptr denotes an explicit operation without a request context.
     * @return A new reader that retains this index version.
     * @pre The index has shared ownership.
     */
    virtual IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const = 0;

    /**
     * @brief Request background warmup cancellation; existing readers stay valid.
     * @note Does not cancel demand loads or wait for outstanding I/O to drain.
     */
    virtual void
    CancelWarmup() const = 0;
};

using IndexPtr = std::shared_ptr<IIndex>;

}  // namespace milvus::index
