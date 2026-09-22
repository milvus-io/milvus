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

#include <string_view>
#include <utility>

#include "index/Families.h"
#include "index/IndexLoader.h"
#include "index/IndexLoadInput.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

/**
 * @brief Loads disk Knowhere readers from legacy artifacts.
 *
 * Non-streaming backends materialize a unique DiskFileManager generation.
 * Streaming backends open exact raw, unsliced inventory objects; legacy
 * sidecars still use FileSource. Owns the opened input and fixed load options;
 * each Load creates a new reader.
 */
class VectorDiskLoader final : public IndexLoader {
 public:
    static constexpr std::string_view kFamily = families::kVectorDisk;

    /** @brief Derive capabilities from runtime parameters without I/O. */
    static ReaderCaps
    DeriveCaps(const Config& index_meta);

    /**
     * @brief Validate legacy inventory, vector parameters and disk backend
     * options.
     * @param request Input/options; borrowed op_ctx must outlive this task.
     * @return A ready loader with no retained opening context.
     * @note Failure also detaches the source's opening context before the
     * exception propagates.
     */
    static folly::coro::Task<std::unique_ptr<IndexLoader>>
    Open(IndexOpenRequest request);

    /** @copydoc IndexLoader::Load */
    folly::coro::Task<IIndexReaderBasePtr>
    Load(milvus::OpContext* context = nullptr) override;

    /**
     * @brief Select entries that need eager async reads for the disk backend.
     * @return All entries for materialized backends; sidecars only for
     * streaming.
     * @note Selects the backend before inspecting remote engine envelopes.
     */
    static std::vector<std::string>
    AsyncEntryNames(storage::FileSource& source,
                    const storage::LoadOptions& opts);

 private:
    LegacyIndexSource input_;
    // Retained options never keep the Open caller's op_ctx.
    storage::LoadOptions options_;

    VectorDiskLoader(LegacyIndexSource input, storage::LoadOptions options)
        : input_(std::move(input)), options_(std::move(options)) {
    }

    /**
     * @brief Load disk-engine files through the selected backend and attach
     * sidecars.
     * @note Runs inside RunLegacyLoad; use_async controls source I/O only.
     */
    static folly::coro::Task<IIndexReaderBasePtr>
    LoadLegacy(storage::FileSource& source,
               const storage::LoadOptions& opts,
               bool use_async);
};

}  // namespace milvus::index
