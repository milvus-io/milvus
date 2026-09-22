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

#include "index/contracts/Registry.h"
#include "index/IndexLoadInput.h"
#include <functional>
#include "storage/FileManager.h"

namespace milvus::index {

/** @brief Family payload decoding invoked within a legacy loading operation. */
using LegacyLoadFn = std::function<folly::coro::Task<IIndexReaderBasePtr>(
    storage::FileSource&, const storage::LoadOptions&, bool)>;

/**
 * @brief Execute family decoding with a per-call legacy source context.
 * @param input Shared source and pinned transport; no concurrent use is
 * allowed.
 * @param options Fixed family configuration; copied with context for this call.
 * @param load Family payload decoder; owns its targets until I/O and cleanup
 * finish.
 * @param context Borrowed only until task completion; may be null.
 * @return A reader after final cancellation checks.
 * @note Source context is reset on success and failure. Sync stays on the
 * caller; async orchestration uses the loading executor. Families offload
 * blocking file phases to the local-file executor.
 */
folly::coro::Task<IIndexReaderBasePtr>
RunLegacyLoad(LegacyIndexSource& input,
              const storage::LoadOptions& options,
              LegacyLoadFn load,
              milvus::OpContext* context);

}  // namespace milvus::index
