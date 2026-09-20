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

#include "index/IndexLoader.h"
#include "index/IndexLoadInput.h"

namespace milvus::index {

struct LoaderEntry;

/**
 * @brief Family metadata construction, invoked only within the opening scope.
 */
using CreateIndexLoaderFn = folly::coro::Task<std::unique_ptr<IndexLoader>> (*)(
    OpenedIndexInput, storage::LoadOptions);

/**
 * @brief Complete the storage-open and family-construction operation together.
 * @param request Source description and opening context, retained until
 * completion.
 * @param create Family-local constructor/metadata decoder. Receives shared
 * input ownership and fixed options with op_ctx cleared; opening cancellation
 * is inherited.
 * @pre create is a non-null family-local construction function.
 * @return A ready loader, after the input's opening context has been detached.
 * @note Context cleanup also runs when metadata validation or cancellation
 * fails. This operation does not allocate reader payload targets or perform
 * Load.
 */
folly::coro::Task<std::unique_ptr<IndexLoader>>
OpenIndexLoader(IndexOpenRequest request, CreateIndexLoaderFn create);

/**
 * @brief Open and load an index, returning a reader through an awaitable task.
 * @note Keeps the loader alive until Load, issued I/O and cleanup finish.
 * @pre entry and request.options.op_ctx outlive the returned task.
 * @note Async describes the awaitable interface; input selects the I/O mode.
 * This function adds no executor hop. Both phases inherit cancellation and
 * use the same caller context.
 */
folly::coro::Task<IIndexReaderBasePtr>
LoadIndexAsync(const LoaderEntry& entry, IndexOpenRequest request);

/**
 * @brief Blocking cache boundary for the complete Open/Load operation.
 * @note Sync mode stays on the caller thread. Async mode enters the configured
 * executor; callers that can await use LoadIndexAsync instead.
 */
IIndexReaderBasePtr
LoadIndex(const LoaderEntry& entry, IndexOpenRequest request);

}  // namespace milvus::index
