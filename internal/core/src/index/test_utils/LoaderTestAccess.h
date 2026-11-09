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

#include "index/IndexLoadPlan.h"
#include "storage/IndexEntryFormat.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index {

/**
 * @brief Test-only access to private packed planning and finalization steps.
 * @note Allows malformed-plan checks without reading the declared payload;
 * production callers use the complete source-open/create/load lifecycle.
 */
struct LoaderTestAccess {
    // Exercise the production storage finalizer without wrapping it in an Index.
    template <typename Loader, typename... Types>
    static auto
    StorageFinisher() {
        if constexpr (sizeof...(Types) == 0) {
            return &Loader::FinishPackedStorage;
        } else {
            return &Loader::template FinishPackedStorage<Types...>;
        }
    }

    template <typename Loader>
    static IndexLoadPlan
    Plan(const storage::IndexEntryDirectory& directory,
         const nlohmann::json& metadata,
         const storage::LoadOptions& options) {
        return Loader::PlanPacked(directory, metadata, options);
    }
};

}  // namespace milvus::index
