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

#include "index/PackedIndexLoad.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/LoaderTestAccess.h"

namespace milvus::index::test {

// Use the real packed load transaction to inspect the cell's resource charge.
template <typename Loader, typename... Types>
auto
OpenV3Storage(const ReaderBackend& backend,
              const TestArtifactData& artifact,
              const BackendCaseMetadata& metadata = {}) {
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(artifact)));
    PackedIndexSource source{std::shared_ptr<storage::IndexEntryReader>(
        storage::IndexEntryReader::Open(input, input->Size()))};
    storage::LoadOptions options;
    options.enable_mmap = backend.MmapRequested();
    if (options.enable_mmap) {
        options.mmap_dir_path = std::filesystem::temp_directory_path().string();
    }
    options.params = backend.LoadParams(metadata);
    const auto finish = LoaderTestAccess::StorageFinisher<Loader, Types...>();
    using Result = decltype(folly::coro::blockingWait(
        finish(std::declval<IndexLoadPlan&>(), options, false)));
    return folly::coro::blockingWait(RunPackedIndexLoad<Result>(
        source, options, &LoaderTestAccess::Plan<Loader>, finish, nullptr));
}

}  // namespace milvus::index::test
