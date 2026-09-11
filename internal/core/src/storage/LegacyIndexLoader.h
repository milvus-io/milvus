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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>

#include "filemanager/InputStream.h"
#include "storage/ChunkManager.h"
#include "milvus-storage/filesystem/fs.h"
#include "folly/CancellationToken.h"
#include "folly/coro/Task.h"
#include "pb/common.pb.h"

namespace milvus::storage {

// Opens an exact legacy object path. Local paths retain ChunkManager semantics;
// object-store paths use Arrow range I/O when a filesystem is available.
[[nodiscard]] std::shared_ptr<milvus::InputStream>
OpenLegacyIndexInput(const ChunkManagerPtr& chunk_manager,
                     const milvus_storage::ArrowFileSystemPtr& fs,
                     const std::string& remote_file);

// A snapshot of one immutable legacy index object's envelope. Byte counts are
// decoded payload bytes, remote file bytes, and estimated peak scratch bytes.
struct LegacyIndexFileInfo {
    size_t file_bytes{0};
    size_t payload_bytes{0};
    size_t payload_offset{0};
    size_t max_transient_bytes{0};
    bool raw_payload{false};
};

using LegacyIndexFileInfos =
    std::unordered_map<std::string, LegacyIndexFileInfo>;

[[nodiscard]] folly::coro::Task<std::shared_ptr<const LegacyIndexFileInfos>>
InspectLegacyIndexFilesAsync(std::span<const std::string> files,
                             const ChunkManagerPtr& chunk_manager,
                             const milvus_storage::ArrowFileSystemPtr& fs,
                             proto::common::LoadPriority priority,
                             folly::CancellationToken token = {});

// Reuse a translator's immutable envelope snapshot when available. Direct load
// callers without a snapshot inspect the object normally; no readers are cached.
[[nodiscard]] folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileAsync(
    const std::string& path,
    const ChunkManagerPtr& chunk_manager,
    const milvus_storage::ArrowFileSystemPtr& fs,
    const std::shared_ptr<const LegacyIndexFileInfos>& infos,
    proto::common::LoadPriority priority,
    folly::CancellationToken token = {});

struct LegacyIndexFile {
    std::string path;
    LegacyIndexFileInfo info;
};

enum class LegacyIndexConsumerOrder { Ordered, Unordered };

// The view is borrowed until the returned task completes. Consumers copy/write
// it into their own destination before returning; retaining the view is invalid.
// offset is within this file's decoded payload. Empty files produce one call.
using LegacyIndexConsumer = std::function<folly::coro::Task<void>(
    size_t offset, std::span<const uint8_t> bytes)>;

// Reads and validates the bounded descriptor/event metadata under admission.
// The input must remain alive and immutable throughout inspection and streaming.
[[nodiscard]] folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileAsync(milvus::InputStream& input,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token = {});

// Streams raw payloads in bounded ranges; encoded/encrypted objects retain one
// whole decode unit. Admission covers read, decode, and awaited consumption.
// Runs on the caller's executor and drains issued reads before cancellation.
[[nodiscard]] folly::coro::Task<void>
StreamLegacyIndexFileAsync(milvus::InputStream& input,
                           const LegacyIndexFileInfo& info,
                           const LegacyIndexConsumer& consume,
                           proto::common::LoadPriority priority,
                           folly::CancellationToken token = {});

// Concurrently streams the concatenated payloads in the supplied file order.
// Raw ranges and whole encoded objects share one bounded admission window.
// Ordered consumers are serialized; unordered consumers may overlap and must
// place disjoint ranges by offset. All issued work drains before return/throw.
// Files, source, and consumer must outlive the returned task.
[[nodiscard]] folly::coro::Task<void>
StreamLegacyIndexFilesAsync(
    std::span<const LegacyIndexFile> files,
    const ChunkManagerPtr& chunk_manager,
    const milvus_storage::ArrowFileSystemPtr& fs,
    const LegacyIndexConsumer& consume,
    proto::common::LoadPriority priority,
    folly::CancellationToken token = {},
    LegacyIndexConsumerOrder order = LegacyIndexConsumerOrder::Ordered);

}  // namespace milvus::storage
