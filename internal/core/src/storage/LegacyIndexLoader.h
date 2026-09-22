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
#include <memory>
#include <span>
#include <string>
#include <unordered_map>

#include "filemanager/InputStream.h"
#include "folly/CancellationToken.h"
#include "folly/coro/Task.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/IndexEntryTarget.h"

namespace milvus::storage {

// Opens an exact legacy object path. Local paths retain ChunkManager semantics;
// object-store paths use Arrow range I/O when a filesystem is available.
[[nodiscard]] folly::coro::Task<std::shared_ptr<milvus::InputStream>>
OpenLegacyIndexInputAsync(
    const ChunkManagerPtr& chunk_manager,
    const milvus_storage::ArrowFileSystemPtr& fs,
    const std::string& remote_file,
    proto::common::LoadPriority priority = proto::common::LoadPriority::HIGH);

// A snapshot of one immutable legacy index object's envelope. Byte counts are
// decoded payload bytes, remote file bytes, and estimated peak scratch bytes.
struct LegacyIndexFileInfo {
    size_t file_bytes{0};
    size_t payload_bytes{0};
    size_t payload_offset{0};
    size_t max_transient_bytes{0};
    bool raw_payload{false};

    // Async counts all payload tasks, including aligned write copies, without
    // depending on current admission limits. HIGH/LOW reads the whole object;
    // its batch limit is applied by LegacyIndexLoadTransientBytes below.
    [[nodiscard]] size_t
    TotalTransientBytes(bool use_async_load = true) const;
};

/**
 * @brief Inspect a legacy envelope using synchronous ChunkManager range reads.
 * @note Shares validation and metadata admission with async inspection, but
 * performs I/O inline without depending on an executor. Does not read payloads.
 */
LegacyIndexFileInfo
InspectLegacyIndexFile(const ChunkManagerPtr& chunk_manager,
                       const std::string& path,
                       proto::common::LoadPriority priority,
                       folly::CancellationToken token = {});

// Async has no per-load window. HIGH/LOW retains its 128 MiB batch allowance;
// whole-object decode buffers can require additional scratch within that batch.
[[nodiscard]] size_t
LegacyIndexLoadTransientBytes(size_t total_bytes,
                              size_t max_file_bytes,
                              bool use_async_load);

using LegacyIndexFileInfos =
    std::unordered_map<std::string, LegacyIndexFileInfo>;

[[nodiscard]] folly::coro::Task<std::shared_ptr<const LegacyIndexFileInfos>>
InspectLegacyIndexFilesAsync(std::span<const std::string> files,
                             const ChunkManagerPtr& chunk_manager,
                             const milvus_storage::ArrowFileSystemPtr& fs,
                             proto::common::LoadPriority priority,
                             folly::CancellationToken token = {});

// Reuse a translator's immutable envelope snapshot when available. Direct load
// callers without a snapshot inspect the object normally; no readers are
// cached.
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

// Reads and validates the bounded descriptor/event metadata under admission.
// The input must remain alive and immutable throughout inspection and
// streaming.
[[nodiscard]] folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileAsync(milvus::InputStream& input,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token = {});

// Streams raw payloads in bounded ranges; encoded/encrypted objects retain one
// whole decode unit. Admission covers read, decode, and awaited consumption.
// Runs on the caller's executor and drains issued reads before cancellation.
// File targets must be prepared by the caller and remain alive until
// completion.
[[nodiscard]] folly::coro::Task<void>
StreamLegacyIndexFileAsync(milvus::InputStream& input,
                           const LegacyIndexFileInfo& info,
                           const EntryTarget& target,
                           proto::common::LoadPriority priority,
                           folly::CancellationToken token = {});

// Concurrently streams the concatenated payloads in the supplied file order.
// Raw ranges and whole encoded objects acquire global bytes/slots before
// dispatch. Destination ranges may be written concurrently at disjoint offsets.
// File targets must already be prepared. All issued work drains before
// return/throw. Files, source, and target must outlive the returned task.
[[nodiscard]] folly::coro::Task<void>
StreamLegacyIndexFilesAsync(std::span<const LegacyIndexFile> files,
                            const ChunkManagerPtr& chunk_manager,
                            const milvus_storage::ArrowFileSystemPtr& fs,
                            const EntryTarget& target,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token = {});

// Fill an existing packed-load target from legacy payloads. File targets are
// prepared and finished on LocalFileIOPool, but remain uncommitted until the
// caller finishes opening the engine. Issued writes retain slice admission.
folly::coro::Task<void>
ReadLegacyIndexFilesAsync(std::span<const LegacyIndexFile> files,
                          const ChunkManagerPtr& chunk_manager,
                          const milvus_storage::ArrowFileSystemPtr& fs,
                          const EntryTarget& target,
                          proto::common::LoadPriority priority,
                          folly::CancellationToken token = {});

}  // namespace milvus::storage
