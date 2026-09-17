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

#include <cstdint>
#include <memory>
#include <span>
#include "nlohmann/json.hpp"
#include "filemanager/InputStream.h"
#include "folly/CancellationToken.h"
#include "folly/coro/Task.h"
#include "pb/common.pb.h"
#include "storage/IndexEntryFormat.h"
#include "storage/IndexLoadPlan.h"
#include "storage/plugin/PluginInterface.h"

namespace milvus::storage {

// The V3 async path owns its reader. No method delegates loading to IndexEntryReader.
// All calls run on the selected shared load executor; issued reads drain even
// when cancellation is requested, keeping caller-owned destinations alive.
class AsyncIndexEntryReader {
 public:
    // Parse the packed directory and metadata from an already-open stream.
    // Size() must be cached by the stream; no synchronous I/O is issued here.
    static folly::coro::Task<std::unique_ptr<AsyncIndexEntryReader>>
    Open(std::shared_ptr<milvus::InputStream> input,
         int64_t collection_id,
         proto::common::LoadPriority priority,
         folly::CancellationToken token = {});

    // Immutable entry layout, used for destination planning
    // and resource estimates. No I/O; the reference lives as long as this reader.
    const IndexEntryDirectory&
    Directory() const noexcept {
        return directory_;
    }

    // Index properties decoded from the metadata entry (not entry locations).
    const nlohmann::json&
    IndexMeta() const noexcept {
        return metadata_;
    }

    // Fill the entry targets on the shared async executor. Publish the first
    // failure before releasing admission, drain issued slices, and clean up
    // uncommitted files on failure. This reader must outlive the task.
    folly::coro::Task<IndexLoadArtifact>
    ReadEntriesAsync(std::vector<EntryLoadPlan> entries,
                     proto::common::LoadPriority priority,
                     folly::CancellationToken token = {});

 private:
    AsyncIndexEntryReader() = default;

    // Entry and slice come from the validated directory and BuildSlices(). The
    // caller owns the destination and admission lease through CRC and file-write completion.
    folly::coro::Task<void>
    ReadSliceIntoAsync(const EntryMeta& entry,
                       size_t slice_index,
                       uint64_t offset,
                       std::span<uint8_t> destination,
                       folly::CancellationToken token) const;

    folly::coro::Task<IndexLoadArtifact>
    ReadEntriesAsyncImpl(
        std::vector<EntryLoadPlan>& entries,
        proto::common::LoadPriority priority,
        const std::vector<std::shared_ptr<IndexFileTarget>>& cleanup_targets,
        folly::CancellationToken token);

    // Caller supplies a validated range and destination. Require a full read
    // and await actual IO completion, including after caller cancellation.
    folly::coro::Task<void>
    ReadExactlyAsync(uint64_t offset,
                     uint8_t* destination,
                     size_t bytes,
                     folly::CancellationToken token) const;

    std::shared_ptr<milvus::InputStream> input_;
    int64_t collection_id_{0};
    std::optional<IndexFileEncryption> encryption_;
    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin_;
    IndexEntryDirectory directory_;
    nlohmann::json metadata_;
};

}  // namespace milvus::storage
