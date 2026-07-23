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

#include <arrow/filesystem/filesystem.h>
#include <google/protobuf/repeated_ptr_field.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_reader.h"
#include "milvus-storage/properties.h"

namespace milvus::segcore {

inline constexpr size_t kTextLobIndexBuildBatchSize = 1024;

inline milvus_storage::lob_column::EncodedRef
MakeTextLobEncodedRef(const void* data, size_t size) {
    return {static_cast<const uint8_t*>(data), size};
}

using TextLobReaderFactory = std::function<
    arrow::Result<std::unique_ptr<milvus_storage::lob_column::LobColumnReader>>(
        std::shared_ptr<arrow::fs::FileSystem>,
        const milvus_storage::lob_column::LobColumnConfig&)>;

// A partition/field TEXT reader shared by every loaded segment that references
// the same LOB path. The reader is a runtime resource, not a capacity cache:
// its lifetime is determined exclusively by the shared_ptr owners held in
// segment/query state.
//
// LobColumnReader is not thread-safe, so all operations are serialized by the
// per-reader mutex.
struct SharedTextLobReader {
    SharedTextLobReader(std::string lob_base_path,
                        TextLobReaderFactory reader_factory)
        : lob_base_path(std::move(lob_base_path)),
          reader_factory(std::move(reader_factory)) {
    }

    const std::string lob_base_path;
    TextLobReaderFactory reader_factory;
    std::shared_ptr<milvus_storage::lob_column::LobColumnReader> reader;
    std::mutex mutex;

    void
    Open(std::shared_ptr<arrow::fs::FileSystem> fs,
         const milvus_storage::api::Properties& properties);

    std::string
    ReadText(std::shared_ptr<arrow::fs::FileSystem> fs,
             const milvus_storage::api::Properties& properties,
             const uint8_t* encoded_ref,
             size_t ref_size);

    std::vector<std::string>
    ReadBatch(std::shared_ptr<arrow::fs::FileSystem> fs,
              const milvus_storage::api::Properties& properties,
              const std::vector<milvus_storage::lob_column::EncodedRef>&
                  encoded_refs);

    void
    ReadBatchInto(
        std::shared_ptr<arrow::fs::FileSystem> fs,
        const milvus_storage::api::Properties& properties,
        const std::vector<milvus_storage::lob_column::EncodedRef>& encoded_refs,
        google::protobuf::RepeatedPtrField<std::string>* dst);

 private:
    std::shared_ptr<milvus_storage::lob_column::LobColumnReader>
    GetOrCreateReaderLocked(std::shared_ptr<arrow::fs::FileSystem> fs,
                            const milvus_storage::api::Properties& properties);
};

// TextLobReaderRegistry only deduplicates SharedTextLobReader objects by LOB
// path. It owns no readers and has no capacity or eviction policy: the map
// stores weak_ptr values, while loaded segment/runtime state owns every live
// reader. Expired registry entries are metadata and are pruned opportunistically.
//
// Thread safety: All methods are thread-safe
class TextLobReaderRegistry {
 public:
    TextLobReaderRegistry();
    explicit TextLobReaderRegistry(TextLobReaderFactory reader_factory);
    ~TextLobReaderRegistry() = default;

    // Non-copyable, non-movable
    TextLobReaderRegistry(const TextLobReaderRegistry&) = delete;
    TextLobReaderRegistry&
    operator=(const TextLobReaderRegistry&) = delete;
    TextLobReaderRegistry(TextLobReaderRegistry&&) = delete;
    TextLobReaderRegistry&
    operator=(TextLobReaderRegistry&&) = delete;

    // Return a deduplicated handle without opening storage files. This is used
    // by test injection; production loading should call Acquire().
    std::shared_ptr<SharedTextLobReader>
    AcquireHandle(const std::string& lob_base_path);

    // Acquire and open the shared reader for a loaded internal V3 TEXT column.
    std::shared_ptr<SharedTextLobReader>
    Acquire(const std::string& lob_base_path,
            std::shared_ptr<arrow::fs::FileSystem> fs,
            const milvus_storage::api::Properties& properties);

    size_t
    LiveReaderCountForTesting() const;

 private:
    void
    PruneExpiredReadersLocked() const;

    TextLobReaderFactory reader_factory_;

    // Key: lob_base_path
    // Value: weak reference to the segment/runtime-owned reader handle.
    mutable std::mutex registry_mutex_;
    mutable std::unordered_map<std::string, std::weak_ptr<SharedTextLobReader>>
        readers_;
    size_t next_expired_entry_cleanup_size_{1};
};

TextLobReaderRegistry&
GetGlobalTextLobReaderRegistry();

}  // namespace milvus::segcore
