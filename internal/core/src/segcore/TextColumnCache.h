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
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>

#include "milvus-storage/common/lrucache.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/text_column/text_column_reader.h"
#include "milvus-storage/text_column/text_column_manager.h"

namespace milvus::segcore {

struct TextColumnCacheConfig {
    size_t max_file_readers = 64;  // Maximum number of cached file readers
};

struct TextColumnCacheStats {
    size_t file_cache_hits = 0;
    size_t file_cache_misses = 0;
    size_t current_file_cache_size = 0;
};

// TextColumnCache provides caching for TEXT column reading
//
// Caches open TextColumnReader instances (FileReaderCache):
//   - Always enabled
//   - Avoids repeated file open/close overhead
//   - Leverages reader's internal caching
//
// Thread safety: All methods are thread-safe
class TextColumnCache {
 public:
    explicit TextColumnCache(
        const TextColumnCacheConfig& config = TextColumnCacheConfig());
    ~TextColumnCache();

    // Non-copyable, non-movable
    TextColumnCache(const TextColumnCache&) = delete;
    TextColumnCache&
    operator=(const TextColumnCache&) = delete;
    TextColumnCache(TextColumnCache&&) = delete;
    TextColumnCache&
    operator=(TextColumnCache&&) = delete;

    // Get or create a TextColumnReader for the given lob_base_path
    //
    // Parameters:
    //   - lob_base_path: Base path for LOB files (e.g., {partition}/lobs/{field_id})
    //   - fs: Arrow filesystem to use
    //   - properties: Properties for the reader
    //
    // Returns: Shared pointer to TextColumnReader (may be shared across calls)
    std::shared_ptr<milvus_storage::text_column::TextColumnReader>
    GetOrCreateReader(const std::string& lob_base_path,
                      std::shared_ptr<arrow::fs::FileSystem> fs,
                      const milvus_storage::api::Properties& properties);

    std::string
    ReadText(const std::string& lob_base_path,
             std::shared_ptr<arrow::fs::FileSystem> fs,
             const milvus_storage::api::Properties& properties,
             const uint8_t* encoded_ref,
             size_t ref_size);

    std::vector<std::string>
    ReadBatch(const std::string& lob_base_path,
              std::shared_ptr<arrow::fs::FileSystem> fs,
              const milvus_storage::api::Properties& properties,
              const std::vector<milvus_storage::text_column::EncodedRef>&
                  encoded_refs);

    // Read multiple texts directly into destination (zero-copy optimization)
    // Parameters:
    //   - lob_base_path: Base path for LOB files
    //   - fs: Arrow filesystem
    //   - properties: Properties for the reader
    //   - encoded_refs: Vector of encoded references with sizes
    //   - dst: Destination to write decoded texts (must be pre-sized to encoded_refs.size())
    void
    ReadBatchInto(const std::string& lob_base_path,
                  std::shared_ptr<arrow::fs::FileSystem> fs,
                  const milvus_storage::api::Properties& properties,
                  const std::vector<milvus_storage::text_column::EncodedRef>&
                      encoded_refs,
                  google::protobuf::RepeatedPtrField<std::string>* dst);

    void
    Invalidate(const std::string& lob_base_path);

    void
    Clear();

    TextColumnCacheStats
    GetStats() const;

    const TextColumnCacheConfig&
    GetConfig() const {
        return config_;
    }

 private:
    TextColumnCacheConfig config_;

    // Key: lob_base_path
    // Value: TextColumnReader
    mutable std::mutex reader_cache_mutex_;
    milvus_storage::LRUCache<
        std::string,
        std::shared_ptr<milvus_storage::text_column::TextColumnReader>>
        reader_cache_;

    mutable std::atomic<size_t> file_cache_hits_{0};
    mutable std::atomic<size_t> file_cache_misses_{0};
};

TextColumnCache&
GetGlobalTextColumnCache();

void
InitGlobalTextColumnCache(const TextColumnCacheConfig& config);

}  // namespace milvus::segcore
