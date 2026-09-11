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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include "filemanager/InputStream.h"
#include "folly/CancellationToken.h"
#include "storage/IndexEntryWriter.h"

namespace milvus::storage {
struct EntryStreamLoadInfo {
    // Exact encrypted-stream task bounds derived from persisted V3 directory
    // slice metadata. Plaintext files leave both byte counts at zero.
    bool encrypted{false};
    size_t total_transient_bytes{0};
    size_t max_task_transient_bytes{0};
};
struct PlainIndexEntryMeta {
    uint64_t offset;
    uint64_t size;
    uint32_t crc32;
};

struct EncryptedIndexEntryMeta {
    uint64_t original_size;
    uint32_t crc32;
    std::vector<SliceMeta> slices;
};

struct IndexEntryMeta {
    bool encrypted;
    PlainIndexEntryMeta plain;
    EncryptedIndexEntryMeta enc;
};

// Format metadata only: no executor, budget, decoded-entry cache or load policy.
struct IndexEntryDirectory {
    bool is_encrypted_{false};
    std::string edek_;
    int64_t ez_id_{0};
    size_t slice_size_{0};
    std::unordered_map<std::string, IndexEntryMeta> entry_index_;
    EntryStreamLoadInfo stream_load_info_;
    std::vector<std::string> entry_names_;
};

// Validate footer bounds and return the directory byte count.
size_t
IndexEntryDirectorySize(std::span<const uint8_t> footer, int64_t file_size);

// Parse already-read directory bytes; no I/O or executor selection.
IndexEntryDirectory
ParseIndexEntryDirectory(std::span<const uint8_t> bytes);

// Reads the V3 directory using the caller's input stream; never schedules work.
IndexEntryDirectory
ReadIndexEntryDirectory(const std::shared_ptr<milvus::InputStream>& input,
                        int64_t file_size,
                        const folly::CancellationToken& token);

}  // namespace milvus::storage
