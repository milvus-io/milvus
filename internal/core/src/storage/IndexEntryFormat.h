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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include "filemanager/InputStream.h"
#include "folly/CancellationToken.h"
#include "storage/IndexEntryWriter.h"
#include "common/EasyAssert.h"

namespace milvus::storage {

inline constexpr size_t kIndexEntryTailReadBytes = 64 * 1024;

// File encryption header. Readers own it alongside their decryption resources.
struct IndexFileEncryption {
    std::string edek;
    int64_t ez_id;
};

struct PlainEntrySource {
    // Absolute offset in the packed V3 object.
    uint64_t remote_offset;
};
struct EncryptedSliceSource {
    uint64_t remote_offset;
    size_t remote_bytes;
    size_t plaintext_offset;
    size_t plaintext_bytes;
};
struct EncryptedEntrySource {
    std::vector<EncryptedSliceSource> slices;
};

// Describes an entry in the source file; does not own loaded entry data.
struct EntryMeta {
    std::string name;
    size_t plaintext_size;
    uint32_t expected_crc;
    std::variant<PlainEntrySource, EncryptedEntrySource> source;
};

// Validated entry locations, with absolute offsets. No I/O or load state.
class IndexEntryDirectory {
 public:
    const std::vector<EntryMeta>&
    Entries() const noexcept {
        return entries_;
    }

    const EntryMeta&
    At(std::string_view name) const;

    bool
    HasEntry(std::string_view name) const noexcept {
        const auto it =
            std::lower_bound(entries_.begin(),
                             entries_.end(),
                             name,
                             [](const auto& entry, std::string_view key) {
                                 return entry.name < key;
                             });
        return it != entries_.end() && it->name == name;
    }

 private:
    friend std::pair<IndexEntryDirectory, std::optional<IndexFileEncryption>>
        ParseIndexEntryDirectory(std::span<const uint8_t>, int64_t);

    std::vector<EntryMeta> entries_;
};

// Validate footer bounds and return the directory byte count.
size_t
IndexEntryDirectorySize(std::span<const uint8_t> footer, int64_t file_size);

// Parse already-read directory bytes; no I/O or executor selection.
std::pair<IndexEntryDirectory, std::optional<IndexFileEncryption>>
ParseIndexEntryDirectory(std::span<const uint8_t> bytes, int64_t file_size);

// Reads the V3 directory using the caller's input stream; never schedules work.
std::pair<IndexEntryDirectory, std::optional<IndexFileEncryption>>
ReadIndexEntryDirectory(const std::shared_ptr<milvus::InputStream>& input,
                        int64_t file_size,
                        const folly::CancellationToken& token);

}  // namespace milvus::storage
