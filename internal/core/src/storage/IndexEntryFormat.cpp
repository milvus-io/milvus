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

#include "storage/IndexEntryFormat.h"
#include <algorithm>
#include <limits>
#include "common/Utils.h"
#include "common/FastMem.h"
#include "nlohmann/json.hpp"
#include "storage/Crc32cUtil.h"
#include "storage/EntryStreamUtils.h"

namespace milvus::storage {

const EntryMeta&
IndexEntryDirectory::At(std::string_view name) const {
    const auto it =
        std::lower_bound(entries_.begin(),
                         entries_.end(),
                         name,
                         [](const auto& entry, std::string_view key) {
                             return entry.name < key;
                         });
    if (it == entries_.end() || it->name != name) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Entry not found: {}", name);
    }
    return *it;
}

std::pair<IndexEntryDirectory, std::optional<IndexFileEncryption>>
ReadIndexEntryDirectory(const std::shared_ptr<milvus::InputStream>& input,
                        int64_t file_size,
                        const folly::CancellationToken& token) {
    ThrowIfCancelled(token, "IndexEntryReader::ReadFooterAndDirectory");
    if (!(file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "V3 index file is too small: {}",
                  file_size);
    }
    constexpr size_t kTailBufferSize = 64 * 1024UL;
    size_t tail_size =
        std::min(static_cast<size_t>(file_size), kTailBufferSize);
    size_t tail_offset = file_size - tail_size;

    std::vector<uint8_t> tail_data(tail_size);
    size_t bytes_read = input->ReadAt(tail_data.data(), tail_offset, tail_size);
    ThrowIfCancelled(token, "IndexEntryReader::ReadFooterAndDirectory");
    if (!(bytes_read == tail_size)) {
        ThrowInfo(ErrorCode::FileReadFailed, "Failed to read file tail");
    }

    const auto dir_size = IndexEntryDirectorySize(
        std::span(tail_data).last(MILVUS_V3_FOOTER_SIZE), file_size);

    // Check if the directory itself needs a second read. The meta entry is
    // loaded separately by Open() and is not needed for directory parsing.
    size_t needed = static_cast<size_t>(dir_size) + MILVUS_V3_FOOTER_SIZE;
    size_t available_before_footer = tail_size - MILVUS_V3_FOOTER_SIZE;

    if (static_cast<size_t>(dir_size) > available_before_footer) {
        size_t new_tail_size = needed;
        size_t new_tail_offset = file_size - new_tail_size;

        std::vector<uint8_t> full_tail_data(new_tail_size);
        size_t need_more = new_tail_size - tail_size;

        size_t additional_read =
            input->ReadAt(full_tail_data.data(), new_tail_offset, need_more);
        ThrowIfCancelled(token, "IndexEntryReader::ReadFooterAndDirectory");
        if (!(additional_read == need_more)) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "Failed to read additional directory data");
        }

        milvus::fastmem::FastMemcpy(
            full_tail_data.data() + need_more, tail_data.data(), tail_size);

        tail_data = std::move(full_tail_data);
        tail_size = new_tail_size;
    }

    return ParseIndexEntryDirectory(
        std::span(tail_data).subspan(
            tail_size - MILVUS_V3_FOOTER_SIZE - dir_size, dir_size),
        file_size);
}

size_t
IndexEntryDirectorySize(std::span<const uint8_t> footer, int64_t file_size) {
    if (!(file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "V3 index file is too small: {}",
                  file_size);
    }
    if (!(footer.size() == MILVUS_V3_FOOTER_SIZE)) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Invalid V3 footer size");
    }
    uint16_t version;
    uint32_t meta_entry_size;
    uint32_t dir_size;

    milvus::fastmem::FastMemcpy(&version, footer.data() + 0, sizeof(uint16_t));
    milvus::fastmem::FastMemcpy(
        &meta_entry_size, footer.data() + 24, sizeof(uint32_t));
    milvus::fastmem::FastMemcpy(
        &dir_size, footer.data() + 28, sizeof(uint32_t));

    if (!(version == MILVUS_V3_FORMAT_VERSION)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "Unsupported V3 format version: {}",
                  version);
    }
    if (!(dir_size > 0)) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Directory table size is zero");
    }
    if (!(static_cast<size_t>(dir_size) + meta_entry_size +
              MILVUS_V3_FOOTER_SIZE + MILVUS_V3_MAGIC_SIZE <=
          static_cast<size_t>(file_size))) {
        ThrowInfo(
            ErrorCode::DataFormatBroken,
            "Directory table + meta entry + footer size exceeds file size");
    }

    return dir_size;
}

std::pair<IndexEntryDirectory, std::optional<IndexFileEncryption>>
ParseIndexEntryDirectory(std::span<const uint8_t> bytes, int64_t file_size) {
    AssertInfo(file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE,
               "V3 index file is too small: {}",
               file_size);
    IndexEntryDirectory directory;
    std::optional<IndexFileEncryption> encryption;
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(bytes.begin(), bytes.end());
    } catch (const nlohmann::json::parse_error& e) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "Failed to parse V3 index directory table JSON: {}",
                  e.what());
    }
    if (!json.contains("entries")) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "Directory table missing entries");
    }
    size_t slice_size = 0;
    if (json.contains("__edek__")) {
        encryption = IndexFileEncryption{
            json["__edek__"].get<std::string>(),
            std::stoll(json["__ez_id__"].get<std::string>())};
        slice_size = json["slice_size"].get<size_t>();
        AssertInfo(IsStreamSliceSizeAligned(slice_size),
                   "Encrypted entry slice_size must be {}-byte aligned, got {}",
                   kStreamSliceAlignment,
                   slice_size);
    }
    const auto max_offset =
        static_cast<uint64_t>(file_size - MILVUS_V3_MAGIC_SIZE);
    const auto& entries = json["entries"];
    directory.entries_.reserve(entries.size());
    directory.entry_names_.reserve(entries.size());
    for (const auto& value : entries) {
        EntryMeta entry;
        entry.name = value["name"].get<std::string>();
        entry.expected_crc = Crc32cFromHex(value["crc32"].get<std::string>());
        if (encryption) {
            entry.plaintext_size = value["original_size"].get<uint64_t>();
            EncryptedEntrySource source;
            source.slices.reserve(value["slices"].size());
            size_t plaintext_offset = 0;
            for (const auto& slice : value["slices"]) {
                const auto offset = slice["offset"].get<uint64_t>();
                const auto size = slice["size"].get<uint64_t>();
                if (plaintext_offset >= entry.plaintext_size) {
                    ThrowInfo(ErrorCode::DataFormatBroken,
                              "Encrypted slice exceeds original entry size {}",
                              entry.plaintext_size);
                }
                AssertInfo(
                    size > 0 && offset <= max_offset &&
                        size <= max_offset - offset,
                    "Encrypted entry '{}' has an empty or out-of-bounds range",
                    entry.name);
                const auto plaintext_bytes = std::min(
                    slice_size, entry.plaintext_size - plaintext_offset);
                source.slices.push_back({MILVUS_V3_MAGIC_SIZE + offset,
                                         size,
                                         plaintext_offset,
                                         plaintext_bytes});
                plaintext_offset += plaintext_bytes;
            }
            if (plaintext_offset != entry.plaintext_size) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "Encrypted slices cover {} bytes, expected {}",
                          plaintext_offset,
                          entry.plaintext_size);
            }
            entry.source = std::move(source);
        } else {
            const auto offset = value["offset"].get<uint64_t>();
            entry.plaintext_size = value["size"].get<uint64_t>();
            AssertInfo(offset <= max_offset &&
                           entry.plaintext_size <= max_offset - offset,
                       "Entry '{}' range exceeds packed file",
                       entry.name);
            entry.source = PlainEntrySource{MILVUS_V3_MAGIC_SIZE + offset};
        }
        directory.entry_names_.push_back(entry.name);
        directory.entries_.push_back(std::move(entry));
    }
    std::sort(directory.entries_.begin(),
              directory.entries_.end(),
              [](const auto& a, const auto& b) { return a.name < b.name; });
    AssertInfo(std::adjacent_find(directory.entries_.begin(),
                                  directory.entries_.end(),
                                  [](const auto& a, const auto& b) {
                                      return a.name == b.name;
                                  }) == directory.entries_.end(),
               "Duplicate entries in V3 directory");
    return {std::move(directory), std::move(encryption)};
}
}  // namespace milvus::storage
