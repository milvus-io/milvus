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
namespace {
size_t
EncryptedStreamBudgetBytes(size_t cipher_len, size_t plain_len) {
    AssertInfo(
        plain_len <= (std::numeric_limits<size_t>::max() / 2) &&
            cipher_len <= std::numeric_limits<size_t>::max() - 2 * plain_len,
        "Encrypted stream budget size overflow");
    return cipher_len + 2 * plain_len;
}

}  // namespace

IndexEntryDirectory
ReadIndexEntryDirectory(const std::shared_ptr<milvus::InputStream>& input,
                        int64_t file_size,
                        const folly::CancellationToken& token) {
    ThrowIfCancelled(token, "IndexEntryReader::ReadFooterAndDirectory");
    AssertInfo(file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE,
               "V3 index file is too small: {}",
               file_size);
    constexpr size_t kTailBufferSize = 64 * 1024UL;
    size_t tail_size =
        std::min(static_cast<size_t>(file_size), kTailBufferSize);
    size_t tail_offset = file_size - tail_size;

    std::vector<uint8_t> tail_data(tail_size);
    size_t bytes_read = input->ReadAt(tail_data.data(), tail_offset, tail_size);
    ThrowIfCancelled(token, "IndexEntryReader::ReadFooterAndDirectory");
    AssertInfo(bytes_read == tail_size, "Failed to read file tail");

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
        AssertInfo(additional_read == need_more,
                   "Failed to read additional directory data");

        milvus::fastmem::FastMemcpy(
            full_tail_data.data() + need_more, tail_data.data(), tail_size);

        tail_data = std::move(full_tail_data);
        tail_size = new_tail_size;
    }

    return ParseIndexEntryDirectory(std::span(tail_data).subspan(
        tail_size - MILVUS_V3_FOOTER_SIZE - dir_size, dir_size));
}

size_t
IndexEntryDirectorySize(std::span<const uint8_t> footer, int64_t file_size) {
    AssertInfo(file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE,
               "V3 index file is too small: {}",
               file_size);
    AssertInfo(footer.size() == MILVUS_V3_FOOTER_SIZE,
               "Invalid V3 footer size");
    uint16_t version;
    uint32_t meta_entry_size;
    uint32_t dir_size;

    milvus::fastmem::FastMemcpy(&version, footer.data() + 0, sizeof(uint16_t));
    milvus::fastmem::FastMemcpy(
        &meta_entry_size, footer.data() + 24, sizeof(uint32_t));
    milvus::fastmem::FastMemcpy(
        &dir_size, footer.data() + 28, sizeof(uint32_t));

    AssertInfo(version == MILVUS_V3_FORMAT_VERSION,
               "Unsupported V3 format version: {}",
               version);
    AssertInfo(dir_size > 0, "Directory table size is zero");
    AssertInfo(static_cast<size_t>(dir_size) + meta_entry_size +
                       MILVUS_V3_FOOTER_SIZE + MILVUS_V3_MAGIC_SIZE <=
                   static_cast<size_t>(file_size),
               "Directory table + meta entry + footer size exceeds file size");

    return dir_size;
}

IndexEntryDirectory
ParseIndexEntryDirectory(std::span<const uint8_t> bytes) {
    IndexEntryDirectory result;
    nlohmann::json dir_json;
    try {
        dir_json = nlohmann::json::parse(bytes.begin(), bytes.end());
    } catch (const nlohmann::json::parse_error& e) {
        AssertInfo(false,
                   "Failed to parse V3 index directory table JSON: {}",
                   e.what());
    }

    AssertInfo(dir_json.contains("entries"), "Directory table missing entries");

    if (dir_json.contains("__edek__")) {
        result.is_encrypted_ = true;
        result.stream_load_info_.encrypted = true;
        result.edek_ = dir_json["__edek__"].get<std::string>();
        result.ez_id_ = std::stoll(dir_json["__ez_id__"].get<std::string>());
        result.slice_size_ = dir_json["slice_size"].get<size_t>();
        AssertInfo(IsStreamSliceSizeAligned(result.slice_size_),
                   "Encrypted entry slice_size must be {}-byte aligned, got {}",
                   kStreamSliceAlignment,
                   result.slice_size_);

        for (const auto& entry : dir_json["entries"]) {
            IndexEntryMeta meta;
            meta.encrypted = true;
            meta.enc.original_size = entry["original_size"].get<uint64_t>();
            meta.enc.crc32 = Crc32cFromHex(entry["crc32"].get<std::string>());
            size_t output_offset = 0;
            for (const auto& s : entry["slices"]) {
                auto slice = SliceMeta{s["offset"].get<uint64_t>(),
                                       s["size"].get<uint64_t>()};
                meta.enc.slices.push_back(slice);

                AssertInfo(output_offset < meta.enc.original_size,
                           "Encrypted slice exceeds original entry size {}",
                           meta.enc.original_size);
                auto remaining =
                    static_cast<size_t>(meta.enc.original_size - output_offset);
                auto plain_len = std::min(remaining, result.slice_size_);
                auto task_transient_bytes = EncryptedStreamBudgetBytes(
                    static_cast<size_t>(slice.size), plain_len);
                result.stream_load_info_.total_transient_bytes = SaturatingAdd(
                    result.stream_load_info_.total_transient_bytes,
                    task_transient_bytes);
                result.stream_load_info_.max_task_transient_bytes =
                    std::max(result.stream_load_info_.max_task_transient_bytes,
                             task_transient_bytes);
                output_offset += plain_len;
            }
            AssertInfo(output_offset == meta.enc.original_size,
                       "Encrypted slices cover {} bytes, expected {}",
                       output_offset,
                       meta.enc.original_size);
            std::string name = entry["name"].get<std::string>();
            result.entry_names_.push_back(name);
            result.entry_index_.emplace(std::move(name), std::move(meta));
        }
    } else {
        result.is_encrypted_ = false;

        for (const auto& entry : dir_json["entries"]) {
            IndexEntryMeta meta;
            meta.encrypted = false;
            meta.plain.offset = entry["offset"].get<uint64_t>();
            meta.plain.size = entry["size"].get<uint64_t>();
            meta.plain.crc32 = Crc32cFromHex(entry["crc32"].get<std::string>());
            std::string name = entry["name"].get<std::string>();
            result.entry_names_.push_back(name);
            result.entry_index_.emplace(std::move(name), std::move(meta));
        }
    }
    return result;
}

}  // namespace milvus::storage
