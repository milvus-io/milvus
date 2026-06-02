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

#include "storage/IndexEntryDirectStreamWriter.h"

#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <algorithm>
#include <utility>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"
#include "storage/Crc32cUtil.h"

namespace milvus::storage {

IndexEntryDirectStreamWriter::IndexEntryDirectStreamWriter(
    std::shared_ptr<milvus::OutputStream> output, size_t read_buf_size)
    : output_(std::move(output)), read_buf_(read_buf_size) {
    output_->Write(MILVUS_V3_MAGIC, MILVUS_V3_MAGIC_SIZE);
}

void
IndexEntryDirectStreamWriter::WriteEntry(const std::string& name,
                                         const void* data,
                                         size_t size) {
    AssertInfo(!finished_, "Cannot write after Finish() has been called");
    CheckDuplicateName(name);

    uint32_t crc = Crc32cValue(data, size);
    output_->Write(data, size);
    dir_entries_.push_back({name, current_offset_, size, crc});
    current_offset_ += size;
}

void
IndexEntryDirectStreamWriter::WriteEntry(const std::string& name,
                                         int fd,
                                         size_t size) {
    AssertInfo(!finished_, "Cannot write after Finish() has been called");
    CheckDuplicateName(name);

    size_t remaining = size;
    size_t entry_start = current_offset_;
    uint32_t crc = 0;

    while (remaining > 0) {
        size_t to_read = std::min(remaining, read_buf_.size());
        size_t chunk_read = 0;

        while (chunk_read < to_read) {
            ssize_t bytes_read =
                ::read(fd, read_buf_.data() + chunk_read, to_read - chunk_read);
            if (bytes_read == -1 && errno == EINTR) {
                continue;
            }
            AssertInfo(bytes_read > 0,
                       "Failed to read from file descriptor: {}",
                       strerror(errno));
            chunk_read += bytes_read;
        }

        crc = Crc32cUpdate(crc, read_buf_.data(), chunk_read);
        output_->Write(read_buf_.data(), chunk_read);
        current_offset_ += chunk_read;
        remaining -= chunk_read;
    }

    dir_entries_.push_back({name, entry_start, size, crc});
}

void
IndexEntryDirectStreamWriter::Finish() {
    AssertInfo(!finished_, "Finish() has already been called");

    // Write __meta__ entry as the last entry in Data Region
    std::string meta_str = meta_json_.dump();
    uint32_t meta_crc = Crc32cValue(meta_str.data(), meta_str.size());
    output_->Write(meta_str.data(), meta_str.size());
    dir_entries_.push_back({MILVUS_V3_META_ENTRY_NAME,
                            current_offset_,
                            meta_str.size(),
                            meta_crc});
    size_t meta_entry_size = meta_str.size();
    current_offset_ += meta_str.size();

    // Build Directory Table JSON (no "version" — version is in Footer)
    nlohmann::json dir_json;
    dir_json["entries"] = nlohmann::json::array();

    for (const auto& entry : dir_entries_) {
        dir_json["entries"].push_back({{"name", entry.name},
                                       {"offset", entry.offset},
                                       {"size", entry.size},
                                       {"crc32", Crc32cToHex(entry.crc32)}});
    }

    std::string dir_str = dir_json.dump();
    output_->Write(dir_str.data(), dir_str.size());

    // Write 32-byte Footer:
    // [2B version][22B reserved][4B meta_entry_size][4B directory_table_size]
    uint8_t footer[MILVUS_V3_FOOTER_SIZE] = {};
    uint16_t version = MILVUS_V3_FORMAT_VERSION;
    uint32_t meta_size_u32 = static_cast<uint32_t>(meta_entry_size);
    uint32_t dir_size_u32 = static_cast<uint32_t>(dir_str.size());

    std::memcpy(footer + 0, &version, sizeof(uint16_t));
    // bytes 2..23 are reserved (zero-filled, already zeroed)
    std::memcpy(footer + 24, &meta_size_u32, sizeof(uint32_t));
    std::memcpy(footer + 28, &dir_size_u32, sizeof(uint32_t));

    output_->Write(footer, MILVUS_V3_FOOTER_SIZE);

    total_bytes_written_ = MILVUS_V3_MAGIC_SIZE + current_offset_ +
                           dir_str.size() + MILVUS_V3_FOOTER_SIZE;

    output_->Close();
    finished_ = true;
}

}  // namespace milvus::storage
