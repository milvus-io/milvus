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

    output_->Write(data, size);
    dir_entries_.push_back({name, current_offset_, size});
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

        output_->Write(read_buf_.data(), chunk_read);
        current_offset_ += chunk_read;
        remaining -= chunk_read;
    }

    dir_entries_.push_back({name, entry_start, size});
}

void
IndexEntryDirectStreamWriter::Finish() {
    AssertInfo(!finished_, "Finish() has already been called");

    nlohmann::json dir_json;
    dir_json["version"] = 3;
    dir_json["entries"] = nlohmann::json::array();

    for (const auto& entry : dir_entries_) {
        dir_json["entries"].push_back({{"name", entry.name},
                                       {"offset", entry.offset},
                                       {"size", entry.size}});
    }

    std::string dir_str = dir_json.dump();
    output_->Write(dir_str.data(), dir_str.size());

    uint64_t dir_size = dir_str.size();
    output_->Write(&dir_size, sizeof(uint64_t));

    total_bytes_written_ = MILVUS_V3_MAGIC_SIZE + current_offset_ +
                           dir_str.size() + sizeof(uint64_t);

    output_->Close();
    finished_ = true;
}

}  // namespace milvus::storage
