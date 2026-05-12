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
#include <memory>
#include <string>
#include <vector>

#include "storage/IndexEntryWriter.h"
#include "filemanager/OutputStream.h"

namespace milvus::storage {

class IndexEntryDirectStreamWriter : public IndexEntryWriter {
 public:
    explicit IndexEntryDirectStreamWriter(
        std::shared_ptr<milvus::OutputStream> output,
        size_t read_buf_size = 16 * 1024 * 1024);

    void
    WriteEntry(const std::string& name, const void* data, size_t size) override;
    void
    WriteEntry(const std::string& name, int fd, size_t size) override;
    void
    Finish() override;
    size_t
    GetTotalBytesWritten() const override {
        return total_bytes_written_;
    }

 private:
    std::shared_ptr<milvus::OutputStream> output_;
    std::vector<char> read_buf_;
    std::vector<DirectoryEntry> dir_entries_;
    size_t current_offset_ = 0;
    size_t total_bytes_written_ = 0;
    bool finished_ = false;
};

}  // namespace milvus::storage
