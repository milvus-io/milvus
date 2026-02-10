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
#include <deque>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "storage/IndexEntryWriter.h"
#include "storage/plugin/PluginInterface.h"
#include "storage/ThreadPools.h"
#include "milvus-storage/filesystem/fs.h"

namespace milvus::storage {

class IndexEntryEncryptedLocalWriter : public IndexEntryWriter {
 public:
    IndexEntryEncryptedLocalWriter(
        const std::string& remote_path,
        milvus_storage::ArrowFileSystemPtr fs,
        std::shared_ptr<plugin::ICipherPlugin> cipher_plugin,
        int64_t ez_id,
        int64_t collection_id,
        size_t slice_size = 16 * 1024 * 1024);
    ~IndexEntryEncryptedLocalWriter();

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
    void
    EncryptAndWriteSlices(const std::string& name,
                          uint64_t original_size,
                          const uint8_t* data,
                          size_t size);
    void
    UploadLocalFile();

    std::string remote_path_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin_;
    int64_t ez_id_;
    int64_t collection_id_;
    std::string edek_;
    size_t slice_size_;

    ThreadPool& pool_;
    std::string local_path_;
    int local_fd_ = -1;
    size_t current_offset_ = 0;
    size_t total_bytes_written_ = 0;
    bool finished_ = false;

    struct EncDirEntry {
        std::string name;
        uint64_t original_size;
        std::vector<SliceMeta> slices;
    };
    std::vector<EncDirEntry> dir_entries_;
};

}  // namespace milvus::storage
