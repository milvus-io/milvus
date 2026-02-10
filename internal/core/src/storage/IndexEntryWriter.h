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
#include <string>
#include <unordered_set>
#include <vector>

#include "common/EasyAssert.h"

namespace milvus::storage {

constexpr char MILVUS_V3_MAGIC[] = "MVSIDXV3";
constexpr size_t MILVUS_V3_MAGIC_SIZE = 8;

struct DirectoryEntry {
    std::string name;
    // Offset within the Data Region (i.e., bytes after the 8-byte magic header).
    // To get the absolute file position, add MILVUS_V3_MAGIC_SIZE.
    uint64_t offset;
    uint64_t size;
};

struct SliceMeta {
    // Offset within the Data Region (i.e., bytes after the 8-byte magic header).
    // To get the absolute file position, add MILVUS_V3_MAGIC_SIZE.
    uint64_t offset;
    uint64_t size;
};

struct EncryptedDirectoryEntry {
    std::string name;
    uint64_t original_size;
    std::vector<SliceMeta> slices;
};

class IndexEntryWriter {
 public:
    virtual ~IndexEntryWriter() = default;
    virtual void
    WriteEntry(const std::string& name, const void* data, size_t size) = 0;
    virtual void
    WriteEntry(const std::string& name, int fd, size_t size) = 0;
    virtual void
    Finish() = 0;
    virtual size_t
    GetTotalBytesWritten() const = 0;

    IndexEntryWriter(const IndexEntryWriter&) = delete;
    IndexEntryWriter&
    operator=(const IndexEntryWriter&) = delete;

 protected:
    IndexEntryWriter() = default;

    void
    CheckDuplicateName(const std::string& name) {
        auto [it, inserted] = written_names_.insert(name);
        AssertInfo(inserted, "Duplicate entry name: {}", name);
    }

    std::unordered_set<std::string> written_names_;
};

}  // namespace milvus::storage
