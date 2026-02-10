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
#include <string>
#include <unordered_map>
#include <vector>

#include "filemanager/InputStream.h"
#include "storage/IndexEntryWriter.h"
#include "storage/ThreadPools.h"
#include "storage/plugin/PluginInterface.h"

namespace milvus::storage {

struct Entry {
    std::vector<uint8_t> data;
};

class IndexEntryReader {
 public:
    static std::unique_ptr<IndexEntryReader>
    Open(std::shared_ptr<milvus::InputStream> input,
         int64_t file_size,
         int64_t collection_id = 0,
         ThreadPoolPriority priority = ThreadPoolPriority::HIGH);

    std::vector<std::string>
    GetEntryNames() const;

    Entry
    ReadEntry(const std::string& name);

    void
    ReadEntryToFile(const std::string& name, const std::string& local_path);

    void
    ReadEntriesToFiles(const std::vector<std::pair<std::string, std::string>>&
                           name_path_pairs);

    IndexEntryReader(const IndexEntryReader&) = delete;
    IndexEntryReader&
    operator=(const IndexEntryReader&) = delete;

 private:
    struct PlainEntryMeta {
        uint64_t offset;
        uint64_t size;
    };

    struct EncryptedEntryMeta {
        uint64_t original_size;
        std::vector<SliceMeta> slices;
    };

    struct EntryMeta {
        bool encrypted;
        PlainEntryMeta plain;
        EncryptedEntryMeta enc;
    };

    IndexEntryReader() = default;

    void
    ReadFooterAndDirectory();
    void
    ValidateMagic();

    Entry
    ReadPlainEntry(const EntryMeta& meta);
    Entry
    ReadEncryptedEntry(const EntryMeta& meta);

    void
    WritePlainEntryToFile(const EntryMeta& meta, const std::string& local_path);
    void
    WriteEncryptedEntryToFile(const EntryMeta& meta,
                              const std::string& local_path);

    std::shared_ptr<milvus::InputStream> input_;
    int64_t file_size_ = 0;
    int64_t collection_id_ = 0;
    ThreadPoolPriority priority_ = ThreadPoolPriority::HIGH;

    bool is_encrypted_ = false;
    std::string edek_;
    int64_t ez_id_ = 0;
    size_t slice_size_ = 0;

    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin_;

    std::unordered_map<std::string, EntryMeta> entry_index_;
    std::vector<std::string> entry_names_;

    static constexpr size_t kSmallEntryCacheThreshold = 1 * 1024 * 1024;
    std::unordered_map<std::string, Entry> small_entry_cache_;
};

}  // namespace milvus::storage
