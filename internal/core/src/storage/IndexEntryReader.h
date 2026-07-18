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
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "folly/CancellationToken.h"
#include "common/EasyAssert.h"
#include "filemanager/InputStream.h"
#include "nlohmann/json.hpp"
#include "storage/FileWriter.h"
#include "storage/IndexEntryWriter.h"
#include "storage/ThreadPools.h"
#include "storage/plugin/PluginInterface.h"

namespace milvus::storage {

size_t
DefaultEntryStreamSliceSize();

struct Entry {
    std::vector<uint8_t> data;
};

struct EntryStreamLoadInfo {
    // Exact encrypted-stream task bounds derived from persisted V3 directory
    // slice metadata. Plaintext files leave both byte counts at zero.
    bool encrypted{false};
    size_t total_transient_bytes{0};
    size_t max_task_transient_bytes{0};
};

class IndexEntryReader {
 public:
    static EntryStreamLoadInfo
    InspectStreamLoadInfo(std::shared_ptr<milvus::InputStream> input,
                          int64_t file_size,
                          folly::CancellationToken cancellation_token =
                              folly::CancellationToken());

    static std::unique_ptr<IndexEntryReader>
    Open(std::shared_ptr<milvus::InputStream> input,
         int64_t file_size,
         int64_t collection_id = 0,
         ThreadPoolPriority priority = ThreadPoolPriority::HIGH,
         folly::CancellationToken cancellation_token =
             folly::CancellationToken());

    const EntryStreamLoadInfo&
    GetStreamLoadInfo() const {
        return stream_load_info_;
    }

    std::vector<std::string>
    GetEntryNames() const;

    Entry
    ReadEntry(const std::string& name);

    void
    ReadEntryToFile(const std::string& name, const std::string& local_path);

    void
    ReadEntriesToFiles(const std::vector<std::pair<std::string, std::string>>&
                           name_path_pairs);

    void
    ReadEntryStreamToFile(const std::string& name,
                          const std::string& local_path,
                          io::Priority write_priority = io::Priority::MIDDLE);

    void
    ReadEntriesStreamToFiles(
        const std::vector<std::pair<std::string, std::string>>& name_path_pairs,
        io::Priority write_priority = io::Priority::MIDDLE);

    /// Stream entry data via transient memory budget.
    /// Downloads are concurrent (full bandwidth). Slices are delivered in entry
    /// order to `slice_consumer`; the data pointer is valid only for the
    /// duration of that call. Global TransientMemoryBudget controls total
    /// inflight slice bytes across all concurrent entry streams.
    /// CRC32c is verified incrementally. Consumers may receive partial data
    /// before a later slice error is reported. Slow consumers keep their slice
    /// budget until the callback returns, which can block other streams.
    /// Plain entries are split into `slice_size` byte slices. Encrypted entries
    /// ignore `slice_size` and use the slice boundaries stored in the V3
    /// directory.
    void
    ReadEntryStream(
        const std::string& name,
        std::function<void(const uint8_t* data, size_t len)> slice_consumer,
        size_t slice_size = DefaultEntryStreamSliceSize());

    /// Return the uncompressed data size of an entry without reading it.
    size_t
    GetEntrySize(const std::string& name) const;

    /// Check if an entry exists in the index file.
    bool
    HasEntry(const std::string& name) const {
        return entry_index_.find(name) != entry_index_.end();
    }

    template <typename T>
    T
    GetMeta(const std::string& key) const {
        AssertInfo(meta_json_.contains(key), "Meta key not found: {}", key);
        return meta_json_[key].get<T>();
    }

    template <typename T>
    T
    GetMeta(const std::string& key, const T& default_value) const {
        if (!meta_json_.contains(key)) {
            return default_value;
        }
        return meta_json_[key].get<T>();
    }

    bool
    HasMeta(const std::string& key) const {
        return meta_json_.contains(key);
    }

    IndexEntryReader(const IndexEntryReader&) = delete;
    IndexEntryReader&
    operator=(const IndexEntryReader&) = delete;

 private:
    struct PlainEntryMeta {
        uint64_t offset;
        uint64_t size;
        uint32_t crc32;
    };

    struct EncryptedEntryMeta {
        uint64_t original_size;
        uint32_t crc32;
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
    void
    CheckCancelled(const std::string& operation) const;

    Entry
    ReadPlainEntry(const EntryMeta& meta);
    Entry
    ReadEncryptedEntry(const EntryMeta& meta);

    void
    ReadPlainEntryStream(const PlainEntryMeta& pm,
                         const std::function<void(const uint8_t* data,
                                                  size_t len)>& slice_consumer,
                         size_t slice_size);

    void
    ReadEncryptedEntryStream(
        const EncryptedEntryMeta& em,
        const std::function<void(const uint8_t* data, size_t len)>&
            slice_consumer);

    void
    VerifyCrc32c(uint32_t expected,
                 const uint8_t* data,
                 size_t size,
                 const std::string& name);

    // Helper struct for tracking entry download state
    struct RangeCrc {
        uint32_t crc;
        size_t len;
    };

    struct EntryDownloadState {
        std::string name;
        int fd;
        uint32_t expected_crc;
        std::vector<RangeCrc> range_crcs;
    };

    struct EntryStreamDownloadState {
        std::string name;
        std::unique_ptr<PositionedFileWriter> writer;
        uint32_t expected_crc;
        std::vector<RangeCrc> range_crcs;
    };

    // Prepare download state for an entry (open file, allocate CRC vector)
    EntryDownloadState
    PrepareEntryDownload(const std::string& name,
                         const std::string& local_path,
                         const EntryMeta& meta);

    // Submit download tasks for an entry to the futures vector (does not wait)
    void
    SubmitEntryDownloadTasks(const EntryMeta& meta,
                             EntryDownloadState& state,
                             std::vector<std::future<void>>& futures);

    static size_t
    DownloadRangeCount(uint64_t size);

    static size_t
    DownloadTaskCount(const EntryMeta& meta);

    // Verify CRC and close file descriptor
    void
    FinalizeEntryDownload(EntryDownloadState& state);

    EntryStreamDownloadState
    PrepareEntryStreamDownload(const std::string& name,
                               const std::string& local_path,
                               const EntryMeta& meta,
                               io::Priority write_priority);

    void
    SubmitEntryStreamDownloadTasks(const EntryMeta& meta,
                                   EntryStreamDownloadState& state,
                                   std::vector<std::future<void>>& futures);

    static size_t
    StreamDownloadTaskCount(const EntryMeta& meta);

    void
    FinalizeEntryStreamDownload(EntryStreamDownloadState& state);

    std::shared_ptr<milvus::InputStream> input_;
    int64_t file_size_ = 0;
    int64_t collection_id_ = 0;
    ThreadPoolPriority priority_ = ThreadPoolPriority::HIGH;
    folly::CancellationToken cancellation_token_;

    bool is_encrypted_ = false;
    std::string edek_;
    int64_t ez_id_ = 0;
    size_t slice_size_ = 0;

    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin_;

    std::unordered_map<std::string, EntryMeta> entry_index_;
    EntryStreamLoadInfo stream_load_info_;
    std::vector<std::string> entry_names_;

    static constexpr size_t kSmallEntryCacheThreshold = 1 * 1024 * 1024;
    std::unordered_map<std::string, Entry> small_entry_cache_;
    nlohmann::json meta_json_;
};

}  // namespace milvus::storage
