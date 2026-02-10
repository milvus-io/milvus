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

#include "storage/IndexEntryReader.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <future>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"
#include "storage/PluginLoader.h"

namespace milvus::storage {

std::unique_ptr<IndexEntryReader>
IndexEntryReader::Open(std::shared_ptr<milvus::InputStream> input,
                       int64_t file_size,
                       int64_t collection_id,
                       ThreadPoolPriority priority) {
    auto reader = std::unique_ptr<IndexEntryReader>(new IndexEntryReader());
    reader->input_ = std::move(input);
    reader->file_size_ = file_size;
    reader->collection_id_ = collection_id;
    reader->priority_ = priority;
    reader->ValidateMagic();
    reader->ReadFooterAndDirectory();
    return reader;
}

void
IndexEntryReader::ValidateMagic() {
    char magic_buf[MILVUS_V3_MAGIC_SIZE];
    size_t bytes_read = input_->ReadAt(magic_buf, 0, MILVUS_V3_MAGIC_SIZE);
    AssertInfo(bytes_read == MILVUS_V3_MAGIC_SIZE,
               "Failed to read V3 magic number");
    AssertInfo(
        std::memcmp(magic_buf, MILVUS_V3_MAGIC, MILVUS_V3_MAGIC_SIZE) == 0,
        "Invalid V3 magic number");
}

void
IndexEntryReader::ReadFooterAndDirectory() {
    constexpr size_t kTailBufferSize = 64 * 1024UL;
    size_t tail_size =
        std::min(static_cast<size_t>(file_size_), kTailBufferSize);
    size_t tail_offset = file_size_ - tail_size;

    std::vector<uint8_t> tail_data(tail_size);
    size_t bytes_read =
        input_->ReadAt(tail_data.data(), tail_offset, tail_size);
    AssertInfo(bytes_read == tail_size, "Failed to read file tail");

    uint64_t dir_size;
    std::memcpy(&dir_size,
                tail_data.data() + tail_size - sizeof(uint64_t),
                sizeof(uint64_t));

    AssertInfo(dir_size > 0, "Directory table size is zero");
    AssertInfo(dir_size + MILVUS_V3_MAGIC_SIZE + sizeof(uint64_t) <=
                   static_cast<size_t>(file_size_),
               "Directory table size exceeds file size");

    size_t available_dir_size = tail_size - sizeof(uint64_t);

    if (dir_size > available_dir_size) {
        size_t need_more = dir_size - available_dir_size;
        size_t new_tail_size = dir_size + sizeof(uint64_t);
        size_t new_tail_offset = file_size_ - new_tail_size;

        std::vector<uint8_t> full_tail_data(new_tail_size);

        size_t additional_read =
            input_->ReadAt(full_tail_data.data(), new_tail_offset, need_more);
        AssertInfo(additional_read == need_more,
                   "Failed to read additional directory data");

        std::memcpy(
            full_tail_data.data() + need_more, tail_data.data(), tail_size);

        tail_data = std::move(full_tail_data);
        tail_size = new_tail_size;
    }

    const uint8_t* dir_start =
        tail_data.data() + tail_size - sizeof(uint64_t) - dir_size;
    const uint8_t* dir_end = dir_start + dir_size;

    nlohmann::json dir_json;
    try {
        dir_json = nlohmann::json::parse(dir_start, dir_end);
    } catch (const nlohmann::json::parse_error& e) {
        AssertInfo(false,
                   "Failed to parse V3 index directory table JSON: {}",
                   e.what());
    }

    AssertInfo(dir_json.contains("entries"), "Directory table missing entries");

    if (dir_json.contains("__edek__")) {
        is_encrypted_ = true;
        edek_ = dir_json["__edek__"].get<std::string>();
        ez_id_ = std::stoll(dir_json["__ez_id__"].get<std::string>());
        slice_size_ = dir_json["slice_size"].get<size_t>();

        cipher_plugin_ = PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(cipher_plugin_ != nullptr,
                   "Cipher plugin required for encrypted V3 index");

        for (const auto& entry : dir_json["entries"]) {
            EntryMeta meta;
            meta.encrypted = true;
            meta.enc.original_size = entry["original_size"].get<uint64_t>();
            for (const auto& s : entry["slices"]) {
                meta.enc.slices.push_back(
                    {s["offset"].get<uint64_t>(), s["size"].get<uint64_t>()});
            }
            std::string name = entry["name"].get<std::string>();
            entry_names_.push_back(name);
            entry_index_.emplace(std::move(name), std::move(meta));
        }
    } else {
        is_encrypted_ = false;

        for (const auto& entry : dir_json["entries"]) {
            EntryMeta meta;
            meta.encrypted = false;
            meta.plain.offset = entry["offset"].get<uint64_t>();
            meta.plain.size = entry["size"].get<uint64_t>();
            std::string name = entry["name"].get<std::string>();
            entry_names_.push_back(name);
            entry_index_.emplace(std::move(name), std::move(meta));
        }
    }
}

std::vector<std::string>
IndexEntryReader::GetEntryNames() const {
    return entry_names_;
}

Entry
IndexEntryReader::ReadEntry(const std::string& name) {
    auto cache_it = small_entry_cache_.find(name);
    if (cache_it != small_entry_cache_.end()) {
        return cache_it->second;
    }

    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    Entry result;
    if (meta.encrypted) {
        result = ReadEncryptedEntry(meta);
    } else {
        result = ReadPlainEntry(meta);
    }

    if (result.data.size() <= kSmallEntryCacheThreshold) {
        small_entry_cache_[name] = result;
    }

    return result;
}

Entry
IndexEntryReader::ReadPlainEntry(const EntryMeta& meta) {
    const auto& pm = meta.plain;
    Entry result;
    result.data.resize(pm.size);

    constexpr size_t kRangeSize = 16 * 1024 * 1024;

    if (pm.size <= kRangeSize) {
        size_t n = input_->ReadAt(
            result.data.data(), MILVUS_V3_MAGIC_SIZE + pm.offset, pm.size);
        AssertInfo(n == pm.size, "Failed to read entry data");
        return result;
    }

    auto& pool = ThreadPools::GetThreadPool(priority_);
    uint8_t* dest = result.data.data();

    std::vector<std::future<void>> futures;
    size_t remaining = pm.size;
    size_t offset = 0;

    while (remaining > 0) {
        size_t len = std::min(remaining, kRangeSize);
        size_t this_offset = offset;

        futures.push_back(pool.Submit([this, dest, this_offset, len, &pm]() {
            size_t n =
                input_->ReadAt(dest + this_offset,
                               MILVUS_V3_MAGIC_SIZE + pm.offset + this_offset,
                               len);
            AssertInfo(n == len, "Failed to read entry data range");
        }));

        remaining -= len;
        offset += len;
    }

    for (auto& f : futures) {
        f.get();
    }

    return result;
}

Entry
IndexEntryReader::ReadEncryptedEntry(const EntryMeta& meta) {
    const auto& em = meta.enc;
    Entry result;
    result.data.resize(em.original_size);

    auto& pool = ThreadPools::GetThreadPool(priority_);
    uint8_t* dest = result.data.data();

    std::vector<std::future<void>> futures;
    size_t cur_output_offset = 0;

    for (const auto& slice : em.slices) {
        size_t this_output_offset = cur_output_offset;
        size_t remaining = em.original_size - cur_output_offset;
        size_t plain_len = std::min(remaining, slice_size_);
        cur_output_offset += plain_len;

        futures.push_back(
            pool.Submit([this, &slice, dest, this_output_offset, plain_len]() {
                std::vector<uint8_t> cipher(slice.size);
                size_t n = input_->ReadAt(cipher.data(),
                                          MILVUS_V3_MAGIC_SIZE + slice.offset,
                                          slice.size);
                AssertInfo(n == slice.size, "Failed to read encrypted slice");

                auto dec =
                    cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
                auto plain = dec->Decrypt(cipher.data(), cipher.size());

                AssertInfo(plain.size() == plain_len,
                           "Decrypted size mismatch: expected {}, got {}",
                           plain_len,
                           plain.size());
                std::memcpy(
                    dest + this_output_offset, plain.data(), plain.size());
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    return result;
}

void
IndexEntryReader::ReadEntryToFile(const std::string& name,
                                  const std::string& local_path) {
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    if (meta.encrypted) {
        WriteEncryptedEntryToFile(meta, local_path);
    } else {
        WritePlainEntryToFile(meta, local_path);
    }
}

void
IndexEntryReader::WritePlainEntryToFile(const EntryMeta& meta,
                                        const std::string& local_path) {
    const auto& pm = meta.plain;
    int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    AssertInfo(fd != -1, "Failed to create file: {}", local_path);

    auto& pool = ThreadPools::GetThreadPool(priority_);
    constexpr size_t kRangeSize = 16 * 1024 * 1024;

    std::vector<std::future<void>> futures;
    size_t remaining = pm.size;
    size_t file_offset = 0;
    size_t src_offset = pm.offset;

    while (remaining > 0) {
        size_t len = std::min(remaining, kRangeSize);
        size_t this_file_offset = file_offset;
        size_t this_src_offset = src_offset;

        futures.push_back(
            pool.Submit([this, this_src_offset, len, fd, this_file_offset]() {
                std::vector<uint8_t> buf(len);
                size_t n = input_->ReadAt(
                    buf.data(), MILVUS_V3_MAGIC_SIZE + this_src_offset, len);
                AssertInfo(n == len, "Failed to read data for file");
                auto written = ::pwrite(fd, buf.data(), len, this_file_offset);
                AssertInfo(written == static_cast<ssize_t>(len),
                           "Failed to pwrite");
            }));

        remaining -= len;
        file_offset += len;
        src_offset += len;
    }

    for (auto& f : futures) {
        f.get();
    }

    ::close(fd);
}

void
IndexEntryReader::WriteEncryptedEntryToFile(const EntryMeta& meta,
                                            const std::string& local_path) {
    const auto& em = meta.enc;
    int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    AssertInfo(fd != -1, "Failed to create file: {}", local_path);

    auto trc_ret = ::ftruncate(fd, em.original_size);
    AssertInfo(trc_ret == 0,
               "Failed to ftruncate file {}: {}",
               local_path,
               strerror(errno));

    auto& pool = ThreadPools::GetThreadPool(priority_);

    std::vector<std::future<void>> futures;
    size_t output_offset = 0;

    for (const auto& slice : em.slices) {
        size_t this_output_offset = output_offset;
        size_t remaining = em.original_size - output_offset;
        size_t plain_len = std::min(remaining, slice_size_);
        output_offset += plain_len;

        futures.push_back(
            pool.Submit([this, &slice, fd, this_output_offset, plain_len]() {
                std::vector<uint8_t> cipher(slice.size);
                size_t n = input_->ReadAt(cipher.data(),
                                          MILVUS_V3_MAGIC_SIZE + slice.offset,
                                          slice.size);
                AssertInfo(n == slice.size, "Failed to read encrypted slice");

                auto dec =
                    cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
                auto plain = dec->Decrypt(cipher.data(), cipher.size());

                AssertInfo(plain.size() == plain_len,
                           "Decrypted size mismatch: expected {}, got {}",
                           plain_len,
                           plain.size());
                auto written = ::pwrite(
                    fd, plain.data(), plain.size(), this_output_offset);
                AssertInfo(written == static_cast<ssize_t>(plain.size()),
                           "Failed to pwrite");
            }));
    }

    for (auto& f : futures) {
        f.get();
    }

    ::close(fd);
}

void
IndexEntryReader::ReadEntriesToFiles(
    const std::vector<std::pair<std::string, std::string>>& name_path_pairs) {
    for (const auto& [name, path] : name_path_pairs) {
        ReadEntryToFile(name, path);
    }
}

}  // namespace milvus::storage
