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
#include <functional>
#include <future>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/Channel.h"
#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"
#include "storage/ChunkStreamUtils.h"
#include "storage/Crc32cUtil.h"
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

    // Parse __meta__ entry
    auto meta_entry = reader->ReadEntry(MILVUS_V3_META_ENTRY_NAME);
    if (!meta_entry.data.empty()) {
        try {
            reader->meta_json_ = nlohmann::json::parse(meta_entry.data.begin(),
                                                       meta_entry.data.end());
        } catch (const nlohmann::json::parse_error& e) {
            AssertInfo(
                false, "Failed to parse V3 index meta JSON: {}", e.what());
        }
    }

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

    // Parse 32-byte Footer from the last 32 bytes
    AssertInfo(tail_size >= MILVUS_V3_FOOTER_SIZE,
               "File too small for V3 footer");
    const uint8_t* footer_ptr =
        tail_data.data() + tail_size - MILVUS_V3_FOOTER_SIZE;

    uint16_t version;
    uint32_t meta_entry_size;
    uint32_t dir_size;

    std::memcpy(&version, footer_ptr + 0, sizeof(uint16_t));
    std::memcpy(&meta_entry_size, footer_ptr + 24, sizeof(uint32_t));
    std::memcpy(&dir_size, footer_ptr + 28, sizeof(uint32_t));

    AssertInfo(version == MILVUS_V3_FORMAT_VERSION,
               "Unsupported V3 format version: {}",
               version);
    AssertInfo(dir_size > 0, "Directory table size is zero");
    AssertInfo(static_cast<size_t>(dir_size) + meta_entry_size +
                       MILVUS_V3_FOOTER_SIZE + MILVUS_V3_MAGIC_SIZE <=
                   static_cast<size_t>(file_size_),
               "Directory table + meta entry + footer size exceeds file size");

    // Check if we need a second read
    size_t needed =
        static_cast<size_t>(dir_size) + meta_entry_size + MILVUS_V3_FOOTER_SIZE;
    size_t available_before_footer = tail_size - MILVUS_V3_FOOTER_SIZE;

    if (static_cast<size_t>(dir_size) + meta_entry_size >
        available_before_footer) {
        size_t new_tail_size = needed;
        size_t new_tail_offset = file_size_ - new_tail_size;

        std::vector<uint8_t> full_tail_data(new_tail_size);
        size_t need_more = new_tail_size - tail_size;

        size_t additional_read =
            input_->ReadAt(full_tail_data.data(), new_tail_offset, need_more);
        AssertInfo(additional_read == need_more,
                   "Failed to read additional directory data");

        std::memcpy(
            full_tail_data.data() + need_more, tail_data.data(), tail_size);

        tail_data = std::move(full_tail_data);
        tail_size = new_tail_size;
    }

    // Parse Directory Table JSON
    const uint8_t* dir_start =
        tail_data.data() + tail_size - MILVUS_V3_FOOTER_SIZE - dir_size;
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
            meta.enc.crc32 = Crc32cFromHex(entry["crc32"].get<std::string>());
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
            meta.plain.crc32 = Crc32cFromHex(entry["crc32"].get<std::string>());
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

void
IndexEntryReader::VerifyCrc32c(uint32_t expected,
                               const uint8_t* data,
                               size_t size,
                               const std::string& name) {
    uint32_t actual = Crc32cValue(data, size);
    AssertInfo(actual == expected,
               "CRC-32C mismatch for entry '{}': expected {}, got {}",
               name,
               Crc32cToHex(expected),
               Crc32cToHex(actual));
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
        VerifyCrc32c(pm.crc32, result.data.data(), pm.size, "");
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

    // CRC verification: sequential pass over the assembled buffer
    VerifyCrc32c(pm.crc32, result.data.data(), pm.size, "");

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

    // CRC verification over full plaintext buffer
    VerifyCrc32c(em.crc32, result.data.data(), em.original_size, "");

    return result;
}

IndexEntryReader::EntryDownloadState
IndexEntryReader::PrepareEntryDownload(const std::string& name,
                                       const std::string& local_path,
                                       const EntryMeta& meta) {
    constexpr size_t kRangeSize = 16 * 1024 * 1024;

    int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    AssertInfo(fd != -1, "Failed to create file: {}", local_path);

    EntryDownloadState state;
    state.name = name;
    state.fd = fd;

    try {
        if (meta.encrypted) {
            state.expected_crc = meta.enc.crc32;
            state.range_crcs.resize(meta.enc.slices.size());
            auto trc_ret = ::ftruncate(fd, meta.enc.original_size);
            AssertInfo(trc_ret == 0,
                       "Failed to ftruncate file {}: {}",
                       local_path,
                       strerror(errno));
        } else {
            state.expected_crc = meta.plain.crc32;
            size_t num_ranges = (meta.plain.size + kRangeSize - 1) / kRangeSize;
            state.range_crcs.resize(num_ranges);
        }
    } catch (...) {
        ::close(fd);
        throw;
    }

    return state;
}

void
IndexEntryReader::SubmitEntryDownloadTasks(
    const EntryMeta& meta,
    EntryDownloadState& state,
    std::vector<std::future<void>>& futures) {
    constexpr size_t kRangeSize = 16 * 1024 * 1024;
    auto& pool = ThreadPools::GetThreadPool(priority_);

    if (meta.encrypted) {
        const auto& em = meta.enc;
        size_t output_offset = 0;

        for (size_t i = 0; i < em.slices.size(); i++) {
            const auto& slice = em.slices[i];
            size_t this_output_offset = output_offset;
            size_t remaining = em.original_size - output_offset;
            size_t plain_len = std::min(remaining, slice_size_);
            output_offset += plain_len;

            futures.push_back(pool.Submit([this,
                                           slice,
                                           fd = state.fd,
                                           this_output_offset,
                                           plain_len,
                                           i,
                                           &state]() {
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
                state.range_crcs[i] = {
                    Crc32cValue(reinterpret_cast<const uint8_t*>(plain.data()),
                                plain.size()),
                    plain.size()};
            }));
        }
    } else {
        const auto& pm = meta.plain;
        size_t remaining = pm.size;
        size_t file_offset = 0;
        size_t src_offset = pm.offset;
        size_t range_idx = 0;

        while (remaining > 0) {
            size_t len = std::min(remaining, kRangeSize);
            size_t this_file_offset = file_offset;
            size_t this_src_offset = src_offset;
            size_t this_range_idx = range_idx;

            futures.push_back(pool.Submit([this,
                                           this_src_offset,
                                           len,
                                           fd = state.fd,
                                           this_file_offset,
                                           this_range_idx,
                                           &state]() {
                std::vector<uint8_t> buf(len);
                size_t n = input_->ReadAt(
                    buf.data(), MILVUS_V3_MAGIC_SIZE + this_src_offset, len);
                AssertInfo(n == len, "Failed to read data for file");
                auto written = ::pwrite(fd, buf.data(), len, this_file_offset);
                AssertInfo(written == static_cast<ssize_t>(len),
                           "Failed to pwrite");
                state.range_crcs[this_range_idx] = {
                    Crc32cValue(buf.data(), len), len};
            }));

            remaining -= len;
            file_offset += len;
            src_offset += len;
            range_idx++;
        }
    }
}

void
IndexEntryReader::FinalizeEntryDownload(EntryDownloadState& state) {
    uint32_t combined_crc = 0;
    if (!state.range_crcs.empty()) {
        combined_crc = state.range_crcs[0].crc;
        for (size_t i = 1; i < state.range_crcs.size(); i++) {
            combined_crc = Crc32cCombine(
                combined_crc, state.range_crcs[i].crc, state.range_crcs[i].len);
        }
    }
    AssertInfo(combined_crc == state.expected_crc,
               "CRC-32C mismatch for entry '{}': expected {}, got {}",
               state.name,
               Crc32cToHex(state.expected_crc),
               Crc32cToHex(combined_crc));

    ::close(state.fd);
    state.fd = -1;
}

void
IndexEntryReader::ReadEntryToFile(const std::string& name,
                                  const std::string& local_path) {
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    auto state = PrepareEntryDownload(name, local_path, meta);
    try {
        std::vector<std::future<void>> futures;
        SubmitEntryDownloadTasks(meta, state, futures);

        for (auto& f : futures) {
            f.get();
        }

        FinalizeEntryDownload(state);
    } catch (...) {
        if (state.fd != -1) {
            ::close(state.fd);
        }
        throw;
    }
}

void
IndexEntryReader::ReadEntriesToFiles(
    const std::vector<std::pair<std::string, std::string>>& name_path_pairs) {
    if (name_path_pairs.empty()) {
        return;
    }

    // Prepare all download states
    std::vector<EntryDownloadState> states;
    states.reserve(name_path_pairs.size());

    // Helper to close all open fds in states
    auto close_all_fds = [&states]() {
        for (auto& s : states) {
            if (s.fd != -1) {
                ::close(s.fd);
                s.fd = -1;
            }
        }
    };

    try {
        for (const auto& [name, path] : name_path_pairs) {
            auto it = entry_index_.find(name);
            AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
            states.push_back(PrepareEntryDownload(name, path, it->second));
        }

        // Submit ALL tasks for ALL entries at once (avoids thread pool deadlock)
        std::vector<std::future<void>> all_futures;
        for (size_t i = 0; i < name_path_pairs.size(); i++) {
            const auto& meta = entry_index_.at(name_path_pairs[i].first);
            SubmitEntryDownloadTasks(meta, states[i], all_futures);
        }

        // Wait for ALL tasks to complete
        for (auto& f : all_futures) {
            f.get();
        }

        // Verify CRCs and close all file descriptors
        for (auto& state : states) {
            FinalizeEntryDownload(state);
        }
    } catch (...) {
        close_all_fds();
        throw;
    }
}

size_t
IndexEntryReader::GetEntrySize(const std::string& name) const {
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    if (it->second.encrypted) {
        return it->second.enc.original_size;
    }
    return it->second.plain.size;
}

void
IndexEntryReader::ReadEntryStream(
    const std::string& name,
    std::function<void(const uint8_t* data, size_t len)> callback,
    size_t chunk_size) {
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    if (meta.encrypted) {
        ReadEncryptedEntryStream(meta.enc, callback);
    } else {
        ReadPlainEntryStream(meta.plain, callback, chunk_size);
    }
}

void
IndexEntryReader::ReadPlainEntryStream(
    const PlainEntryMeta& pm,
    const std::function<void(const uint8_t* data, size_t len)>& callback,
    size_t chunk_size) {
    size_t num_chunks = (pm.size + chunk_size - 1) / chunk_size;
    if (num_chunks == 0) {
        return;
    }

    auto& pool = ThreadPools::GetThreadPool(priority_);
    auto& budget = ChunkInflightBudget::GetInstance();
    auto channel = std::make_shared<Channel<std::shared_ptr<ChunkResult>>>(
        budget.MaxInflight());

    size_t next_submit = 0;
    size_t next_consume = 0;
    size_t local_inflight = 0;

    auto submitOne = [&]() {
        size_t seq = next_submit;
        size_t off = seq * chunk_size;
        size_t len = std::min<size_t>(chunk_size, pm.size - off);
        size_t src = pm.offset + off;

        pool.Submit([this, channel, seq, src, len]() {
            auto r = std::make_shared<ChunkResult>();
            r->seq = seq;
            try {
                r->data.resize(len);
                size_t n = input_->ReadAt(
                    r->data.data(), MILVUS_V3_MAGIC_SIZE + src, len);
                AssertInfo(n == len, "Failed to read entry chunk");
            } catch (...) {
                r->error = std::current_exception();
            }
            channel->push(std::move(r));
        });

        next_submit++;
        local_inflight++;
    };

    // 1. Fill window: block for first slot, then try-acquire remaining.
    if (next_submit < num_chunks) {
        budget.Acquire();
        submitOne();
    }
    while (next_submit < num_chunks && budget.TryAcquire()) {
        submitOne();
    }

    // 2. Slide: consume one, submit next
    uint32_t running_crc = 0;
    bool first = true;
    std::map<size_t, std::shared_ptr<ChunkResult>> reorder_buf;
    std::exception_ptr first_error = nullptr;

    auto deliverChunk = [&](const std::shared_ptr<ChunkResult>& c) {
        uint32_t chunk_crc = Crc32cValue(c->data.data(), c->data.size());
        running_crc =
            first ? chunk_crc
                  : Crc32cCombine(running_crc, chunk_crc, c->data.size());
        first = false;
        callback(c->data.data(), c->data.size());
        budget.Release();
        next_consume++;
        local_inflight--;
    };

    while (next_consume < num_chunks && local_inflight > 0) {
        std::shared_ptr<ChunkResult> chunk;
        bool ok = channel->pop(chunk);
        AssertInfo(ok, "Channel closed unexpectedly");

        if (chunk->error) {
            if (!first_error) {
                first_error = chunk->error;
            }
            budget.Release();
            local_inflight--;
            continue;
        }

        if (first_error) {
            // Drain mode: release slot, do not deliver or submit more.
            budget.Release();
            local_inflight--;
            continue;
        }

        if (chunk->seq == next_consume) {
            deliverChunk(chunk);
        } else {
            reorder_buf[chunk->seq] = std::move(chunk);
        }

        // Deliver any consecutive buffered chunks
        while (reorder_buf.count(next_consume)) {
            auto node = reorder_buf.extract(next_consume);
            deliverChunk(node.mapped());
        }

        // Replenish: try non-blocking first
        while (next_submit < num_chunks && budget.TryAcquire()) {
            submitOne();
        }

        // If nothing inflight but still work to do, must block-acquire
        // to guarantee progress. Safe because channel is empty here.
        if (local_inflight == 0 && next_submit < num_chunks) {
            budget.Acquire();
            submitOne();
        }
    }

    // Release budget held by any chunks still sitting in reorder_buf
    // (possible when an error shortened the normal delivery path).
    for (size_t i = 0; i < reorder_buf.size(); ++i) {
        budget.Release();
    }
    local_inflight -= reorder_buf.size();
    reorder_buf.clear();

    if (first_error) {
        std::rethrow_exception(first_error);
    }

    AssertInfo(running_crc == pm.crc32,
               "CRC-32C mismatch in stream read: expected {}, got {}",
               Crc32cToHex(pm.crc32),
               Crc32cToHex(running_crc));
}

void
IndexEntryReader::ReadEncryptedEntryStream(
    const EncryptedEntryMeta& em,
    const std::function<void(const uint8_t* data, size_t len)>& callback) {
    size_t num_slices = em.slices.size();
    if (num_slices == 0) {
        return;
    }

    auto& pool = ThreadPools::GetThreadPool(priority_);
    auto& budget = ChunkInflightBudget::GetInstance();
    auto channel = std::make_shared<Channel<std::shared_ptr<ChunkResult>>>(
        budget.MaxInflight());

    size_t next_submit = 0;
    size_t next_consume = 0;
    size_t local_inflight = 0;

    auto submitOne = [&]() {
        size_t seq = next_submit;
        const auto& slice = em.slices[seq];

        pool.Submit([this, channel, seq, slice]() {
            auto r = std::make_shared<ChunkResult>();
            r->seq = seq;
            try {
                std::vector<uint8_t> cipher(slice.size);
                size_t n = input_->ReadAt(cipher.data(),
                                          MILVUS_V3_MAGIC_SIZE + slice.offset,
                                          slice.size);
                AssertInfo(n == slice.size, "Failed to read encrypted slice");

                auto dec =
                    cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
                auto plain = dec->Decrypt(cipher.data(), cipher.size());

                r->data.assign(reinterpret_cast<const uint8_t*>(plain.data()),
                               reinterpret_cast<const uint8_t*>(plain.data()) +
                                   plain.size());
            } catch (...) {
                r->error = std::current_exception();
            }
            channel->push(std::move(r));
        });

        next_submit++;
        local_inflight++;
    };

    // Fill window: block for first slot, then try-acquire remaining
    if (next_submit < num_slices) {
        budget.Acquire();
        submitOne();
    }
    while (next_submit < num_slices && budget.TryAcquire()) {
        submitOne();
    }

    // Slide: consume + replenish
    uint32_t running_crc = 0;
    bool first = true;
    std::map<size_t, std::shared_ptr<ChunkResult>> reorder_buf;
    std::exception_ptr first_error = nullptr;

    auto deliverChunk = [&](const std::shared_ptr<ChunkResult>& c) {
        uint32_t chunk_crc = Crc32cValue(c->data.data(), c->data.size());
        running_crc =
            first ? chunk_crc
                  : Crc32cCombine(running_crc, chunk_crc, c->data.size());
        first = false;
        callback(c->data.data(), c->data.size());
        budget.Release();
        next_consume++;
        local_inflight--;
    };

    while (next_consume < num_slices && local_inflight > 0) {
        std::shared_ptr<ChunkResult> chunk;
        bool ok = channel->pop(chunk);
        AssertInfo(ok, "Channel closed unexpectedly");

        if (chunk->error) {
            if (!first_error) {
                first_error = chunk->error;
            }
            budget.Release();
            local_inflight--;
            continue;
        }

        if (first_error) {
            budget.Release();
            local_inflight--;
            continue;
        }

        if (chunk->seq == next_consume) {
            deliverChunk(chunk);
        } else {
            reorder_buf[chunk->seq] = std::move(chunk);
        }

        while (reorder_buf.count(next_consume)) {
            auto node = reorder_buf.extract(next_consume);
            deliverChunk(node.mapped());
        }

        while (next_submit < num_slices && budget.TryAcquire()) {
            submitOne();
        }

        if (local_inflight == 0 && next_submit < num_slices) {
            budget.Acquire();
            submitOne();
        }
    }

    for (size_t i = 0; i < reorder_buf.size(); ++i) {
        budget.Release();
    }
    local_inflight -= reorder_buf.size();
    reorder_buf.clear();

    if (first_error) {
        std::rethrow_exception(first_error);
    }

    AssertInfo(running_crc == em.crc32,
               "CRC-32C mismatch in encrypted stream read");
}

}  // namespace milvus::storage
