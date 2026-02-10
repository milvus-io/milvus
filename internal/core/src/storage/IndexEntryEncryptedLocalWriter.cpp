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

#include "storage/IndexEntryEncryptedLocalWriter.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <algorithm>
#include <utility>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"
#include "storage/RemoteOutputStream.h"

namespace milvus::storage {

IndexEntryEncryptedLocalWriter::IndexEntryEncryptedLocalWriter(
    const std::string& remote_path,
    milvus_storage::ArrowFileSystemPtr fs,
    std::shared_ptr<plugin::ICipherPlugin> cipher_plugin,
    int64_t ez_id,
    int64_t collection_id,
    size_t slice_size)
    : remote_path_(remote_path),
      fs_(std::move(fs)),
      cipher_plugin_(std::move(cipher_plugin)),
      ez_id_(ez_id),
      collection_id_(collection_id),
      slice_size_(slice_size),
      pool_(ThreadPools::GetThreadPool(ThreadPoolPriority::MIDDLE)) {
    auto [encryptor, edek] =
        cipher_plugin_->GetEncryptor(ez_id_, collection_id_);
    edek_ = std::move(edek);

    auto uuid = boost::uuids::random_generator()();
    local_path_ = "/tmp/milvus_enc_" + boost::uuids::to_string(uuid);

    local_fd_ = ::open(local_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    AssertInfo(
        local_fd_ != -1, "Failed to create temp file: {}", strerror(errno));

    auto written = ::write(local_fd_, MILVUS_V3_MAGIC, MILVUS_V3_MAGIC_SIZE);
    AssertInfo(written == static_cast<ssize_t>(MILVUS_V3_MAGIC_SIZE),
               "Failed to write magic number");
}

IndexEntryEncryptedLocalWriter::~IndexEntryEncryptedLocalWriter() {
    if (local_fd_ != -1) {
        ::close(local_fd_);
        ::unlink(local_path_.c_str());
    }
}

void
IndexEntryEncryptedLocalWriter::WriteEntry(const std::string& name,
                                           const void* data,
                                           size_t size) {
    AssertInfo(!finished_, "Cannot write after Finish() has been called");
    CheckDuplicateName(name);
    EncryptAndWriteSlices(
        name, size, reinterpret_cast<const uint8_t*>(data), size);
}

static std::string
ReadExact(int fd, size_t len) {
    std::string buf(len, '\0');
    size_t total_read = 0;
    while (total_read < len) {
        ssize_t n = ::read(fd, buf.data() + total_read, len - total_read);
        if (n == -1 && errno == EINTR) {
            continue;
        }
        AssertInfo(
            n > 0, "Failed to read from file descriptor: {}", strerror(errno));
        total_read += n;
    }
    return buf;
}

void
IndexEntryEncryptedLocalWriter::WriteEntry(const std::string& name,
                                           int fd,
                                           size_t size) {
    AssertInfo(!finished_, "Cannot write after Finish() has been called");
    CheckDuplicateName(name);

    std::vector<SliceMeta> slices;
    const size_t W = std::max(static_cast<size_t>(pool_.GetMaxThreadNum()),
                              static_cast<size_t>(1));
    std::deque<std::future<std::string>> pending;
    size_t remaining = size;

    while (remaining > 0 || !pending.empty()) {
        // Fill the sliding window: read one slice from fd and submit encryption
        while (pending.size() < W && remaining > 0) {
            size_t len = std::min(remaining, slice_size_);
            auto slice_data = ReadExact(fd, len);
            pending.push_back(pool_.Submit([this, s = std::move(slice_data)]() {
                auto [enc, unused_edek] =
                    cipher_plugin_->GetEncryptor(ez_id_, collection_id_);
                return enc->Encrypt(s);
            }));
            remaining -= len;
        }
        // Drain the oldest completed future
        if (!pending.empty()) {
            auto encrypted = pending.front().get();
            pending.pop_front();
            auto written =
                ::write(local_fd_, encrypted.data(), encrypted.size());
            AssertInfo(written == static_cast<ssize_t>(encrypted.size()),
                       "Failed to write encrypted slice");
            slices.push_back(
                {current_offset_, static_cast<uint64_t>(encrypted.size())});
            current_offset_ += encrypted.size();
        }
    }
    dir_entries_.push_back(
        {name, static_cast<uint64_t>(size), std::move(slices)});
}

void
IndexEntryEncryptedLocalWriter::EncryptAndWriteSlices(const std::string& name,
                                                      uint64_t original_size,
                                                      const uint8_t* data,
                                                      size_t size) {
    std::vector<SliceMeta> slices;
    const size_t W = std::max(static_cast<size_t>(pool_.GetMaxThreadNum()),
                              static_cast<size_t>(1));
    std::deque<std::future<std::string>> pending;
    size_t remaining = size;
    size_t read_offset = 0;

    while (remaining > 0 || !pending.empty()) {
        while (pending.size() < W && remaining > 0) {
            size_t len = std::min(remaining, slice_size_);
            auto slice_data = std::string(
                reinterpret_cast<const char*>(data + read_offset), len);
            pending.push_back(pool_.Submit([this, s = std::move(slice_data)]() {
                auto [enc, unused_edek] =
                    cipher_plugin_->GetEncryptor(ez_id_, collection_id_);
                return enc->Encrypt(s);
            }));
            read_offset += len;
            remaining -= len;
        }
        if (!pending.empty()) {
            auto encrypted = pending.front().get();
            pending.pop_front();
            auto written =
                ::write(local_fd_, encrypted.data(), encrypted.size());
            AssertInfo(written == static_cast<ssize_t>(encrypted.size()),
                       "Failed to write encrypted slice");
            slices.push_back(
                {current_offset_, static_cast<uint64_t>(encrypted.size())});
            current_offset_ += encrypted.size();
        }
    }
    dir_entries_.push_back({name, original_size, std::move(slices)});
}

void
IndexEntryEncryptedLocalWriter::Finish() {
    AssertInfo(!finished_, "Finish() has already been called");

    nlohmann::json dir_json;
    dir_json["version"] = 3;
    dir_json["slice_size"] = slice_size_;
    dir_json["entries"] = nlohmann::json::array();

    for (const auto& entry : dir_entries_) {
        nlohmann::json e;
        e["name"] = entry.name;
        e["original_size"] = entry.original_size;
        e["slices"] = nlohmann::json::array();
        for (const auto& s : entry.slices) {
            e["slices"].push_back({{"offset", s.offset}, {"size", s.size}});
        }
        dir_json["entries"].push_back(std::move(e));
    }

    dir_json["__edek__"] = edek_;
    dir_json["__ez_id__"] = std::to_string(ez_id_);

    auto dir_str = dir_json.dump();
    auto written = ::write(local_fd_, dir_str.data(), dir_str.size());
    AssertInfo(written == static_cast<ssize_t>(dir_str.size()),
               "Failed to write directory table");

    uint64_t dir_size = dir_str.size();
    written = ::write(local_fd_, &dir_size, sizeof(uint64_t));
    AssertInfo(written == static_cast<ssize_t>(sizeof(uint64_t)),
               "Failed to write footer");

    ::close(local_fd_);
    local_fd_ = -1;

    total_bytes_written_ = MILVUS_V3_MAGIC_SIZE + current_offset_ +
                           dir_str.size() + sizeof(uint64_t);

    UploadLocalFile();

    ::unlink(local_path_.c_str());
    finished_ = true;
}

void
IndexEntryEncryptedLocalWriter::UploadLocalFile() {
    auto result = fs_->OpenOutputStream(remote_path_);
    AssertInfo(result.ok(),
               "Failed to open remote output stream: {}",
               result.status().ToString());
    auto remote_stream =
        std::make_shared<RemoteOutputStream>(std::move(result.ValueOrDie()));

    int read_fd = ::open(local_path_.c_str(), O_RDONLY);
    AssertInfo(
        read_fd != -1, "Failed to open local file for upload: {}", local_path_);

    constexpr size_t kBufSize = 16 * 1024 * 1024;
    std::vector<char> buf(kBufSize);
    while (true) {
        ssize_t n = ::read(read_fd, buf.data(), kBufSize);
        if (n > 0) {
            remote_stream->Write(buf.data(), n);
        } else if (n == 0) {
            break;  // EOF
        } else {
            if (errno == EINTR) {
                continue;
            }
            ::close(read_fd);
            ThrowInfo(ErrorCode::UnexpectedError,
                      fmt::format("Failed to read local file {}: {}",
                                  local_path_,
                                  strerror(errno)));
        }
    }
    ::close(read_fd);
    remote_stream->Close();
}

}  // namespace milvus::storage
