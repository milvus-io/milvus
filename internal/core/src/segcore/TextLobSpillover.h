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

#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <filesystem>

#include <atomic>
#include <fcntl.h>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"

namespace milvus::segcore {

/**
 * TextLobRef is a 16-byte fixed-size reference to TEXT data stored in a
 * temporary LOB file. It is stored in ConcurrentVector<std::string> as a
 * binary-encoded string.
 *
 * Layout:
 *   - offset (8 bytes): position in the LOB file
 *   - size (4 bytes): length of the text data
 *   - flags (4 bytes): reserved for future use (e.g., compression)
 */
struct TextLobRef {
    uint64_t offset;  // Position in LOB file
    uint32_t size;    // Length of text data
    uint32_t flags;   // Reserved (0 = uncompressed)

    static constexpr size_t kEncodedSize = 16;

    // Encode reference to binary string for storage in ConcurrentVector
    std::string
    Encode() const {
        static_assert(sizeof(TextLobRef) == kEncodedSize,
                      "TextLobRef must be 16 bytes");
        return std::string(reinterpret_cast<const char*>(this), kEncodedSize);
    }

    // Decode reference from binary string
    static TextLobRef
    Decode(std::string_view s) {
        AssertInfo(s.size() == kEncodedSize,
                   "Invalid TextLobRef encoding: expected {} bytes, got {}",
                   kEncodedSize,
                   s.size());
        TextLobRef ref;
        std::memcpy(&ref, s.data(), kEncodedSize);
        return ref;
    }

    // Check if a string looks like a TextLobRef (for debugging)
    static bool
    IsValidEncoding(std::string_view s) {
        return s.size() == kEncodedSize;
    }
};

/**
 * TextLobSpillover manages a temporary LOB file for a single TEXT field
 * in a growing segment. Uses a single fd with pwrite/pread for both
 * writing and reading, eliminating the need for flush between write and read.
 *
 * Thread safety:
 *   - pwrite: protected by mutex (serializes offset advancement)
 *   - pread: lock-free (POSIX guarantees pread is thread-safe)
 *   - pwrite + pread concurrent: safe (both operate on kernel page cache)
 *
 * File format: simple binary append (raw text bytes concatenated)
 *   [text1_bytes][text2_bytes][text3_bytes]...
 */
class TextLobSpillover {
 public:
    TextLobSpillover(int64_t segment_id,
                     FieldId field_id,
                     const std::string& base_path)
        : segment_id_(segment_id),
          field_id_(field_id),
          path_(BuildPath(base_path, segment_id, field_id)),
          fd_(-1),
          current_offset_(0) {
        // Create parent directories if needed
        auto parent = std::filesystem::path(path_).parent_path();
        if (!parent.empty()) {
            std::filesystem::create_directories(parent);
        }

        fd_ =
            ::open(path_.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
        AssertInfo(fd_ >= 0,
                   "Failed to create LOB spillover file: {} (errno={})",
                   path_,
                   errno);
    }

    ~TextLobSpillover() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (std::filesystem::exists(path_)) {
            std::filesystem::remove(path_);
        }
    }

    // Non-copyable, non-movable
    TextLobSpillover(const TextLobSpillover&) = delete;
    TextLobSpillover&
    operator=(const TextLobSpillover&) = delete;
    TextLobSpillover(TextLobSpillover&&) = delete;
    TextLobSpillover&
    operator=(TextLobSpillover&&) = delete;

    /**
     * Write text and return encoded reference string.
     * The returned string can be stored in ConcurrentVector<std::string>.
     * Thread-safe (mutex protects offset advancement).
     */
    std::string
    WriteAndEncode(const std::string& text) {
        return WriteAndEncode(text.data(), text.size());
    }

    std::string
    WriteAndEncode(const char* data, size_t size) {
        std::lock_guard<std::mutex> lock(write_mutex_);

        AssertInfo(size <= std::numeric_limits<uint32_t>::max(),
                   "TEXT value too large for LOB spillover: {} bytes (max 4GB)",
                   size);

        TextLobRef ref;
        ref.offset = current_offset_;
        ref.size = static_cast<uint32_t>(size);
        ref.flags = 0;

        size_t total_written = 0;
        while (total_written < size) {
            ssize_t n = ::pwrite(fd_,
                                 data + total_written,
                                 size - total_written,
                                 current_offset_ + total_written);
            AssertInfo(
                n > 0,
                "Failed to pwrite to LOB spillover file: {} at offset {}",
                path_,
                current_offset_ + total_written);
            total_written += n;
        }

        current_offset_ += size;
        return ref.Encode();
    }

    /**
     * Read text using encoded reference string.
     * Lock-free (pread is thread-safe and data written via pwrite
     * is immediately visible in the kernel page cache).
     */
    std::string
    DecodeAndRead(std::string_view ref_str) {
        TextLobRef ref = TextLobRef::Decode(ref_str);
        return PRead(ref.offset, ref.size);
    }

    /**
     * Batch read: pread all refs. No flush needed.
     */
    std::vector<std::string>
    DecodeAndReadBatch(const std::vector<std::string_view>& ref_strs) {
        std::vector<std::string> results;
        results.reserve(ref_strs.size());
        for (const auto& ref_str : ref_strs) {
            TextLobRef ref = TextLobRef::Decode(ref_str);
            results.push_back(PRead(ref.offset, ref.size));
        }
        return results;
    }

    const std::string&
    GetPath() const {
        return path_;
    }

    FieldId
    GetFieldId() const {
        return field_id_;
    }

    int
    GetFd() const {
        return fd_;
    }

    /**
     * Get the current file size (total bytes written).
     * Used by resource tracking to account for spillover disk usage.
     */
    uint64_t
    GetDiskUsage() const {
        return current_offset_;
    }

 private:
    std::string
    PRead(uint64_t offset, uint32_t size) {
        std::string result(size, '\0');
        size_t total_read = 0;
        while (total_read < size) {
            ssize_t n = ::pread(fd_,
                                result.data() + total_read,
                                size - total_read,
                                offset + total_read);
            AssertInfo(n > 0,
                       "Failed to pread from LOB spillover file: {} at "
                       "offset {}, size {}, read so far {}",
                       path_,
                       offset + total_read,
                       size - total_read,
                       total_read);
            total_read += n;
        }
        return result;
    }

    static std::atomic<uint64_t>&
    InstanceCounter() {
        static std::atomic<uint64_t> counter{0};
        return counter;
    }

    static std::string
    BuildPath(const std::string& base_path,
              int64_t segment_id,
              FieldId field_id) {
        auto seq = InstanceCounter().fetch_add(1, std::memory_order_relaxed);
        std::filesystem::path p(base_path);
        p /= "growing_lob";
        p /= std::to_string(segment_id);
        p /=
            std::to_string(field_id.get()) + "_" + std::to_string(seq) + ".lob";
        return p.string();
    }

 private:
    int64_t segment_id_;
    FieldId field_id_;
    std::string path_;

    int fd_;
    std::mutex write_mutex_;
    uint64_t current_offset_;
};

}  // namespace milvus::segcore
