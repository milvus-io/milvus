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

#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Types.h"

namespace milvus {
namespace exec {

// Disk-backed expression cache for one sealed segment.
// Fixed-size slots, pread/pwrite IO, Clock eviction.
// No compression — stores raw bitset directly.
// Growing segments are intentionally not supported: slot size is derived from
// the segment row_count at file creation time.
//
// File layout:
//   [FileHeader 64B][slot_0][slot_1]...[slot_N-1]
//
// Each slot has a fixed header (17B) + raw bitset data.
// Slot size = 17 + ((row_count + 63) / 64) * 8
// All slots in one file are the same size.

class DiskSlotFile {
 public:
    static constexpr uint32_t kMagic = 0x45584352;  // "EXCR"
    static constexpr uint32_t kVersion = 2;
    static constexpr size_t kFileHeaderSize = 64;
    static constexpr size_t kSlotHeaderSize =
        17;  // sig_hash(8) + active_count(8) + flags(1)

#pragma pack(push, 1)
    struct FileHeader {
        uint32_t magic;
        uint32_t version;
        int64_t segment_id;
        uint32_t slot_size;
        uint32_t num_slots;
        uint32_t used_count;
        char reserved[36];
    };

    struct SlotHeader {
        uint64_t sig_hash;
        int64_t active_count;
        uint8_t flags;  // bit 0 = valid_all_ones
    };
#pragma pack(pop)

    static_assert(sizeof(FileHeader) == 64, "FileHeader must be 64 bytes");
    static_assert(sizeof(SlotHeader) == 17, "SlotHeader must be 17 bytes");

    // Create/open a disk slot file for a sealed segment.
    // row_count determines slot size. max_file_size determines number of slots.
    DiskSlotFile(int64_t segment_id,
                 const std::string& path,
                 int64_t row_count,
                 uint64_t max_file_size);

    ~DiskSlotFile();

    // Non-copyable, non-movable
    DiskSlotFile(const DiskSlotFile&) = delete;
    DiskSlotFile&
    operator=(const DiskSlotFile&) = delete;

    // Get cached bitsets. Returns false on miss/mismatch.
    bool
    Get(const std::string& signature,
        int64_t active_count,
        TargetBitmap& out_result,
        TargetBitmap& out_valid);

    // Store raw bitsets into a slot. May evict via Clock if full.
    void
    Put(const std::string& signature,
        int64_t active_count,
        const TargetBitmap& result,
        const TargetBitmap& valid);

    void
    Close();

    uint32_t
    GetNumSlots() const {
        return num_slots_;
    }

    uint32_t
    GetUsedCount() const;

    bool
    HasSignature(const std::string& signature) const;

    uint32_t
    GetSlotSize() const {
        return slot_size_;
    }

    int64_t
    GetRowCount() const {
        return row_count_;
    }

 private:
    struct SlotMeta {
        uint32_t slot_id;
        std::string signature;
        uint64_t sig_hash;
        int64_t active_count;
        std::atomic<uint8_t> usage_count{0};
    };

    void
    WriteFileHeader();

    uint32_t
    AllocateSlot();  // find free or evict, returns slot_id

    void
    EvictOne();  // Clock sweep

    void
    RebuildClockKeys();

    int64_t segment_id_;
    std::string path_;
    int64_t row_count_;
    int fd_{-1};
    uint32_t slot_size_{0};
    uint32_t num_slots_{0};
    uint32_t bitset_bytes_{0};  // raw bitset size = ((row_count+63)/64)*8

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<SlotMeta>> slot_index_;
    std::vector<uint32_t> free_slots_;  // available slot IDs

    // Clock state
    std::vector<std::string> clock_keys_;
    uint32_t clock_hand_{0};
    bool clock_dirty_{true};
};

}  // namespace exec
}  // namespace milvus
