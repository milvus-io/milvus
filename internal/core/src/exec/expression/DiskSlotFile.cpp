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

#include "exec/expression/DiskSlotFile.h"

#include <sys/stat.h>
#include <cstring>

#include "log/Log.h"
#include "xxhash.h"

namespace milvus {
namespace exec {

DiskSlotFile::DiskSlotFile(int64_t segment_id,
                           const std::string& path,
                           int64_t row_count,
                           uint64_t max_file_size)
    : segment_id_(segment_id), path_(path), row_count_(row_count) {
    // Calculate sizes
    bitset_bytes_ = static_cast<uint32_t>(((row_count + 63) / 64) * 8);
    slot_size_ = kSlotHeaderSize + bitset_bytes_ * 2;  // result + valid

    // Need at least 1 slot
    if (max_file_size <= kFileHeaderSize + slot_size_) {
        num_slots_ = 1;
    } else {
        num_slots_ = static_cast<uint32_t>((max_file_size - kFileHeaderSize) /
                                           slot_size_);
    }

    // Open file (create if not exists)
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        LOG_ERROR(
            "DiskSlotFile: failed to open {}: {}", path_, strerror(errno));
        return;
    }

    // Allocate file space
    size_t file_size =
        kFileHeaderSize + static_cast<size_t>(num_slots_) * slot_size_;
    if (::ftruncate(fd_, static_cast<off_t>(file_size)) != 0) {
        LOG_ERROR("DiskSlotFile: ftruncate failed for {}: {}",
                  path_,
                  strerror(errno));
        ::close(fd_);
        fd_ = -1;
        return;
    }

    // Write file header
    WriteFileHeader();

    // Initialize free slots (all slots start as free)
    free_slots_.reserve(num_slots_);
    for (uint32_t i = 0; i < num_slots_; ++i) {
        free_slots_.push_back(i);
    }

    LOG_DEBUG(
        "DiskSlotFile: created segment_id={} path={} row_count={} "
        "slot_size={} num_slots={} bitset_bytes={}",
        segment_id_,
        path_,
        row_count,
        slot_size_,
        num_slots_,
        bitset_bytes_);
}

DiskSlotFile::~DiskSlotFile() {
    Close();
}

void
DiskSlotFile::WriteFileHeader() {
    FileHeader hdr{};
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.segment_id = segment_id_;
    hdr.slot_size = slot_size_;
    hdr.num_slots = num_slots_;
    hdr.used_count = 0;
    std::memset(hdr.reserved, 0, sizeof(hdr.reserved));

    ssize_t written = ::pwrite(fd_, &hdr, sizeof(hdr), 0);
    if (written != sizeof(hdr)) {
        LOG_ERROR("DiskSlotFile: failed to write file header: {}",
                  strerror(errno));
    }
}

bool
DiskSlotFile::Get(const std::string& signature,
                  int64_t active_count,
                  TargetBitmap& out_result,
                  TargetBitmap& out_valid) {
    std::shared_lock lock(mutex_);

    if (fd_ < 0) {
        return false;
    }

    auto it = slot_index_.find(signature);
    if (it == slot_index_.end()) {
        return false;
    }

    auto& meta = *it->second;

    // Staleness check
    if (meta.active_count != active_count) {
        return false;
    }

    // Bump usage_count (atomic, no lock needed — Clock touch)
    auto old = meta.usage_count.load(std::memory_order_relaxed);
    if (old < 5) {
        meta.usage_count.store(old + 1, std::memory_order_relaxed);
    }

    // Read slot from disk
    std::vector<char> buf(slot_size_);
    off_t offset =
        static_cast<off_t>(kFileHeaderSize) +
        static_cast<off_t>(meta.slot_id) * static_cast<off_t>(slot_size_);

    ssize_t bytes_read = ::pread(fd_, buf.data(), slot_size_, offset);
    if (bytes_read != static_cast<ssize_t>(slot_size_)) {
        LOG_ERROR("DiskSlotFile::Get: pread failed slot_id={}: {}",
                  meta.slot_id,
                  strerror(errno));
        return false;
    }

    // Parse slot header to verify
    SlotHeader slot_hdr;
    std::memcpy(&slot_hdr, buf.data(), sizeof(SlotHeader));
    if (slot_hdr.sig_hash != meta.sig_hash) {
        LOG_ERROR("DiskSlotFile::Get: sig_hash mismatch on disk for slot_id={}",
                  meta.slot_id);
        return false;
    }
    if (slot_hdr.active_count != active_count) {
        LOG_ERROR(
            "DiskSlotFile::Get: active_count mismatch on disk for slot_id={}",
            meta.slot_id);
        return false;
    }

    // Copy result bitset
    out_result = TargetBitmap(row_count_);
    std::memcpy(reinterpret_cast<char*>(out_result.data()),
                buf.data() + kSlotHeaderSize,
                bitset_bytes_);

    // Copy valid bitset
    out_valid = TargetBitmap(row_count_);
    std::memcpy(reinterpret_cast<char*>(out_valid.data()),
                buf.data() + kSlotHeaderSize + bitset_bytes_,
                bitset_bytes_);

    return true;
}

void
DiskSlotFile::Put(const std::string& signature,
                  int64_t active_count,
                  const TargetBitmap& result,
                  const TargetBitmap& valid) {
    uint64_t sig_hash = XXH64(signature.data(), signature.size(), 0);

    std::unique_lock lock(mutex_);

    if (fd_ < 0) {
        return;
    }
    if (result.size() != static_cast<size_t>(row_count_) ||
        valid.size() != static_cast<size_t>(row_count_)) {
        LOG_WARN(
            "DiskSlotFile::Put: row_count mismatch, segment_id={} expected={} "
            "result_size={} valid_size={}",
            segment_id_,
            row_count_,
            result.size(),
            valid.size());
        return;
    }

    auto existing = slot_index_.find(signature);
    if (existing != slot_index_.end()) {
        free_slots_.push_back(existing->second->slot_id);
        slot_index_.erase(existing);
        clock_dirty_ = true;
    }

    // Allocate a slot (may evict)
    uint32_t slot_id = AllocateSlot();

    // Build slot buffer: [SlotHeader][raw bitset data]
    std::vector<char> buf(slot_size_, 0);

    SlotHeader slot_hdr;
    slot_hdr.sig_hash = sig_hash;
    slot_hdr.active_count = active_count;
    slot_hdr.flags = 0;
    std::memcpy(buf.data(), &slot_hdr, sizeof(SlotHeader));

    // Copy result bitset
    size_t result_copy =
        std::min(static_cast<size_t>(bitset_bytes_), result.size_in_bytes());
    std::memcpy(buf.data() + kSlotHeaderSize,
                reinterpret_cast<const char*>(result.data()),
                result_copy);

    // Copy valid bitset
    size_t valid_copy =
        std::min(static_cast<size_t>(bitset_bytes_), valid.size_in_bytes());
    std::memcpy(buf.data() + kSlotHeaderSize + bitset_bytes_,
                reinterpret_cast<const char*>(valid.data()),
                valid_copy);

    // Write to disk
    off_t offset = static_cast<off_t>(kFileHeaderSize) +
                   static_cast<off_t>(slot_id) * static_cast<off_t>(slot_size_);

    ssize_t written = ::pwrite(fd_, buf.data(), slot_size_, offset);
    if (written != static_cast<ssize_t>(slot_size_)) {
        LOG_ERROR("DiskSlotFile::Put: pwrite failed slot_id={}: {}",
                  slot_id,
                  strerror(errno));
        // Put slot back to free list since write failed
        free_slots_.push_back(slot_id);
        return;
    }

    // Insert metadata
    auto meta = std::make_unique<SlotMeta>();
    meta->slot_id = slot_id;
    meta->signature = signature;
    meta->sig_hash = sig_hash;
    meta->active_count = active_count;
    meta->usage_count.store(1, std::memory_order_relaxed);

    slot_index_[signature] = std::move(meta);
    clock_dirty_ = true;

    LOG_DEBUG("DiskSlotFile::Put signature={} slot_id={} active_count={}",
              signature,
              slot_id,
              active_count);
}

uint32_t
DiskSlotFile::AllocateSlot() {
    // Must be called under unique_lock
    if (!free_slots_.empty()) {
        uint32_t slot_id = free_slots_.back();
        free_slots_.pop_back();
        return slot_id;
    }

    // No free slots — evict one via Clock
    EvictOne();

    if (!free_slots_.empty()) {
        uint32_t slot_id = free_slots_.back();
        free_slots_.pop_back();
        return slot_id;
    }

    // Should not happen if EvictOne works correctly, but fallback to slot 0
    LOG_ERROR("DiskSlotFile::AllocateSlot: failed to free a slot, reusing 0");
    return 0;
}

void
DiskSlotFile::EvictOne() {
    // Must be called under unique_lock
    if (slot_index_.empty()) {
        return;
    }

    // Rebuild clock key snapshot if needed
    if (clock_dirty_) {
        RebuildClockKeys();
    }

    if (clock_keys_.empty()) {
        return;
    }

    // Clock sweep: scan entries, decrement usage_count, evict first with 0
    // Worst case: full scan + one more round (all entries accessed)
    size_t max_scan = clock_keys_.size() * 2;
    for (size_t i = 0; i < max_scan; ++i) {
        if (clock_hand_ >= clock_keys_.size()) {
            clock_hand_ = 0;
        }
        std::string key = clock_keys_[clock_hand_];
        auto it = slot_index_.find(key);
        if (it == slot_index_.end()) {
            // Entry was removed but clock_keys_ stale
            clock_hand_++;
            continue;
        }

        auto& meta = *it->second;
        auto count = meta.usage_count.load(std::memory_order_relaxed);
        if (count > 0) {
            // Give it a chance: decrement and move on
            meta.usage_count.store(count - 1, std::memory_order_relaxed);
            clock_hand_++;
        } else {
            // usage_count == 0: evict this entry
            uint32_t freed_slot = meta.slot_id;
            LOG_DEBUG("DiskSlotFile::EvictOne signature={} slot_id={}",
                      key,
                      freed_slot);

            // Remove from clock_keys_ (swap with last for O(1))
            clock_keys_[clock_hand_] = clock_keys_.back();
            clock_keys_.pop_back();
            slot_index_.erase(it);

            // Return slot to free list
            free_slots_.push_back(freed_slot);
            return;
        }
    }

    // If we get here, all entries have high usage_count (all hot).
    // Force evict the one at current clock_hand_.
    if (!clock_keys_.empty()) {
        if (clock_hand_ >= clock_keys_.size()) {
            clock_hand_ = 0;
        }
        std::string key = clock_keys_[clock_hand_];
        auto it = slot_index_.find(key);
        if (it != slot_index_.end()) {
            uint32_t freed_slot = it->second->slot_id;
            clock_keys_[clock_hand_] = clock_keys_.back();
            clock_keys_.pop_back();
            slot_index_.erase(it);
            free_slots_.push_back(freed_slot);
        }
    }
}

void
DiskSlotFile::RebuildClockKeys() {
    clock_keys_.clear();
    clock_keys_.reserve(slot_index_.size());
    for (const auto& [k, _] : slot_index_) {
        clock_keys_.push_back(k);
    }
    clock_hand_ = 0;
    clock_dirty_ = false;
}

void
DiskSlotFile::Close() {
    std::unique_lock lock(mutex_);
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    slot_index_.clear();
    free_slots_.clear();
    clock_keys_.clear();
    clock_hand_ = 0;
    clock_dirty_ = true;
}

uint32_t
DiskSlotFile::GetUsedCount() const {
    std::shared_lock lock(mutex_);
    return static_cast<uint32_t>(slot_index_.size());
}

bool
DiskSlotFile::HasSignature(const std::string& signature) const {
    std::shared_lock lock(mutex_);
    return slot_index_.find(signature) != slot_index_.end();
}

}  // namespace exec
}  // namespace milvus
