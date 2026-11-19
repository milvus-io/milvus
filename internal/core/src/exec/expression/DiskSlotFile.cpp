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

#include <folly/ScopeGuard.h>
#include <sys/stat.h>
#include <cstring>
#include <cassert>

#include "cachinglayer/Metrics.h"

#include "log/Log.h"
#include "xxhash.h"

namespace milvus {
namespace exec {

ExprCacheDiskBudget::ExprCacheDiskBudget(size_t capacity)
    : capacity_(capacity),
      gauge_(cachinglayer::monitor::cache_loaded_bytes(
          cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::DISK)) {
}

bool
ExprCacheDiskBudget::TryAcquire(size_t bytes) {
    std::lock_guard lock(mutex_);
    // Preserve the existing zero-as-unlimited disk configuration.
    if (capacity_ != 0 && (bytes > capacity_ || used_ > capacity_ - bytes)) {
        return false;
    }
    used_ += bytes;
    gauge_.Increment(bytes);
    return true;
}

void
ExprCacheDiskBudget::Release(size_t bytes) noexcept {
    std::lock_guard lock(mutex_);
    assert(bytes <= used_);
    used_ -= bytes;
    gauge_.Decrement(bytes);
}

void
ExprCacheDiskBudget::SetCapacity(size_t capacity) {
    std::lock_guard lock(mutex_);
    capacity_ = capacity;
}

size_t
ExprCacheDiskBudget::GetUsedBytes() const {
    std::lock_guard lock(mutex_);
    return used_;
}

DiskSlotFile::Reservation::Reservation(
    std::shared_ptr<ExprCacheDiskBudget> budget,
    std::function<bool(size_t)> acquire)
    : budget_(std::move(budget)), acquire_(std::move(acquire)) {
}

DiskSlotFile::Reservation::~Reservation() {
    budget_->Release(bytes_);
}

bool
DiskSlotFile::Reservation::Grow(size_t bytes) {
    if (!acquire_(bytes)) {
        return false;
    }
    bytes_ += bytes;
    return true;
}

DiskSlotFile::DiskSlotFile(int64_t segment_id,
                           const std::string& path,
                           int64_t row_count,
                           uint64_t max_file_size,
                           std::unique_ptr<Reservation> reservation)
    : reservation_(std::move(reservation)),
      segment_id_(segment_id),
      path_(path),
      row_count_(row_count) {
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

    if (reservation_ && !reservation_->Grow(kFileHeaderSize)) {
        return;
    }

    // Open file (create if not exists)
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        LOG_ERROR(
            "DiskSlotFile: failed to open {}: {}", path_, strerror(errno));
        return;
    }

    // The destructor does not run if construction throws after opening the
    // file (for example, while formatting a log message).
    auto close_on_failure = folly::makeGuard([&]() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        ::unlink(path_.c_str());
    });

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

    LOG_DEBUG(
        "DiskSlotFile: created segment_id={} path={} row_count={} "
        "slot_size={} num_slots={} bitset_bytes={}",
        segment_id_,
        path_,
        row_count,
        slot_size_,
        num_slots_,
        bitset_bytes_);
    close_on_failure.dismiss();
}

DiskSlotFile::~DiskSlotFile() {
    Remove();
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
DiskSlotFile::Get(ExprCacheSignature signature,
                  int64_t active_count,
                  TargetBitmap& out_result,
                  TargetBitmap& out_valid,
                  const std::function<bool()>& before_materialize,
                  const std::span<const int32_t>* offsets) {
    std::shared_lock lock(mutex_);

    if (fd_ < 0) {
        return false;
    }

    // Exact canonical identity is checked by the map before selecting a slot.
    // The on-disk hash below is an additional header check, not identity proof.
    auto it = slot_index_.find(signature);
    if (it == slot_index_.end()) {
        return false;
    }

    auto& meta = *it->second;

    // Staleness check
    if (meta.active_count != active_count) {
        return false;
    }

    if (before_materialize && !before_materialize()) {
        return false;
    }

    // Bump usage_count (atomic, no lock needed — Clock touch)
    auto old = meta.usage_count.load(std::memory_order_relaxed);
    if (old < 5) {
        meta.usage_count.store(old + 1, std::memory_order_relaxed);
    }

    off_t offset =
        static_cast<off_t>(kFileHeaderSize) +
        static_cast<off_t>(meta.slot_id) * static_cast<off_t>(slot_size_);

    SlotHeader slot_hdr;
    ssize_t bytes_read = ::pread(fd_, &slot_hdr, sizeof(SlotHeader), offset);
    if (bytes_read != static_cast<ssize_t>(sizeof(SlotHeader))) {
        LOG_ERROR("DiskSlotFile::Get: pread header failed slot_id={}: {}",
                  meta.slot_id,
                  strerror(errno));
        return false;
    }
    if (slot_hdr.sig_low != meta.sig_hash.low ||
        slot_hdr.sig_high != meta.sig_hash.high) {
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

    if (offsets != nullptr) {
        for (auto position : *offsets) {
            if (position < 0 || position >= row_count_) {
                return false;
            }
        }
        // Read each touched word once even for unsorted or repeated offsets.
        std::unordered_map<uint32_t, std::pair<uint64_t, uint64_t>> words;
        TargetBitmap selected(offsets->size(), false);
        TargetBitmap selected_valid(offsets->size(), false);
        for (size_t i = 0; i < offsets->size(); ++i) {
            const auto position = (*offsets)[i];
            const uint32_t word_id = position / 64;
            auto [word, inserted] = words.try_emplace(word_id);
            if (inserted) {
                const auto word_offset = offset + kSlotHeaderSize + word_id * 8;
                if (::pread(fd_, &word->second.first, 8, word_offset) != 8 ||
                    ::pread(fd_,
                            &word->second.second,
                            8,
                            word_offset + bitset_bytes_) != 8) {
                    return false;
                }
            }
            selected[i] = (word->second.first >> (position % 64)) & 1;
            selected_valid[i] = (word->second.second >> (position % 64)) & 1;
        }
        out_result = std::move(selected);
        out_valid = std::move(selected_valid);
        return true;
    }

    out_result = TargetBitmap(row_count_);
    bytes_read = ::pread(fd_,
                         reinterpret_cast<char*>(out_result.data()),
                         bitset_bytes_,
                         offset + static_cast<off_t>(kSlotHeaderSize));
    if (bytes_read != static_cast<ssize_t>(bitset_bytes_)) {
        LOG_ERROR("DiskSlotFile::Get: pread result failed slot_id={}: {}",
                  meta.slot_id,
                  strerror(errno));
        return false;
    }

    out_valid = TargetBitmap(row_count_);
    bytes_read =
        ::pread(fd_,
                reinterpret_cast<char*>(out_valid.data()),
                bitset_bytes_,
                offset + static_cast<off_t>(kSlotHeaderSize + bitset_bytes_));
    if (bytes_read != static_cast<ssize_t>(bitset_bytes_)) {
        LOG_ERROR("DiskSlotFile::Get: pread valid failed slot_id={}: {}",
                  meta.slot_id,
                  strerror(errno));
        return false;
    }

    return true;
}

bool
DiskSlotFile::Contains(ExprCacheSignature signature,
                       int64_t active_count) const {
    std::shared_lock lock(mutex_);
    const auto it = slot_index_.find(signature);
    return fd_ >= 0 && it != slot_index_.end() &&
           it->second->active_count == active_count;
}

void
DiskSlotFile::Put(ExprCacheSignature signature,
                  int64_t active_count,
                  const TargetBitmap& result,
                  const TargetBitmap& valid,
                  const std::function<void()>& before_write) {
    if (active_count != row_count_ || result.size() != row_count_ ||
        valid.size() != row_count_) {
        return;
    }
    std::lock_guard writer_lock(writer_mutex_);
    std::unique_lock lock(mutex_);
    if (fd_ < 0) {
        return;
    }
    auto existing = slot_index_.find(signature);
    // A replacement must preserve the old slot until publication. With no
    // spare slot, keep the existing deterministic snapshot result.
    if (existing != slot_index_.end() && free_slots_.empty() &&
        next_unused_slot_ >= num_slots_) {
        return;
    }
    free_slots_.reserve(free_slots_.size() + 2);
    auto meta = std::make_unique<SlotMeta>();
    const auto slot_id = AllocateSlot();
    if (slot_id >= num_slots_) {
        return;
    }
    auto recycle = folly::makeGuard([&] {
        if (!lock.owns_lock()) {
            lock.lock();
        }
        free_slots_.push_back(slot_id);
    });
    meta->slot_id = slot_id;
    meta->sig_hash = signature;
    meta->active_count = active_count;
    meta->usage_count.store(1, std::memory_order_relaxed);
    // Readers can continue using every published slot while this writer
    // prepares and writes an unpublished slot.
    lock.unlock();
    std::vector<char> buffer(slot_size_, 0);
    const SlotHeader header{signature.low, signature.high, active_count, 0};
    std::memcpy(buffer.data(), &header, sizeof(header));
    std::memcpy(buffer.data() + kSlotHeaderSize, result.data(), bitset_bytes_);
    std::memcpy(buffer.data() + kSlotHeaderSize + bitset_bytes_,
                valid.data(),
                bitset_bytes_);
    if (before_write) {
        before_write();
    }
    // Charge each allocated slot once, including unpublished/partially written
    // slots. Recycled slots keep their charge: their disk blocks still exist.
    if (reservation_ && slot_id >= reserved_slots_) {
        const auto slots = slot_id + 1 - reserved_slots_;
        if (!reservation_->Grow(static_cast<size_t>(slots) * slot_size_)) {
            return;
        }
        reserved_slots_ = slot_id + 1;
    }
    const off_t offset =
        kFileHeaderSize + static_cast<off_t>(slot_id) * slot_size_;
    if (::pwrite(fd_, buffer.data(), buffer.size(), offset) !=
        static_cast<ssize_t>(buffer.size())) {
        return;
    }
    lock.lock();
    existing = slot_index_.find(signature);
    if (existing != slot_index_.end()) {
        free_slots_.push_back(existing->second->slot_id);
        existing->second = std::move(meta);
    } else {
        slot_index_.emplace(signature, std::move(meta));
    }
    used_count_.store(slot_index_.size(), std::memory_order_relaxed);
    clock_dirty_ = true;
    recycle.dismiss();
}

uint32_t
DiskSlotFile::AllocateSlot() {
    // Must be called under unique_lock
    if (!free_slots_.empty()) {
        uint32_t slot_id = free_slots_.back();
        free_slots_.pop_back();
        return slot_id;
    }

    // Keep room to recycle a slot on write failure or eviction.
    free_slots_.reserve(1);
    if (next_unused_slot_ < num_slots_) {
        return next_unused_slot_++;
    }

    // All slots have been allocated — evict one via Clock.
    EvictOne();

    if (!free_slots_.empty()) {
        uint32_t slot_id = free_slots_.back();
        free_slots_.pop_back();
        return slot_id;
    }

    return num_slots_;  // No writable unpublished slot.
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
        auto key = clock_keys_[clock_hand_];
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
            used_count_.store(slot_index_.size(), std::memory_order_relaxed);

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
        auto key = clock_keys_[clock_hand_];
        auto it = slot_index_.find(key);
        if (it != slot_index_.end()) {
            uint32_t freed_slot = it->second->slot_id;
            clock_keys_[clock_hand_] = clock_keys_.back();
            clock_keys_.pop_back();
            slot_index_.erase(it);
            used_count_.store(slot_index_.size(), std::memory_order_relaxed);
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
DiskSlotFile::Remove() noexcept {
    Close();
    // Deletion is best effort; a leftover file has no lookup metadata and is
    // removed by the existing cache-directory cleanup on backend rebuild.
    ::unlink(path_.c_str());
}

void
DiskSlotFile::Close() {
    std::lock_guard writer_lock(writer_mutex_);
    std::unique_lock lock(mutex_);
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    slot_index_.clear();
    used_count_.store(0, std::memory_order_relaxed);
    free_slots_.clear();
    next_unused_slot_ = 0;
    clock_keys_.clear();
    clock_hand_ = 0;
    clock_dirty_ = true;
}

uint32_t
DiskSlotFile::GetUsedCount() const {
    return used_count_.load(std::memory_order_relaxed);
}

uint64_t
DiskSlotFile::GetUsedBytes() const {
    return kFileHeaderSize + static_cast<uint64_t>(GetUsedCount()) * slot_size_;
}

}  // namespace exec
}  // namespace milvus
