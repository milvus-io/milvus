#include "common/OffsetMapping.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace milvus {
namespace {

constexpr size_t kInitialP2LBufferCapacity = 8192;

size_t
NextP2LBufferCapacity(size_t required) {
    auto capacity = kInitialP2LBufferCapacity;
    while (capacity < required) {
        capacity *= 2;
    }
    return capacity;
}

}  // namespace

struct OffsetMappingP2LBuffer {
    explicit OffsetMappingP2LBuffer(size_t capacity)
        : capacity(capacity), data(std::make_unique<int32_t[]>(capacity)) {
    }

    size_t capacity;
    std::unique_ptr<int32_t[]> data;
};

OffsetMappingSnapshot::OffsetMappingSnapshot(
    std::shared_ptr<const OffsetMappingP2LBuffer> p2l,
    bool enabled,
    int64_t valid_count,
    int64_t total_count)
    : p2l_(std::move(p2l)),
      enabled_(enabled),
      valid_count_(valid_count),
      total_count_(total_count) {
}

bool
OffsetMappingSnapshot::IsEnabled() const {
    return enabled_;
}

int64_t
OffsetMappingSnapshot::GetValidCount() const {
    return valid_count_;
}

int64_t
OffsetMappingSnapshot::GetTotalCount() const {
    return total_count_;
}

int64_t
OffsetMappingSnapshot::GetVisiblePhysicalCount(int64_t logical_count) const {
    if (!enabled_) {
        return logical_count;
    }
    if (logical_count <= 0 || p2l_ == nullptr || valid_count_ == 0) {
        return 0;
    }
    if (logical_count >= total_count_) {
        return valid_count_;
    }
    const auto* begin = p2l_->data.get();
    const auto* end = begin + valid_count_;
    return std::lower_bound(begin, end, static_cast<int32_t>(logical_count)) -
           begin;
}

int64_t
OffsetMappingSnapshot::GetPhysicalOffset(int64_t logical_offset) const {
    if (!enabled_) {
        return logical_offset;
    }
    if (p2l_ == nullptr || logical_offset < 0 ||
        logical_offset >= total_count_) {
        return -1;
    }
    const auto* begin = p2l_->data.get();
    const auto* end = begin + valid_count_;
    const auto it =
        std::lower_bound(begin, end, static_cast<int32_t>(logical_offset));
    if (it == end || *it != logical_offset) {
        return -1;
    }
    return it - begin;
}

int64_t
OffsetMappingSnapshot::GetLogicalOffset(int64_t physical_offset) const {
    if (!enabled_) {
        return physical_offset;
    }
    if (p2l_ == nullptr || physical_offset < 0 ||
        physical_offset >= valid_count_) {
        return -1;
    }
    return p2l_->data[physical_offset];
}

bool
OffsetMappingSnapshot::IsValid(int64_t logical_offset) const {
    return GetPhysicalOffset(logical_offset) >= 0;
}

const int32_t*
OffsetMappingSnapshot::GetLogicalOffsets(int64_t physical_offset,
                                         size_t count) const {
    if (!enabled_ || p2l_ == nullptr || physical_offset < 0 || count == 0) {
        return nullptr;
    }
    const auto offset = static_cast<size_t>(physical_offset);
    if (offset > static_cast<size_t>(valid_count_) ||
        count > static_cast<size_t>(valid_count_) - offset) {
        return nullptr;
    }
    return p2l_->data.get() + offset;
}

OffsetMappingAppendResult
OffsetMapping::Append(const bool* valid_data,
                      int64_t count,
                      int64_t start_logical) {
    if (count == 0 || valid_data == nullptr) {
        return {};
    }

    std::unique_lock lock(mutex_);
    if (start_logical < 0) {
        start_logical = total_count_;
    }
    auto physical_idx = valid_count_;
    const auto append_physical_offset = physical_idx;
    const auto required_logical_size = start_logical + count;
    int64_t batch_valid_count = 0;
    for (int64_t i = 0; i < count; ++i) {
        if (valid_data[i]) {
            ++batch_valid_count;
        }
    }
    if (batch_valid_count != 0) {
        const auto required_physical_size = physical_idx + batch_valid_count;
        if (p2l_ == nullptr ||
            static_cast<int64_t>(p2l_->capacity) < required_physical_size) {
            auto new_buffer =
                std::make_shared<OffsetMappingP2LBuffer>(NextP2LBufferCapacity(
                    static_cast<size_t>(required_physical_size)));
            if (p2l_ != nullptr && valid_count_ > 0) {
                std::memcpy(
                    new_buffer->data.get(),
                    p2l_->data.get(),
                    static_cast<size_t>(valid_count_) * sizeof(int32_t));
            }
            p2l_ = std::move(new_buffer);
        }
    }

    for (int64_t i = 0; i < count; ++i) {
        const auto logical_offset = start_logical + i;
        if (valid_data[i]) {
            p2l_->data[physical_idx] = static_cast<int32_t>(logical_offset);
            ++physical_idx;
        }
    }

    total_count_ = std::max(total_count_, required_logical_size);
    valid_count_ = std::max(valid_count_, physical_idx);
    enabled_ = true;
    return {append_physical_offset, batch_valid_count};
}

OffsetMappingSnapshot
OffsetMapping::GetSnapshot() const {
    std::shared_lock lock(mutex_);
    return OffsetMappingSnapshot{p2l_, enabled_, valid_count_, total_count_};
}

}  // namespace milvus
