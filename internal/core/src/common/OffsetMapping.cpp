#include "common/OffsetMapping.h"

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus {
namespace {

bool
ShouldSkipBitsetTransform(const BitsetView& bitset,
                          int64_t total_count,
                          TargetBitmap& result,
                          OffsetMapping::BitsetTransformStatus& status) {
    result.clear();
    if (bitset.empty()) {
        status = OffsetMapping::BitsetTransformStatus::NoFilter;
        return true;
    }
    if (bitset.all()) {
        status = OffsetMapping::BitsetTransformStatus::AllFiltered;
        return true;
    }
    if (static_cast<int64_t>(bitset.size()) >= total_count && bitset.none()) {
        status = OffsetMapping::BitsetTransformStatus::NoFilter;
        return true;
    }
    return false;
}

}  // namespace

int64_t
OffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    return logical_offset;
}

int64_t
OffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    return physical_offset;
}

bool
OffsetMapping::IsValid(int64_t logical_offset) const {
    return GetPhysicalOffset(logical_offset) >= 0;
}

int64_t
OffsetMapping::GetValidCount() const {
    return 0;
}

bool
OffsetMapping::IsEnabled() const {
    return false;
}

int64_t
OffsetMapping::GetTotalCount() const {
    return 0;
}

OffsetMapping::BitsetTransformStatus
OffsetMapping::TransformBitset(const BitsetView& bitset,
                               TargetBitmap& result) const {
    (void)bitset;
    result.clear();
    return BitsetTransformStatus::NoFilter;
}

void
OffsetMapping::TransformOffsets(std::vector<int64_t>& offsets) const {
    (void)offsets;
}

void
OffsetMapping::TransformLogicalOffsets(std::vector<int64_t>& offsets) const {
    (void)offsets;
}

void
OffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    physical_offsets.clear();
    physical_offsets.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        const bool valid = logical_offsets[i] >= 0;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(logical_offsets[i]);
        }
    }
}

void
SealedOffsetMapping::Build(const bool* valid_data, int64_t total_count) {
    constexpr int64_t start_logical = 0;
    constexpr int64_t start_physical = 0;
    if (total_count == 0 || valid_data == nullptr) {
        return;
    }

    enabled_ = true;
    total_count_ = start_logical + total_count;
    valid_count_ = 0;
    l2p_vec_.clear();
    p2l_vec_.clear();
    l2p_map_.clear();
    p2l_map_.clear();

    int64_t valid_count = 0;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            ++valid_count;
        }
    }

    use_map_ = valid_count * 10 < total_count;

    if (use_map_) {
        int64_t physical_idx = start_physical;
        for (int64_t i = 0; i < total_count; ++i) {
            if (valid_data[i]) {
                l2p_map_[start_logical + i] = physical_idx;
                p2l_map_[physical_idx] = start_logical + i;
                ++physical_idx;
            }
        }
    } else {
        const int64_t required_size = start_logical + total_count;
        l2p_vec_.resize(required_size, -1);

        const int64_t required_p2l_size = start_physical + valid_count;
        p2l_vec_.resize(required_p2l_size, -1);

        int64_t physical_idx = start_physical;
        for (int64_t i = 0; i < total_count; ++i) {
            if (valid_data[i]) {
                l2p_vec_[start_logical + i] = physical_idx;
                p2l_vec_[physical_idx] = start_logical + i;
                ++physical_idx;
            } else {
                l2p_vec_[start_logical + i] = -1;
            }
        }
    }

    valid_count_ = valid_count;
}

int64_t
SealedOffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    return GetPhysicalOffsetInternal(logical_offset);
}

int64_t
SealedOffsetMapping::GetPhysicalOffsetInternal(int64_t logical_offset) const {
    if (!enabled_) {
        return logical_offset;
    }
    if (logical_offset < 0 || logical_offset >= total_count_) {
        return -1;
    }
    if (use_map_) {
        auto it = l2p_map_.find(static_cast<int32_t>(logical_offset));
        return it == l2p_map_.end() ? -1 : it->second;
    }
    return logical_offset < static_cast<int64_t>(l2p_vec_.size())
               ? l2p_vec_[logical_offset]
               : -1;
}

int64_t
SealedOffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    return GetLogicalOffsetInternal(physical_offset);
}

int64_t
SealedOffsetMapping::GetLogicalOffsetInternal(int64_t physical_offset) const {
    if (!enabled_) {
        return physical_offset;
    }
    if (physical_offset < 0 || physical_offset >= valid_count_) {
        return -1;
    }
    if (use_map_) {
        auto it = p2l_map_.find(static_cast<int32_t>(physical_offset));
        return it == p2l_map_.end() ? -1 : it->second;
    }
    return physical_offset < static_cast<int64_t>(p2l_vec_.size())
               ? p2l_vec_[physical_offset]
               : -1;
}

int64_t
SealedOffsetMapping::GetValidCount() const {
    return valid_count_;
}

bool
SealedOffsetMapping::IsEnabled() const {
    return enabled_;
}

int64_t
SealedOffsetMapping::GetTotalCount() const {
    return total_count_;
}

OffsetMapping::BitsetTransformStatus
SealedOffsetMapping::TransformBitset(const BitsetView& bitset,
                                     TargetBitmap& result) const {
    if (!enabled_) {
        result.clear();
        return BitsetTransformStatus::NoFilter;
    }
    BitsetTransformStatus status;
    if (ShouldSkipBitsetTransform(bitset, total_count_, result, status)) {
        return status;
    }

    result.resize(valid_count_, true);
    if (use_map_) {
        for (int64_t physical_idx = 0; physical_idx < valid_count_;
             ++physical_idx) {
            auto it = p2l_map_.find(static_cast<int32_t>(physical_idx));
            if (it != p2l_map_.end() &&
                it->second < static_cast<int64_t>(bitset.size())) {
                result[physical_idx] = bitset.test(it->second);
            }
        }
    } else {
        for (int64_t physical_idx = 0; physical_idx < valid_count_;
             ++physical_idx) {
            auto logical_idx = p2l_vec_[physical_idx];
            if (logical_idx >= 0 &&
                logical_idx < static_cast<int64_t>(bitset.size())) {
                result[physical_idx] = bitset.test(logical_idx);
            }
        }
    }
    return BitsetTransformStatus::Transformed;
}

void
SealedOffsetMapping::TransformOffsets(std::vector<int64_t>& offsets) const {
    if (!enabled_) {
        return;
    }
    for (auto& offset : offsets) {
        if (offset >= 0) {
            offset = GetLogicalOffsetInternal(offset);
        }
    }
}

void
SealedOffsetMapping::TransformLogicalOffsets(
    std::vector<int64_t>& offsets) const {
    if (!enabled_) {
        return;
    }
    for (auto& offset : offsets) {
        if (offset >= 0) {
            offset = GetPhysicalOffsetInternal(offset);
        }
    }
}

void
SealedOffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    physical_offsets.clear();
    physical_offsets.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        const auto physical_offset =
            GetPhysicalOffsetInternal(logical_offsets[i]);
        const bool valid = physical_offset >= 0;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(physical_offset);
        }
    }
}

void
GrowingOffsetMapping::Append(const bool* valid_data,
                             int64_t count,
                             int64_t start_logical,
                             int64_t start_physical) {
    if (count == 0 || valid_data == nullptr) {
        return;
    }

    std::unique_lock lock(mutex_);
    if (start_logical < 0) {
        start_logical = total_count_;
    }
    auto physical_idx = start_physical >= 0 ? start_physical : valid_count_;
    for (int64_t i = 0; i < count; ++i) {
        if (valid_data[i]) {
            const auto logical_offset = start_logical + i;
            auto l2p_result =
                l2p_map_.emplace(static_cast<int32_t>(logical_offset),
                                 static_cast<int32_t>(physical_idx));
            AssertInfo(l2p_result.second,
                       "duplicate logical offset {} in growing offset mapping",
                       logical_offset);
            auto p2l_result =
                p2l_map_.emplace(static_cast<int32_t>(physical_idx),
                                 static_cast<int32_t>(logical_offset));
            AssertInfo(p2l_result.second,
                       "duplicate physical offset {} in growing offset mapping",
                       physical_idx);
            ++physical_idx;
        }
    }

    total_count_ = std::max(total_count_, start_logical + count);
    valid_count_ = physical_idx;
    enabled_ = true;
}

int64_t
GrowingOffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    std::shared_lock lock(mutex_);
    if (!enabled_) {
        return logical_offset;
    }
    return GetPhysicalOffsetInternal(logical_offset, total_count_);
}

int64_t
GrowingOffsetMapping::GetPhysicalOffsetInternal(int64_t logical_offset,
                                                int64_t total_count) const {
    if (logical_offset < 0 || logical_offset >= total_count) {
        return -1;
    }
    auto it = l2p_map_.find(static_cast<int32_t>(logical_offset));
    return it == l2p_map_.end() ? -1 : it->second;
}

int64_t
GrowingOffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    std::shared_lock lock(mutex_);
    if (!enabled_) {
        return physical_offset;
    }
    return GetLogicalOffsetInternal(physical_offset, valid_count_);
}

int64_t
GrowingOffsetMapping::GetLogicalOffsetInternal(int64_t physical_offset,
                                               int64_t valid_count) const {
    if (physical_offset < 0 || physical_offset >= valid_count) {
        return -1;
    }
    auto it = p2l_map_.find(static_cast<int32_t>(physical_offset));
    return it == p2l_map_.end() ? -1 : it->second;
}

int64_t
GrowingOffsetMapping::GetValidCount() const {
    std::shared_lock lock(mutex_);
    return valid_count_;
}

bool
GrowingOffsetMapping::IsEnabled() const {
    std::shared_lock lock(mutex_);
    return enabled_;
}

int64_t
GrowingOffsetMapping::GetTotalCount() const {
    std::shared_lock lock(mutex_);
    return total_count_;
}

OffsetMapping::BitsetTransformStatus
GrowingOffsetMapping::TransformBitset(const BitsetView& bitset,
                                      TargetBitmap& result) const {
    std::shared_lock lock(mutex_);
    if (!enabled_) {
        result.clear();
        return BitsetTransformStatus::NoFilter;
    }
    const auto valid_count = valid_count_;
    const auto total_count = total_count_;
    BitsetTransformStatus status;
    if (ShouldSkipBitsetTransform(bitset, total_count, result, status)) {
        return status;
    }

    result.resize(valid_count, true);
    for (int64_t physical_idx = 0; physical_idx < valid_count; ++physical_idx) {
        auto it = p2l_map_.find(static_cast<int32_t>(physical_idx));
        if (it != p2l_map_.end() &&
            it->second < static_cast<int64_t>(bitset.size())) {
            result[physical_idx] = bitset.test(it->second);
        }
    }
    return BitsetTransformStatus::Transformed;
}

void
GrowingOffsetMapping::TransformOffsets(std::vector<int64_t>& offsets) const {
    std::shared_lock lock(mutex_);
    if (!enabled_) {
        return;
    }
    const auto valid_count = valid_count_;
    for (auto& offset : offsets) {
        if (offset >= 0) {
            offset = GetLogicalOffsetInternal(offset, valid_count);
        }
    }
}

void
GrowingOffsetMapping::TransformLogicalOffsets(
    std::vector<int64_t>& offsets) const {
    std::shared_lock lock(mutex_);
    if (!enabled_) {
        return;
    }
    const auto total_count = total_count_;
    for (auto& offset : offsets) {
        if (offset >= 0) {
            offset = GetPhysicalOffsetInternal(offset, total_count);
        }
    }
}

void
GrowingOffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    std::shared_lock lock(mutex_);
    physical_offsets.clear();
    physical_offsets.reserve(count);
    const auto total_count = total_count_;
    for (int64_t i = 0; i < count; ++i) {
        const auto physical_offset =
            GetPhysicalOffsetInternal(logical_offsets[i], total_count);
        const bool valid = physical_offset >= 0;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(physical_offset);
        }
    }
}

}  // namespace milvus
