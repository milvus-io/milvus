#include "common/OffsetMapping.h"

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "common/EasyAssert.h"
#include "storage/MmapManager.h"

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

void
OffsetMappingArray::Resize(
    size_t size,
    int32_t value,
    bool enable_mmap,
    const storage::MmapChunkManagerPtr& mmap_chunk_manager,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    Clear();
    if (size == 0) {
        return;
    }

    if (enable_mmap) {
        AssertInfo(mmap_chunk_manager != nullptr,
                   "offset mapping mmap chunk manager is null");
        AssertInfo(mmap_descriptor != nullptr,
                   "offset mapping mmap descriptor is null");
        mmap_chunk_manager_ = mmap_chunk_manager;
        mmap_descriptor_ = mmap_descriptor;
        mmap_data_ = static_cast<int32_t*>(mmap_chunk_manager_->Allocate(
            mmap_descriptor_, sizeof(int32_t) * size));
        AssertInfo(mmap_data_ != nullptr,
                   "failed to allocate offset mapping mmap buffer, size: {}",
                   sizeof(int32_t) * size);
        size_ = size;
        std::fill_n(mmap_data_, size_, value);
        return;
    }

    vec_.assign(size, value);
    size_ = size;
}

void
OffsetMappingArray::Clear() {
    size_ = 0;
    vec_.clear();
    mmap_data_ = nullptr;
    mmap_descriptor_ = nullptr;
    mmap_chunk_manager_ = nullptr;
}

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

bool
OffsetMapping::IsMmap() const {
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
SealedOffsetMapping::Build(const bool* valid_data,
                           int64_t total_count,
                           const OffsetMappingBuildOptions& options) {
    constexpr int64_t start_logical = 0;
    constexpr int64_t start_physical = 0;

    valid_count_ = 0;
    total_count_ = 0;
    enabled_ = false;
    use_i2o_map_ = false;
    use_o2i_map_ = false;
    l2p_vec_.Clear();
    p2l_vec_.Clear();
    l2p_map_.clear();
    p2l_map_.clear();

    if (total_count == 0 || valid_data == nullptr) {
        return;
    }

    enabled_ = true;
    total_count_ = start_logical + total_count;

    int64_t valid_count = 0;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            ++valid_count;
        }
    }

    const bool use_sparse_mapping = valid_count * 10 < total_count;
    use_i2o_map_ = !options.enable_mmap_i2o_map && use_sparse_mapping;
    use_o2i_map_ = !options.enable_mmap_o2i_map && use_sparse_mapping;

    const int64_t required_size = start_logical + total_count;
    const int64_t required_p2l_size = start_physical + valid_count;

    auto build_options = options;
    const bool need_o2i_mmap =
        !use_o2i_map_ && build_options.enable_mmap_o2i_map && required_size > 0;
    const bool need_i2o_mmap = !use_i2o_map_ &&
                               build_options.enable_mmap_i2o_map &&
                               required_p2l_size > 0;
    if (need_i2o_mmap || need_o2i_mmap) {
        if (build_options.mmap_chunk_manager == nullptr) {
            build_options.mmap_chunk_manager =
                storage::MmapManager::GetInstance().GetMmapChunkManager();
        }
    }
    if (need_o2i_mmap && build_options.o2i_mmap_descriptor == nullptr) {
        build_options.o2i_mmap_descriptor =
            build_options.mmap_chunk_manager->Register();
    }
    if (need_i2o_mmap && build_options.i2o_mmap_descriptor == nullptr) {
        build_options.i2o_mmap_descriptor =
            build_options.mmap_chunk_manager->Register();
    }
    if (need_i2o_mmap && need_o2i_mmap) {
        AssertInfo(build_options.i2o_mmap_descriptor !=
                       build_options.o2i_mmap_descriptor,
                   "offset mapping i2o and o2i mmap descriptors must be "
                   "different");
    }

    if (!use_o2i_map_) {
        l2p_vec_.Resize(required_size,
                        -1,
                        build_options.enable_mmap_o2i_map,
                        build_options.mmap_chunk_manager,
                        build_options.o2i_mmap_descriptor);
    }

    if (!use_i2o_map_) {
        p2l_vec_.Resize(required_p2l_size,
                        -1,
                        build_options.enable_mmap_i2o_map,
                        build_options.mmap_chunk_manager,
                        build_options.i2o_mmap_descriptor);
    }

    int64_t physical_idx = start_physical;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            if (use_o2i_map_) {
                l2p_map_[start_logical + i] = physical_idx;
            } else {
                l2p_vec_[start_logical + i] = physical_idx;
            }
            if (use_i2o_map_) {
                p2l_map_[physical_idx] = start_logical + i;
            } else {
                p2l_vec_[physical_idx] = start_logical + i;
            }
            ++physical_idx;
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
    if (use_o2i_map_) {
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
    if (use_i2o_map_) {
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

bool
SealedOffsetMapping::IsMmap() const {
    return l2p_vec_.IsMmap() || p2l_vec_.IsMmap();
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
    if (use_i2o_map_) {
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
