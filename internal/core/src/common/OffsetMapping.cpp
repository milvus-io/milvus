#include "common/OffsetMapping.h"

namespace milvus {

void
OffsetMapping::Build(const bool* valid_data,
                     int64_t total_count,
                     int64_t start_logical,
                     int64_t start_physical) {
    if (total_count == 0 || valid_data == nullptr) {
        return;
    }

    std::unique_lock<std::shared_mutex> lck(mutex_);
    enabled_ = true;
    total_count_ = start_logical + total_count;

    // Count valid elements first
    int64_t valid_count = 0;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            valid_count++;
        }
    }

    // Auto-select storage mode: use map when valid ratio < 10%
    use_map_ = (valid_count * 10 < total_count);

    if (use_map_) {
        // Map mode: only store valid entries
        int64_t physical_idx = start_physical;
        for (int64_t i = 0; i < total_count; ++i) {
            if (valid_data[i]) {
                l2p_map_[start_logical + i] = physical_idx;
                p2l_map_[physical_idx] = start_logical + i;
                physical_idx++;
            }
        }
    } else {
        // Vec mode: store all entries
        int64_t required_size = start_logical + total_count;
        if (static_cast<int64_t>(l2p_vec_.size()) < required_size) {
            l2p_vec_.resize(required_size, -1);
        }

        int64_t physical_idx = start_physical;
        for (int64_t i = 0; i < total_count; ++i) {
            if (valid_data[i]) {
                l2p_vec_[start_logical + i] = physical_idx;
                if (physical_idx >= static_cast<int64_t>(p2l_vec_.size())) {
                    p2l_vec_.resize(physical_idx + 1, -1);
                }
                p2l_vec_[physical_idx] = start_logical + i;
                physical_idx++;
            } else {
                l2p_vec_[start_logical + i] = -1;
            }
        }
    }

    valid_count_ += valid_count;
}

void
OffsetMapping::BuildIncremental(const bool* valid_data,
                                int64_t count,
                                int64_t start_logical,
                                int64_t start_physical) {
    if (count == 0 || valid_data == nullptr) {
        return;
    }

    std::unique_lock<std::shared_mutex> lck(mutex_);
    enabled_ = true;
    total_count_ = start_logical + count;

    // Incremental builds always use vec mode
    if (use_map_ && !l2p_map_.empty()) {
        // Convert from map to vec if needed
        int64_t max_logical = 0;
        for (const auto& [logical, physical] : l2p_map_) {
            if (logical > max_logical) {
                max_logical = logical;
            }
        }
        l2p_vec_.resize(max_logical + 1, -1);
        for (const auto& [logical, physical] : l2p_map_) {
            l2p_vec_[logical] = physical;
        }
        int64_t max_physical = 0;
        for (const auto& [physical, logical] : p2l_map_) {
            if (physical > max_physical) {
                max_physical = physical;
            }
        }
        p2l_vec_.resize(max_physical + 1, -1);
        for (const auto& [physical, logical] : p2l_map_) {
            p2l_vec_[physical] = logical;
        }
        l2p_map_.clear();
        p2l_map_.clear();
        use_map_ = false;
    }

    // Resize l2p_vec if needed
    int64_t required_size = start_logical + count;
    if (static_cast<int64_t>(l2p_vec_.size()) < required_size) {
        l2p_vec_.resize(required_size, -1);
    }

    int64_t physical_idx = start_physical;
    for (int64_t i = 0; i < count; ++i) {
        if (valid_data[i]) {
            l2p_vec_[start_logical + i] = physical_idx;
            if (physical_idx >= static_cast<int64_t>(p2l_vec_.size())) {
                p2l_vec_.resize(physical_idx + 1, -1);
            }
            p2l_vec_[physical_idx] = start_logical + i;
            physical_idx++;
            valid_count_++;
        } else {
            l2p_vec_[start_logical + i] = -1;
        }
    }
}

int64_t
OffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    std::shared_lock<std::shared_mutex> lck(mutex_);
    if (!enabled_) {
        return logical_offset;
    }
    if (use_map_) {
        auto it = l2p_map_.find(static_cast<int32_t>(logical_offset));
        if (it != l2p_map_.end()) {
            return it->second;
        }
        return -1;
    }
    if (logical_offset < static_cast<int64_t>(l2p_vec_.size())) {
        return l2p_vec_[logical_offset];
    }
    return -1;
}

int64_t
OffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    std::shared_lock<std::shared_mutex> lck(mutex_);
    if (!enabled_) {
        return physical_offset;
    }
    if (use_map_) {
        auto it = p2l_map_.find(static_cast<int32_t>(physical_offset));
        if (it != p2l_map_.end()) {
            return it->second;
        }
        return -1;
    }
    if (physical_offset < static_cast<int64_t>(p2l_vec_.size())) {
        return p2l_vec_[physical_offset];
    }
    return -1;
}

bool
OffsetMapping::IsValid(int64_t logical_offset) const {
    return GetPhysicalOffset(logical_offset) >= 0;
}

int64_t
OffsetMapping::GetValidCount() const {
    std::shared_lock<std::shared_mutex> lck(mutex_);
    return valid_count_;
}

bool
OffsetMapping::IsEnabled() const {
    std::shared_lock<std::shared_mutex> lck(mutex_);
    return enabled_;
}

int64_t
OffsetMapping::GetNextPhysicalOffset() const {
    std::shared_lock<std::shared_mutex> lck(mutex_);
    return valid_count_;
}

int64_t
OffsetMapping::GetTotalCount() const {
    std::shared_lock<std::shared_mutex> lck(mutex_);
    return total_count_;
}

}  // namespace milvus
