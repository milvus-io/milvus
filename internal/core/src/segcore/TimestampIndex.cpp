// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "TimestampIndex.h"

using namespace std;
using namespace std::chrono;

namespace milvus::segcore {

const int logicalBits = 18;
const uint64_t logicalBitsMask = (1ULL << logicalBits) - 1;

std::pair<system_clock::time_point, uint64_t>
ParseTS(uint64_t ts) {
    uint64_t logical = ts & logicalBitsMask;
    uint64_t physical = ts >> logicalBits;
    auto physicalTime = system_clock::from_time_t(physical / 1000);
    auto ms = milliseconds(physical % 1000);
    physicalTime += ms;

    return make_pair(physicalTime, logical);
}

void
TimestampIndex::set_length_meta(std::vector<int64_t> lengths) {
    lengths_ = std::move(lengths);
}

void
TimestampIndex::build_with(const Timestamp* timestamps, int64_t size) {
    auto num_slice = lengths_.size();
    Assert(num_slice > 0);
    std::vector<int64_t> prefix_sums;
    int offset = 0;
    prefix_sums.push_back(offset);
    std::vector<Timestamp> timestamp_barriers;
    Timestamp last_max_v = 0;
    for (int slice_id = 0; slice_id < num_slice; ++slice_id) {
        auto length = lengths_[slice_id];
        auto [min_v, max_v] = std::minmax_element(timestamps + offset,
                                                  timestamps + offset + length);
        Assert(last_max_v <= *min_v);
        offset += length;
        prefix_sums.push_back(offset);
        timestamp_barriers.push_back(*min_v);
        last_max_v = *max_v;
    }
    timestamp_barriers.push_back(last_max_v);
    Assert(
        std::is_sorted(timestamp_barriers.begin(), timestamp_barriers.end()));
    Assert(offset == size);
    auto min_ts = timestamp_barriers[0];

    this->size_ = size;
    this->start_locs_ = std::move(prefix_sums);
    this->min_timestamp_ = min_ts;
    this->max_timestamp_ = last_max_v;
    this->timestamp_barriers_ = std::move(timestamp_barriers);
}

std::pair<int64_t, int64_t>
TimestampIndex::get_active_range(Timestamp query_timestamp) const {
    if (query_timestamp >= max_timestamp_) {
        // most common case
        return {size_, size_};
    }
    if (query_timestamp < min_timestamp_) {
        return {0, 0};
    }
    auto iter = std::upper_bound(timestamp_barriers_.begin(),
                                 timestamp_barriers_.end(),
                                 query_timestamp);
    int block_id = (iter - timestamp_barriers_.begin()) - 1;
    Assert(0 <= block_id && block_id < lengths_.size());
    return {start_locs_[block_id], start_locs_[block_id + 1]};
}

BitsetType
TimestampIndex::GenerateBitset(Timestamp query_timestamp,
                               std::pair<int64_t, int64_t> active_range,
                               const Timestamp* timestamps,
                               int64_t size) {
    auto [beg, end] = active_range;
    Assert(beg < end);
    BitsetType bitset;
    bitset.reserve(size);
    bitset.resize(beg, false);
    bitset.resize(size, true);
    for (int64_t i = beg; i < end; ++i) {
        bitset[i] = timestamps[i] > query_timestamp;
    }
    return bitset;
}

BitsetType
TimestampIndex::GenerateTTLBitset(Timestamp collection_ttl,
                                  const Timestamp* timestamps,
                                  int64_t size) {
    BitsetType bitset;
    bitset.reserve(size);
    bitset.resize(size, false);
    auto cur = system_clock::now();
    for (int64_t i = 0; i < size; ++i) {
        auto [physicalTime, logical] = ParseTS(timestamps[i]);
        bitset[i] =
            (duration_cast<milliseconds>(physicalTime.time_since_epoch())
                 .count() +
             collection_ttl) >
            duration_cast<milliseconds>(cur.time_since_epoch()).count();
    }
    return bitset;
}

std::vector<int64_t>
GenerateFakeSlices(const Timestamp* timestamps,
                   int64_t size,
                   int min_slice_length) {
    assert(min_slice_length >= 1);
    std::vector<int64_t> results;
    std::vector<int64_t> min_values(size);
    Timestamp value = std::numeric_limits<Timestamp>::max();
    for (int64_t i = 0; i < size; ++i) {
        auto offset = size - 1 - i;
        value = std::min(value, timestamps[offset]);
        min_values[offset] = value;
    }
    value = std::numeric_limits<Timestamp>::min();
    auto slice_length = 0;
    for (int64_t i = 0; i < size; ++i) {
        if (value <= min_values[i] && slice_length >= min_slice_length) {
            // emit new slice
            results.push_back(slice_length);
            slice_length = 0;
        }
        value = std::max(value, timestamps[i]);
        slice_length += 1;
    }
    results.push_back(slice_length);
    return results;
}

}  // namespace milvus::segcore
