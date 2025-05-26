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

#pragma once

#include <boost/dynamic_bitset.hpp>
#include <vector>
#include <utility>

#include "common/Schema.h"

namespace milvus::segcore {

class TimestampIndex {
 public:
    void
    set_length_meta(std::vector<int64_t> lengths);

    void
    build_with(const Timestamp* timestamps, int64_t size);
    // output bitset

    // Return range [beg, end) that is undecided
    // [0, beg) shall be all OK
    // [end, size) shall be all not OK
    std::pair<int64_t, int64_t>
    get_active_range(Timestamp query_timestamp) const;

    static BitsetType
    GenerateBitset(Timestamp query_timestamp,
                   std::pair<int64_t, int64_t> active_range,
                   const Timestamp* timestamps,
                   int64_t size);

    static BitsetType
    GenerateTTLBitset(Timestamp collection_ttl,
                      const Timestamp* timestamps,
                      int64_t size);

 private:
    // numSlice
    std::vector<int64_t> lengths_;
    int64_t size_;
    // numSlice + 1
    std::vector<int64_t> start_locs_;
    Timestamp min_timestamp_;
    Timestamp max_timestamp_;
    // numSlice + 1
    std::vector<Timestamp> timestamp_barriers_;
};

std::vector<int64_t>
GenerateFakeSlices(const Timestamp* timestamps,
                   int64_t size,
                   int min_slice_length = 1);

}  // namespace milvus::segcore
