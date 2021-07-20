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

#include <cmath>  // std::isnan
#include <common/Types.h>
#include "segcore/Reduce.h"

struct SearchResultPair {
    float distance_;
    milvus::SearchResult* search_result_;
    int64_t offset_;
    int64_t index_;

    SearchResultPair(float distance, milvus::SearchResult* search_result, int64_t offset, int64_t index)
        : distance_(distance), search_result_(search_result), offset_(offset), index_(index) {
    }

    bool
    operator<(const SearchResultPair& pair) const {
        return std::isnan(pair.distance_) || (!std::isnan(distance_) && (distance_ < pair.distance_));
    }

    bool
    operator>(const SearchResultPair& pair) const {
        return std::isnan(pair.distance_) || (!std::isnan(distance_) && (distance_ > pair.distance_));
    }

    void
    reset_distance() {
        distance_ = search_result_->result_distances_[offset_];
    }
};
