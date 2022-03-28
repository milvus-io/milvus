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

#include <limits>

#include "common/Consts.h"
#include "common/Types.h"
#include "segcore/Reduce.h"

using milvus::SearchResult;

struct SearchResultPair {
    int64_t primary_key_;
    float distance_;
    milvus::SearchResult* search_result_;
    int64_t index_;
    int64_t offset_;
    int64_t offset_rb_;  // right bound

    SearchResultPair(int64_t primary_key, float distance, SearchResult* result, int64_t index, int64_t lb, int64_t rb)
        : primary_key_(primary_key),
          distance_(distance),
          search_result_(result),
          index_(index),
          offset_(lb),
          offset_rb_(rb) {
    }

    bool
    operator>(const SearchResultPair& other) const {
        if (this->primary_key_ == INVALID_ID) {
            return false;
        } else {
            if (other.primary_key_ == INVALID_ID) {
                return true;
            } else {
                return (distance_ > other.distance_);
            }
        }
    }

    void
    reset() {
        if (offset_ < offset_rb_) {
            offset_++;
            if (offset_ < offset_rb_) {
                primary_key_ = search_result_->primary_keys_.at(offset_);
                distance_ = search_result_->distances_.at(offset_);
            } else {
                primary_key_ = INVALID_ID;
                distance_ = std::numeric_limits<float>::max();
            }
        } else {
            primary_key_ = INVALID_ID;
            distance_ = std::numeric_limits<float>::max();
        }
    }
};
