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

#pragma once

#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "common/RegexIndexCostModel.h"
#include "common/Types.h"

namespace milvus::exec {

inline RegexIndexWorkStats
ComputeRegexBitmapWork(const TargetBitmap& input, size_t physical_batch_rows) {
    RegexIndexWorkStats stats{.remaining_rows = input.size()};
    if (physical_batch_rows == 0) {
        return stats;
    }
    for (size_t begin = 0; begin < input.size(); begin += physical_batch_rows) {
        const auto size = std::min(physical_batch_rows, input.size() - begin);
        const auto active = input.view(begin, size).count();
        stats.active_rows += active;
        if (active != 0) {
            stats.visited_rows += size;
            ++stats.visited_batches;
        }
    }
    return stats;
}

// Only a complete, already-computed nonnullable scalar row predicate may be
// borrowed here. The owner retains it; no bitmap is cloned or evaluated.
// Build suffix statistics lazily once for the query's physical batch geometry.
class RegexPrefilterStats {
 public:
    explicit RegexPrefilterStats(std::shared_ptr<const TargetBitmap> truth)
        : truth_(std::move(truth)) {
    }

    bool
    Matches(const std::shared_ptr<const TargetBitmap>& truth) const {
        return truth_.get() == truth.get();
    }

    std::optional<RegexIndexWorkStats>
    Get(size_t begin, size_t remaining_rows, size_t physical_batch_rows) {
        if (truth_ == nullptr || physical_batch_rows == 0 ||
            begin > truth_->size() ||
            remaining_rows != truth_->size() - begin ||
            begin % physical_batch_rows != 0) {
            return std::nullopt;
        }
        if (physical_batch_rows_ != physical_batch_rows) {
            const auto batches = truth_->size() / physical_batch_rows +
                                 (truth_->size() % physical_batch_rows != 0);
            // This is optional routing metadata, not an excuse for O(N)
            // suffix memory when a query chooses a tiny physical batch size.
            // Default 8192-row batches need only 124 entries for a million rows.
            if (batches >= kMaxSuffixEntries) {
                return std::nullopt;
            }
            physical_batch_rows_ = physical_batch_rows;
            suffix_.assign(batches + 1, {});
            for (size_t part = batches; part-- > 0;) {
                const auto offset = part * physical_batch_rows;
                const auto size =
                    std::min(physical_batch_rows, truth_->size() - offset);
                const auto active = truth_->view(offset, size).count();
                suffix_[part] = suffix_[part + 1];
                suffix_[part].remaining_rows += size;
                suffix_[part].active_rows += active;
                if (active != 0) {
                    suffix_[part].visited_rows += size;
                    ++suffix_[part].visited_batches;
                }
            }
        }
        return suffix_[begin / physical_batch_rows];
    }

 private:
    static constexpr size_t kMaxSuffixEntries = 8192;
    std::shared_ptr<const TargetBitmap> truth_;
    size_t physical_batch_rows_ = 0;
    std::vector<RegexIndexWorkStats> suffix_;
};

}  // namespace milvus::exec
