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

#include "index/growing/RTreeGrowingSpatialIndex.h"

#include <iterator>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/scalar/spatial/RTreeIndexReader.h"

namespace milvus::index {
namespace {

int64_t
CheckedBatchEnd(int64_t row_begin, size_t row_count) {
    if (row_begin < 0 ||
        row_count > static_cast<size_t>(
                        std::numeric_limits<int64_t>::max() - row_begin)) {
        ThrowInfo(UnexpectedError,
                  "growing R-Tree row range [{}, {} rows) overflows int64",
                  row_begin,
                  row_count);
    }
    return row_begin + static_cast<int64_t>(row_count);
}

size_t
CheckedValueCount(int64_t count) {
    AssertInfo(count >= 0, "R-Tree shard has a negative value count");
    const auto value = static_cast<uint64_t>(count);
    if (value > std::numeric_limits<size_t>::max()) {
        ThrowInfo(UnexpectedError,
                  "R-Tree shard value count exceeds size_t domain");
    }
    return static_cast<size_t>(value);
}

void
ValidateAcceptedBatch(int64_t row_begin,
                      int64_t row_end,
                      const std::vector<rtree_detail::Value>& values,
                      const std::vector<size_t>& null_offsets) {
    int64_t previous_value = -1;
    for (const auto& value : values) {
        AssertInfo(value.second >= row_begin && value.second < row_end &&
                       value.second > previous_value,
                   "growing R-Tree produced invalid row offset {} for batch "
                   "[{}, {})",
                   value.second,
                   row_begin,
                   row_end);
        previous_value = value.second;
    }

    size_t previous_null = 0;
    bool first_null = true;
    size_t value_index = 0;
    for (const auto offset : null_offsets) {
        AssertInfo(offset >= static_cast<size_t>(row_begin) &&
                       offset < static_cast<size_t>(row_end) &&
                       (first_null || offset > previous_null),
                   "growing R-Tree produced invalid null offset {} for batch "
                   "[{}, {})",
                   offset,
                   row_begin,
                   row_end);
        while (value_index < values.size() &&
               values[value_index].second < static_cast<int64_t>(offset)) {
            ++value_index;
        }
        AssertInfo(value_index == values.size() ||
                       values[value_index].second !=
                           static_cast<int64_t>(offset),
                   "growing R-Tree row {} is both indexed and null",
                   offset);
        previous_null = offset;
        first_null = false;
    }
}

}  // namespace

RTreeGrowingSpatialIndex::RTreeGrowingSpatialIndex(
    int64_t publish_window_rows)
    : publish_window_rows_(publish_window_rows) {
    if (publish_window_rows_ <= 0) {
        ThrowInfo(ConfigInvalid,
                  "growing R-Tree publish window must be positive, got {}",
                  publish_window_rows_);
    }
}

void
RTreeGrowingSpatialIndex::CommitIfNeeded() {
    Flush();
}

void
RTreeGrowingSpatialIndex::Flush() {
    std::lock_guard lock(writer_mutex_);
    if (accepted_end_ > published_end_) {
        PublishPending();
    }
}

void
RTreeGrowingSpatialIndex::PublishPending() {
    AssertInfo(accepted_end_ > published_end_,
               "growing R-Tree has no pending rows to publish");

    auto frozen_null_offsets =
        std::make_shared<const std::vector<size_t>>(null_offsets_);
    auto next_levels = levels_;
    auto next_window_count = value_window_count_;

    if (!pending_values_.empty()) {
        if (next_window_count == std::numeric_limits<uint64_t>::max()) {
            ThrowInfo(UnexpectedError,
                      "growing R-Tree publication count overflows uint64");
        }
        ++next_window_count;

        size_t target_level = 0;
        size_t merged_count = pending_values_.size();
        while (target_level < next_levels.size() &&
               next_levels[target_level] != nullptr) {
            const auto existing_count =
                CheckedValueCount(next_levels[target_level]->Count());
            if (existing_count >
                std::numeric_limits<size_t>::max() - merged_count) {
                ThrowInfo(UnexpectedError,
                          "growing R-Tree compaction count overflows size_t");
            }
            merged_count += existing_count;
            ++target_level;
        }

        std::vector<rtree_detail::Value> merged;
        merged.reserve(merged_count);
        merged.insert(
            merged.end(), pending_values_.begin(), pending_values_.end());
        for (size_t level = 0; level < target_level; ++level) {
            next_levels[level]->AppendValues(merged);
            next_levels[level].reset();
        }
        auto candidate = RTreeQueryEngine::Create(std::move(merged));
        if (target_level == next_levels.size()) {
            next_levels.resize(target_level + 1);
        }
        next_levels[target_level] = std::move(candidate);
    }

    std::vector<std::shared_ptr<const RTreeQueryEngine>> engines;
    engines.reserve(next_levels.size());
    for (const auto& engine : next_levels) {
        if (engine != nullptr) {
            engines.push_back(engine);
        }
    }
    auto state = RTreeIndexState::CreateFromValidatedShards(
        std::move(engines), std::move(frozen_null_offsets), accepted_end_);
    auto reader = std::make_unique<RTreeIndexReader>(std::move(state));
    PublishSnapshot(std::move(reader), accepted_end_);

    // Everything above is built from local/shared immutable state. Only after
    // publication succeeds may the writer discard its retryable window.
    levels_ = std::move(next_levels);
    value_window_count_ = next_window_count;
    pending_values_.clear();
    published_end_ = accepted_end_;
}

void
RTreeGrowingSpatialIndex::Append(int64_t row_begin,
                                 const ScalarBatch<std::string_view>& batch) {
    const auto batch_end = CheckedBatchEnd(row_begin, batch.row_count);
    std::lock_guard lock(writer_mutex_);

    // Segment storage is immutable. Retrying a wholly accepted range only
    // retries its pending publication and never inserts duplicate row IDs.
    if (batch_end <= accepted_end_) {
        if (accepted_end_ > published_end_) {
            PublishPending();
        }
        return;
    }
    if (row_begin != accepted_end_) {
        ThrowInfo(UnexpectedError,
                  "growing R-Tree expected row {}, got range [{}, {})",
                  accepted_end_,
                  row_begin,
                  batch_end);
    }
    AssertInfo(batch.row_count == 0 || batch.values != nullptr,
               "growing R-Tree batch has null values for {} rows",
               batch.row_count);

    RTreeBuildEngine batch_engine("");
    std::vector<size_t> batch_null_offsets;
    if (batch.valid != nullptr) {
        batch_null_offsets.reserve(batch.row_count);
    }
    for (size_t i = 0; i < batch.row_count; ++i) {
        const auto offset = row_begin + static_cast<int64_t>(i);
        if (batch.valid != nullptr && !batch.valid[i]) {
            batch_null_offsets.push_back(static_cast<size_t>(offset));
            continue;
        }
        const auto wkb = batch.values[i];
        batch_engine.AddGeometry(
            reinterpret_cast<const uint8_t*>(wkb.data()), wkb.size(), offset);
    }
    auto batch_values = std::move(batch_engine).TakeValues();
    ValidateAcceptedBatch(
        row_begin, batch_end, batch_values, batch_null_offsets);

    if (batch_values.size() >
            std::numeric_limits<size_t>::max() - pending_values_.size() ||
        batch_null_offsets.size() >
            std::numeric_limits<size_t>::max() - null_offsets_.size()) {
        ThrowInfo(UnexpectedError,
                  "growing R-Tree pending state exceeds size_t domain");
    }
    pending_values_.reserve(pending_values_.size() + batch_values.size());
    null_offsets_.reserve(null_offsets_.size() + batch_null_offsets.size());
    pending_values_.insert(pending_values_.end(),
                           std::make_move_iterator(batch_values.begin()),
                           std::make_move_iterator(batch_values.end()));
    null_offsets_.insert(null_offsets_.end(),
                         batch_null_offsets.begin(),
                         batch_null_offsets.end());
    accepted_end_ = batch_end;

    if (accepted_end_ - published_end_ >= publish_window_rows_) {
        PublishPending();
    }
}

DataType
RTreeGrowingSpatialIndex::ValueType() const {
    return DataType::GEOMETRY;
}

std::string
RTreeGrowingSpatialIndex::Family() const {
    return "rtree";
}

}  // namespace milvus::index
