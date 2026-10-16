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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/growing/IGrowingIndex.h"
#include "index/scalar/spatial/RTreeEngine.h"

// Complete append windows become immutable query shards. Binary tier carry
// bounds active query fanout without copying the full tree on every append.
namespace milvus::index {

class RTreeGrowingSpatialIndex final
    : public IGrowingIndex,
      public IAppendable<ScalarBatch<std::string_view>> {
 public:
    explicit RTreeGrowingSpatialIndex(int64_t publish_window_rows);

    ~RTreeGrowingSpatialIndex() override = default;

    void
    CommitIfNeeded() override;

    void
    Flush() override;

    void
    Append(int64_t row_begin,
           const ScalarBatch<std::string_view>& batch) override;

    DataType
    ValueType() const override;

    std::string
    Family() const override;

 private:
    void
    PublishPending();

    const int64_t publish_window_rows_;

    std::mutex writer_mutex_;
    std::vector<rtree_detail::Value> pending_values_;
    std::vector<size_t> null_offsets_;

    // Each value-bearing publication has tier weight one. Binary carry keeps
    // at most one immutable shard per level, so active fanout is bounded by
    // floor(log2(value_window_count_)) + 1 regardless of partial row windows.
    std::vector<std::shared_ptr<const RTreeQueryEngine>> levels_;
    uint64_t value_window_count_{0};
    int64_t accepted_end_{0};
    int64_t published_end_{0};
};

}  // namespace milvus::index
