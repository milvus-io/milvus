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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/growing/IGrowingIndex.h"
#include "index/growing/GrowingCommitPolicy.h"
#include "tantivy-wrapper.h"

// A serialized writer publishes independent manual-reload reader generations.
// Segment feeds only complete contiguous row ranges; query consumers still own
// uncovered-tail handling and bitmap bounds.
namespace milvus::index {

class TantivyGrowingTextIndex final : public IGrowingIndex,
                                      public IAppendable<TextBatch> {
 public:
    TantivyGrowingTextIndex(const char* unique_id,
                            const char* analyzer_name,
                            const char* analyzer_params,
                            DataType value_type,
                            int64_t commit_interval_in_ms);

    ~TantivyGrowingTextIndex() override = default;

    void
    CommitIfNeeded() override;

    void
    Flush() override;

    void
    Append(int64_t row_begin, const TextBatch& batch) override;

    DataType
    ValueType() const override;

    std::string
    Family() const override;

 private:
    struct OwnedBatch {
        int64_t row_begin{0};
        std::vector<std::string> values;
        std::vector<uint8_t> valid;
    };

    static OwnedBatch
    OwnBatch(int64_t row_begin, const TextBatch& batch);

    void
    AddOwnedBatch(const OwnedBatch& batch);

    void
    RecoverUncommitted();

    void
    PublishCommitted();

    void
    CommitAndPublish();

    std::mutex writer_mutex_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> writer_;
    GrowingCommitPolicy commit_policy_;
    DataType value_type_{DataType::NONE};

    std::vector<OwnedBatch> uncommitted_;
    // Cumulative accepted row offsets. Published readers receive an immutable
    // copy of the prefix belonging to their committed generation.
    std::vector<size_t> null_offsets_;
    int64_t accepted_end_{0};
    int64_t committed_end_{0};
    int64_t published_end_{0};
    bool needs_recovery_{false};
};

}  // namespace milvus::index
