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

#include "index/growing/TantivyGrowingTextIndex.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/scalar/text/TextIndexArtifact.h"
#include "index/scalar/text/TextIndexReader.h"

namespace milvus::index {
namespace {

constexpr int64_t kMaxTantivyDocuments =
    static_cast<int64_t>(std::numeric_limits<uint32_t>::max());

const char*
RequireCString(const char* value, const char* label) {
    AssertInfo(value != nullptr, "growing Tantivy {} must not be null", label);
    return value;
}

int64_t
CheckedBatchEnd(int64_t row_begin, size_t row_count) {
    if (row_begin < 0 || row_begin > kMaxTantivyDocuments ||
        row_count > static_cast<size_t>(kMaxTantivyDocuments - row_begin)) {
        ThrowInfo(UnexpectedError,
                  "growing Tantivy text row range [{}, {} rows) is outside "
                  "the uint32 document domain",
                  row_begin,
                  row_count);
    }
    return row_begin + static_cast<int64_t>(row_count);
}

}  // namespace

TantivyGrowingTextIndex::TantivyGrowingTextIndex(const char* unique_id,
                                                 const char* analyzer_name,
                                                 const char* analyzer_params,
                                                 DataType value_type,
                                                 int64_t commit_interval_in_ms)
    : writer_(std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
          RequireCString(unique_id, "field name"),
          /*in_ram=*/true,
          "",
          TANTIVY_INDEX_LATEST_VERSION,
          RequireCString(analyzer_name, "analyzer name"),
          RequireCString(analyzer_params, "analyzer parameters"),
          /*analyzer_extra_info=*/"",
          milvus::tantivy::DEFAULT_NUM_THREADS,
          milvus::tantivy::DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES,
          /*enable_background_merge=*/true)),
      commit_policy_(commit_interval_in_ms), value_type_(value_type) {
    AssertInfo(IsStringDataType(value_type_),
               "growing Tantivy text requires a string value type, got {}",
               static_cast<int>(value_type_));
}

TantivyGrowingTextIndex::OwnedBatch
TantivyGrowingTextIndex::OwnBatch(int64_t row_begin,
                                  const TextBatch& batch) {
    static_cast<void>(CheckedBatchEnd(row_begin, batch.row_count));
    AssertInfo(batch.row_count == 0 || batch.values != nullptr,
               "growing Tantivy text batch has null values for {} rows",
               batch.row_count);

    OwnedBatch owned;
    owned.row_begin = row_begin;
    owned.values.reserve(batch.row_count);
    if (batch.valid != nullptr) {
        owned.valid.reserve(batch.row_count);
    }
    for (size_t i = 0; i < batch.row_count; ++i) {
        const bool valid = batch.valid == nullptr || batch.valid[i];
        const auto value = batch.values[i];
        if (!valid || value.empty()) {
            owned.values.emplace_back();
        } else {
            owned.values.emplace_back(value.data(), value.size());
        }
        if (batch.valid != nullptr) {
            owned.valid.push_back(valid ? 1 : 0);
        }
    }
    return owned;
}

void
TantivyGrowingTextIndex::AddOwnedBatch(const OwnedBatch& batch) {
    static const std::string empty;
    for (size_t i = 0; i < batch.values.size(); ++i) {
        const auto offset = batch.row_begin + static_cast<int64_t>(i);
        if (!batch.valid.empty() && batch.valid[i] == 0) {
            writer_->add_array_data(&empty, 0, offset);
        } else {
            writer_->add_data(&batch.values[i], 1, offset);
        }
    }
}

void
TantivyGrowingTextIndex::RecoverUncommitted() {
    needs_recovery_ = true;
    writer_->rollback();
    for (const auto& batch : uncommitted_) {
        AddOwnedBatch(batch);
    }
    needs_recovery_ = false;
}

void
TantivyGrowingTextIndex::PublishCommitted() {
    AssertInfo(committed_end_ > published_end_,
               "growing Tantivy text has no committed generation to publish");
    auto snapshot = writer_->create_snapshot_reader(SetBitsetGrowing);
    const auto count = static_cast<int64_t>(snapshot->count());
    AssertInfo(count == committed_end_,
               "growing Tantivy text snapshot count {} disagrees with "
               "committed row end {}",
               count,
               committed_end_);
    const auto null_end = std::lower_bound(null_offsets_.begin(),
                                           null_offsets_.end(),
                                           static_cast<size_t>(committed_end_));
    auto frozen_null_offsets =
        std::make_shared<const std::vector<size_t>>(null_offsets_.begin(),
                                                    null_end);
    const auto payload_bytes = TextIndexRamPayloadBytes(*snapshot);
    auto reader = std::make_unique<TextIndexReader>(nullptr,
                                                     std::move(snapshot),
                                                     std::move(frozen_null_offsets),
                                                     committed_end_,
                                                     value_type_,
                                                     /*file_backed=*/false,
                                                     payload_bytes);
    PublishSnapshot(std::move(reader), committed_end_);
    published_end_ = committed_end_;
    commit_policy_.NoteCommitted();
}

void
TantivyGrowingTextIndex::CommitAndPublish() {
    if (committed_end_ > published_end_) {
        PublishCommitted();
    }
    if (accepted_end_ == committed_end_) {
        return;
    }

    try {
        writer_->commit();
    } catch (...) {
        auto failure = std::current_exception();
        needs_recovery_ = true;
        RecoverUncommitted();
        std::rethrow_exception(failure);
    }

    committed_end_ = accepted_end_;
    uncommitted_.clear();
    PublishCommitted();
}

void
TantivyGrowingTextIndex::CommitIfNeeded() {
    std::lock_guard lock(writer_mutex_);
    if (needs_recovery_) {
        RecoverUncommitted();
    }
    if (committed_end_ > published_end_) {
        PublishCommitted();
    }
    if (accepted_end_ > committed_end_ &&
        (published_end_ == 0 || commit_policy_.ShouldCommit())) {
        CommitAndPublish();
    }
}

void
TantivyGrowingTextIndex::Flush() {
    std::lock_guard lock(writer_mutex_);
    if (needs_recovery_) {
        RecoverUncommitted();
    }
    CommitAndPublish();
}

void
TantivyGrowingTextIndex::Append(int64_t row_begin, const TextBatch& batch) {
    const auto batch_end = CheckedBatchEnd(row_begin, batch.row_count);
    std::lock_guard lock(writer_mutex_);

    if (needs_recovery_) {
        RecoverUncommitted();
    }
    if (committed_end_ > published_end_) {
        PublishCommitted();
    }
    // Segment storage is immutable. Retrying a range already accepted by the
    // writer only completes its pending commit/publication; it must not add the
    // same document IDs again.
    if (batch_end <= accepted_end_) {
        if (accepted_end_ > committed_end_ && commit_policy_.ShouldCommit()) {
            CommitAndPublish();
        }
        return;
    }
    if (row_begin != accepted_end_) {
        ThrowInfo(UnexpectedError,
                  "growing Tantivy text expected row {}, got range [{}, {})",
                  accepted_end_,
                  row_begin,
                  batch_end);
    }
    auto owned = OwnBatch(row_begin, batch);
    if (owned.values.empty()) {
        if (commit_policy_.ShouldCommit()) {
            CommitAndPublish();
        }
        return;
    }

    const auto new_null_count =
        static_cast<size_t>(std::count(owned.valid.begin(),
                                      owned.valid.end(),
                                      static_cast<uint8_t>(0)));
    uncommitted_.reserve(uncommitted_.size() + 1);
    if (new_null_count != 0) {
        const auto required = null_offsets_.size() + new_null_count;
        if (required > null_offsets_.capacity()) {
            const auto limit = static_cast<size_t>(kMaxTantivyDocuments);
            auto grown = std::max<size_t>(null_offsets_.capacity(), 1);
            grown = grown > limit / 2 ? limit : grown * 2;
            null_offsets_.reserve(std::max(required, grown));
        }
    }
    try {
        AddOwnedBatch(owned);
    } catch (...) {
        auto failure = std::current_exception();
        needs_recovery_ = true;
        RecoverUncommitted();
        std::rethrow_exception(failure);
    }

    for (size_t i = 0; i < owned.valid.size(); ++i) {
        if (owned.valid[i] == 0) {
            null_offsets_.push_back(static_cast<size_t>(row_begin) + i);
        }
    }
    uncommitted_.push_back(std::move(owned));
    accepted_end_ = batch_end;
    if (commit_policy_.ShouldCommit()) {
        CommitAndPublish();
    }
}

DataType
TantivyGrowingTextIndex::ValueType() const {
    return value_type_;
}

std::string
TantivyGrowingTextIndex::Family() const {
    return "text";
}

}  // namespace milvus::index
