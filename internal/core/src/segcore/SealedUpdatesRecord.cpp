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

#include "segcore/SealedUpdatesRecord.h"

#include <algorithm>
#include <numeric>
#include <unordered_set>
#include <utility>

#include "common/EasyAssert.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

namespace milvus::segcore::experimental {
namespace {
constexpr int64_t kReadBatchSize = 4096;

void
CheckCancelled(OpContext* op_ctx) {
    if (op_ctx && op_ctx->cancellation_token.isCancellationRequested()) {
        ThrowInfo(FollyCancel, "POC sealed update preparation cancelled");
    }
}
}  // namespace

SealedUpdatesRecord::SealedUpdatesRecord(
    std::shared_ptr<ChunkedSegmentSealedImpl> source)
    : source_(std::move(source)) {
    AssertInfo(source_ != nullptr, "POC source segment is missing");
    read_state_ = source_->PinReadState();
}

SealedUpdatesRecord::~SealedUpdatesRecord() = default;

std::unique_ptr<SealedUpdatesRecord>
SealedUpdatesRecord::Build(std::shared_ptr<ChunkedSegmentSealedImpl> source,
                           const std::vector<int64_t>& field_ids,
                           bool enable_temp_index,
                           OpContext* op_ctx) {
    CheckCancelled(op_ctx);
    auto result = std::unique_ptr<SealedUpdatesRecord>(
        new SealedUpdatesRecord(std::move(source)));
    auto& segment =
        static_cast<SegmentInternalInterface&>(*result->read_state_);
    const auto row_count = segment.get_row_count();
    AssertInfo(row_count >= 0, "POC sealed row count is negative");
    auto schema = segment.get_schema_snapshot();
    std::unordered_set<int64_t> seen;
    for (auto field_id : field_ids) {
        AssertInfo(seen.insert(field_id).second, "Duplicate POC update field");
        AssertInfo(!segment.is_primary_field(FieldId(field_id)),
                   "POC updates cannot modify primary keys");
        const auto& field = (*schema)[FieldId(field_id)];
        AssertInfo(field.get_data_type() == DataType::INT64 ||
                       field.get_data_type() == DataType::VARCHAR,
                   "POC fields must be INT64 or VARCHAR");
    }
    AssertInfo(!field_ids.empty(), "POC update field list is empty");
    result->records_ =
        std::make_unique<UpdatesRecord>(row_count, enable_temp_index);
    result->insertion_timestamps_.resize(row_count);
    std::vector<int64_t> offsets;
    for (int64_t start = 0; start < row_count;) {
        CheckCancelled(op_ctx);
        auto count = std::min(kReadBatchSize, row_count - start);
        offsets.resize(count);
        std::iota(offsets.begin(), offsets.end(), start);
        // This accessor applies import/CDC commit-time semantics as well.
        segment.bulk_subscript(op_ctx,
                               SystemFieldType::Timestamp,
                               offsets.data(),
                               count,
                               result->insertion_timestamps_.data() + start);
        start += count;
    }
    for (auto field_id : field_ids) {
        const auto& field = (*schema)[FieldId(field_id)];
        const bool is_int = field.get_data_type() == DataType::INT64;
        std::vector<UpdateValue> values;
        values.reserve(row_count);
        for (int64_t start = 0; start < row_count;) {
            CheckCancelled(op_ctx);
            auto count = std::min(kReadBatchSize, row_count - start);
            offsets.resize(count);
            std::iota(offsets.begin(), offsets.end(), start);
            // Reuses scalar index reverse lookup when raw columns are absent.
            auto data = segment.bulk_subscript(
                op_ctx, FieldId(field_id), offsets.data(), count);
            AssertInfo(data != nullptr, "POC source returned no field data");
            const auto& scalars = data->scalars();
            AssertInfo((is_int ? scalars.long_data().data_size()
                               : scalars.string_data().data_size()) == count,
                       "POC source value count differs from requested offsets");
            AssertInfo(!field.is_nullable() || data->valid_data_size() == count,
                       "POC nullable source has incomplete validity");
            for (int64_t i = 0; i < count; ++i) {
                if (field.is_nullable() && !data->valid_data(i)) {
                    values.emplace_back(std::monostate{});
                } else if (is_int) {
                    values.emplace_back(scalars.long_data().data(i));
                } else {
                    values.emplace_back(scalars.string_data().data(i));
                }
            }
            start += count;
        }
        result->records_->InitializeField(
            field_id,
            is_int ? UpdateFieldType::Int64 : UpdateFieldType::VarChar,
            field.is_nullable(),
            values,
            result->insertion_timestamps_);
    }
    return result;
}

size_t
SealedUpdatesRecord::Apply(const std::vector<UpdateValue>& primary_keys,
                           uint64_t mutation_ts,
                           const std::vector<UpdateAssignment>& assignments,
                           OpContext* op_ctx) {
    CheckCancelled(op_ctx);
    auto schema = read_state_->get_schema_snapshot();
    auto pk_field = schema->get_primary_field_id();
    AssertInfo(pk_field.has_value(), "POC segment has no primary key field");
    auto pk_type = (*schema)[*pk_field].get_data_type();
    IdArray ids;
    for (const auto& pk : primary_keys) {
        if (pk_type == DataType::INT64) {
            AssertInfo(std::holds_alternative<int64_t>(pk),
                       "POC primary key must be INT64");
            ids.mutable_int_id()->add_data(std::get<int64_t>(pk));
        } else {
            AssertInfo(pk_type == DataType::VARCHAR &&
                           std::holds_alternative<std::string>(pk),
                       "POC primary key must be VARCHAR");
            ids.mutable_str_id()->add_data(std::get<std::string>(pk));
        }
    }
    return records_->Apply(
        primary_keys, mutation_ts, assignments, [&](const auto&, uint64_t ts) {
            std::vector<int64_t> offsets;
            if (primary_keys.empty()) {
                return offsets;
            }
            const auto rows = insertion_timestamps_.size();
            BitsetType matches(rows, false);
            auto& segment =
                static_cast<SegmentInternalInterface&>(*read_state_);
            segment.search_ids(matches, ids, op_ctx);
            BitsetType deleted(rows, false);
            BitsetTypeView deleted_view(deleted);
            segment.mask_with_delete(deleted_view, rows, ts);
            for (auto offset = matches.find_first(); offset;
                 offset = matches.find_next(*offset)) {
                if (insertion_timestamps_[*offset] < ts && !deleted[*offset]) {
                    offsets.push_back(*offset);
                }
            }
            CheckCancelled(op_ctx);
            return offsets;
        });
}

}  // namespace milvus::segcore::experimental
