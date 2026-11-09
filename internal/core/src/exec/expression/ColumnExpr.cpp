// Licensed to the LF AI & Data foundation under on0
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

#include "ColumnExpr.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <vector>

#include "bitset/bitset.h"
#include "boost/variant/get.hpp"
#include "common/Tracer.h"
#include "common/Types.h"
#include "opentelemetry/trace/span.h"

namespace milvus {
namespace exec {

template <typename T>
bool
PhyColumnExpr::GatherFromValueReader(const int64_t* offsets,
                                     int64_t count,
                                     T* values,
                                     TargetBitmapView valid) {
    using ReaderValue =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;
    if (!value_lookup_.Covers(offsets, count)) {
        return false;
    }
    const auto* selected = value_lookup_.Reader<ReaderValue>();
    AssertInfo(selected != nullptr,
               "cached value reader does not match column type {}",
               expr_->GetColumn().data_type_);

    std::vector<uint8_t> seen(static_cast<size_t>(count), 0);
    selected->Gather(
        offsets,
        count,
        [&](int64_t i, const ReaderValue* value, bool is_valid) {
            AssertInfo(i >= 0 && i < count,
                       "value gather output {} exceeds batch size {}",
                       i,
                       count);
            const auto pos = static_cast<size_t>(i);
            AssertInfo(
                seen[pos] == 0, "value gather produced output {} twice", i);
            seen[pos] = 1;
            valid[pos] = is_valid;
            if (!is_valid) {
                return;
            }
            AssertInfo(value != nullptr,
                       "value reader returned a null value for row {}",
                       offsets[pos]);
            if constexpr (std::is_same_v<T, std::string>) {
                values[pos].assign(value->data(), value->size());
            } else {
                values[pos] = *value;
            }
        });
    for (int64_t i = 0; i < count; ++i) {
        AssertInfo(seen[static_cast<size_t>(i)] != 0,
                   "value gather omitted row {}",
                   offsets[i]);
    }
    return true;
}

int64_t
PhyColumnExpr::GetNextBatchSize() {
    auto current_rows = GetCurrentRows();

    return current_rows + batch_size_ >= active_count_
               ? active_count_ - current_rows
               : batch_size_;
}

void
PhyColumnExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span("PhyColumnExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("data_type", static_cast<int>(expr_->type()));

    auto input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);
    switch (this->expr_->type()) {
        case DataType::BOOL:
            result = DoEval<bool>(input);
            break;
        case DataType::INT8:
            result = DoEval<int8_t>(input);
            break;
        case DataType::INT16:
            result = DoEval<int16_t>(input);
            break;
        case DataType::INT32:
            result = DoEval<int32_t>(input);
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            result = DoEval<int64_t>(input);
            break;
        case DataType::FLOAT:
            result = DoEval<float>(input);
            break;
        case DataType::DOUBLE:
            result = DoEval<double>(input);
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT: {
            result = DoEval<std::string>(input);
            break;
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported data type: {}",
                      this->expr_->type());
    }
}

template <typename T>
VectorPtr
PhyColumnExpr::DoEval(OffsetVector* input) {
    AssertInfo(!expr_->GetColumn().element_level_,
               "ColumnExpr of row-level access is not supported");

    if (HasValueReader()) {
        return DoEvalFromValueReader<T>(input);
    }

    if (segment_->type() == SegmentType::Sealed) {
        const auto count =
            input ? static_cast<int64_t>(input->size()) : GetNextBatchSize();
        if (count == 0) {
            return nullptr;
        }
        if (!data_column_) {
            const auto field = expr_->GetColumn().field_id_;
            data_column_ = snapshot_
                               ? snapshot_->GetDataScanResources(field).first
                               : segment_->GetDataScanResources(field).first;
            AssertInfo(
                data_column_ != nullptr, "field {} has no column", field.get());
        }
        using Value = std::
            conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;
        auto result = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, count);
        auto* values = result->RawAsValues<T>();
        TargetBitmapView valid(result->GetValidRawData(), count);
        valid.set();
        if (input) {
            auto take = data_column_->Take(
                op_ctx_,
                TakeOptions{OffsetView::From(input->data(), count),
                            TargetTypeOf<Value>()});
            AssertInfo(take != nullptr && take->Size() == count,
                       "invalid column Take result for {} rows",
                       count);
            const auto items = take->template Access<Value>();
            for (int64_t i = 0; i < count; ++i) {
                const auto item = items[i];
                valid[i] = item.is_valid;
                if (item.is_valid) {
                    values[i] = *item.value;
                }
            }
            return result;
        }
        if (!scan_cursor_) {
            const auto policy = segcore::SegcoreConfig::default_config()
                                        .get_scan_cursor_owns_pin()
                                    ? ScanPinPolicy::CursorOwned
                                    : ScanPinPolicy::ResultOwned;
            scan_cursor_ = data_column_->Scan(
                op_ctx_,
                ScanOptions::ForData(
                    current_row_, TargetTypeOf<Value>(), policy));
            AssertInfo(scan_cursor_ != nullptr, "column does not support Scan");
        }
        if (scan_cursor_->Position() != current_row_) {
            scan_cursor_->Seek(current_row_);
        }
        ScanBatch batch;
        int64_t processed = 0;
        while (processed < count) {
            const auto remaining = count - processed;
            const auto returned = scan_cursor_->Next(
                remaining, ScanReadMode::DataAndValidity, &batch);
            AssertInfo(returned && batch.size > 0 && batch.size <= remaining &&
                           batch.row_id_start == current_row_ + processed &&
                           !batch.values.empty(),
                       "invalid column Scan batch at row {}",
                       current_row_ + processed);
            const auto* data = batch.values.template data_as<Value>();
            for (int64_t i = 0; i < batch.size; ++i) {
                const auto is_valid = !batch.validity || batch.validity[i];
                valid[processed + i] = is_valid;
                if (is_valid) {
                    values[processed + i] = data[i];
                }
            }
            processed += batch.size;
        }
        current_row_ += count;
        return result;
    }

    // similar to PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op)
    // take offsets as input
    if (has_offset_input_) {
        auto real_batch_size = input->size();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, real_batch_size);
        T* res_value = res_vec->RawAsValues<T>();
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();

        int64_t processed_rows = 0;
        // Keep the chunk's data accessor (which pins the chunk) across
        // iterations and rebuild only when the chunk id changes, avoiding a
        // per-row accessor construction on Growing's stable per-chunk buffers.
        int64_t cached_chunk_id = -1;
        segcore::ChunkDataAccessor cda;
        for (auto i = 0; i < real_batch_size; ++i) {
            auto offset = (*input)[i];
            const auto chunk_size = segment_chunk_reader_.SizePerChunk();
            const auto chunk_id = offset / chunk_size;
            const auto chunk_offset = offset % chunk_size;
            if (chunk_id != cached_chunk_id) {
                cda = segment_chunk_reader_.GetChunkDataAccessor(
                    expr_->GetColumn().data_type_,
                    expr_->GetColumn().field_id_,
                    chunk_id);
                cached_chunk_id = chunk_id;
            }
            auto chunk_data_by_offset = cda(chunk_offset);
            if (!chunk_data_by_offset.has_value()) {
                valid_res[processed_rows] = false;
            } else {
                res_value[processed_rows] =
                    segcore::get_from_variant<T>(chunk_data_by_offset);
            }
            processed_rows++;
        }
        return res_vec;
    }

    // normal path
    if (segment_->is_chunked()) {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, real_batch_size);
        T* res_value = res_vec->RawAsValues<T>();
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();
        auto cda = segment_chunk_reader_.GetMultipleChunkDataAccessor(
            expr_->GetColumn().data_type_,
            expr_->GetColumn().field_id_,
            current_chunk_id_,
            current_chunk_pos_);
        for (int i = 0; i < real_batch_size; ++i) {
            auto data = cda();
            if (!data.has_value()) {
                valid_res[i] = false;
                continue;
            }
            res_value[i] = segcore::get_from_variant<T>(data);
        }
        return res_vec;
    } else {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, real_batch_size);
        T* res_value = res_vec->RawAsValues<T>();
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();

        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size =
                chunk_id == num_chunk_ - 1
                    ? active_count_ -
                          chunk_id * segment_chunk_reader_.SizePerChunk()
                    : segment_chunk_reader_.SizePerChunk();
            auto cda = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->GetColumn().data_type_,
                expr_->GetColumn().field_id_,
                chunk_id);

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                auto val = cda(i);
                if (!val.has_value()) {
                    valid_res[processed_rows] = false;
                } else {
                    res_value[processed_rows] =
                        segcore::get_from_variant<T>(val);
                }
                processed_rows++;

                if (processed_rows >= batch_size_) {
                    current_chunk_id_ = chunk_id;
                    current_chunk_pos_ = i + 1;
                    return res_vec;
                }
            }
        }
        return res_vec;
    }
}

template <typename T>
VectorPtr
PhyColumnExpr::DoEvalFromValueReader(OffsetVector* input) {
    const auto real_batch_size = input != nullptr
                                     ? static_cast<int64_t>(input->size())
                                     : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    std::vector<int64_t> offsets(static_cast<size_t>(real_batch_size));
    if (input != nullptr) {
        for (int64_t i = 0; i < real_batch_size; ++i) {
            offsets[static_cast<size_t>(i)] = (*input)[i];
        }
    } else {
        const auto row_begin = GetCurrentRows();
        for (int64_t i = 0; i < real_batch_size; ++i) {
            offsets[static_cast<size_t>(i)] = row_begin + i;
        }
    }

    auto result = std::make_shared<ColumnVector>(expr_->GetColumn().data_type_,
                                                 real_batch_size);
    auto* values = result->RawAsValues<T>();
    TargetBitmapView valid(result->GetValidRawData(), real_batch_size);
    valid.set();
    if (!GatherFromValueReader(
            offsets.data(), real_batch_size, values, valid)) {
        AssertInfo(segment_->type() == SegmentType::Growing,
                   "sealed value reader does not cover the requested offsets");
        int64_t cached_chunk_id = -1;
        segcore::ChunkDataAccessor accessor;
        for (int64_t i = 0; i < real_batch_size; ++i) {
            const auto offset = offsets[static_cast<size_t>(i)];
            const auto chunk_size = segment_chunk_reader_.SizePerChunk();
            const auto chunk_id = offset / chunk_size;
            const auto chunk_offset = offset % chunk_size;
            if (chunk_id != cached_chunk_id) {
                accessor = segment_chunk_reader_.GetChunkDataAccessor(
                    expr_->GetColumn().data_type_,
                    expr_->GetColumn().field_id_,
                    chunk_id);
                cached_chunk_id = chunk_id;
            }
            auto value = accessor(chunk_offset);
            if (!value.has_value()) {
                valid[static_cast<size_t>(i)] = false;
            } else {
                values[i] = segcore::get_from_variant<T>(value);
            }
        }
    }
    if (input == nullptr) {
        current_row_ += real_batch_size;
    }
    return result;
}

}  //namespace exec
}  // namespace milvus
