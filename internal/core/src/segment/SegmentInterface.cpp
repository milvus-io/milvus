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

#include "SegmentInterface.h"

#include <cstdint>

#include "base/Utils.h"
#include "common/EasyAssert.h"
#include "base/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"

namespace milvus::segment {

int64_t
SegmentInternalInterface::get_field_avg_size(FieldId field_id) const {
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    if (milvus::base::SystemProperty::Instance().IsSystem(field_id)) {
        if (field_id == TimestampFieldID || field_id == RowFieldID) {
            return sizeof(int64_t);
        }

        throw SegcoreError(FieldIDInvalid, "unsupported system field id");
    }

    auto schema = get_schema();
    auto& field_meta = schema[field_id];
    auto data_type = field_meta.get_data_type();

    std::shared_lock lck(mutex_);
    if (datatype_is_variable(data_type)) {
        if (variable_fields_avg_size_.find(field_id) ==
            variable_fields_avg_size_.end()) {
            return 0;
        }

        return variable_fields_avg_size_.at(field_id).second;
    } else {
        return field_meta.get_sizeof();
    }
}

void
SegmentInternalInterface::set_field_avg_size(FieldId field_id,
                                             int64_t num_rows,
                                             int64_t field_size) {
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    auto schema = get_schema();
    auto& field_meta = schema[field_id];
    auto data_type = field_meta.get_data_type();

    std::unique_lock lck(mutex_);
    if (datatype_is_variable(data_type)) {
        AssertInfo(num_rows > 0,
                   "The num rows of field data should be greater than 0");
        if (variable_fields_avg_size_.find(field_id) ==
            variable_fields_avg_size_.end()) {
            variable_fields_avg_size_.emplace(field_id, std::make_pair(0, 0));
        }

        auto& field_info = variable_fields_avg_size_.at(field_id);
        auto size = field_info.first * field_info.second + field_size;
        field_info.first = field_info.first + num_rows;
        field_info.second = size / field_info.first;
    }
}

void
SegmentInternalInterface::timestamp_filter(BitsetType& bitset,
                                           Timestamp timestamp) const {
    auto& timestamps = get_timestamps();
    auto cnt = bitset.size();
    if (timestamps[cnt - 1] <= timestamp) {
        // no need to filter out anything.
        return;
    }

    auto pilot = upper_bound(timestamps, 0, cnt, timestamp);
    // offset bigger than pilot should be filtered out.
    for (int offset = pilot; offset < cnt; offset = bitset.find_next(offset)) {
        if (offset == BitsetType::npos) {
            return;
        }
        bitset[offset] = false;
    }
}

void
SegmentInternalInterface::timestamp_filter(BitsetType& bitset,
                                           const std::vector<int64_t>& offsets,
                                           Timestamp timestamp) const {
    auto& timestamps = get_timestamps();
    auto cnt = bitset.size();
    if (timestamps[cnt - 1] <= timestamp) {
        // no need to filter out anything.
        return;
    }

    // point query, faster than binary search.
    for (auto& offset : offsets) {
        if (timestamps[offset] > timestamp) {
            bitset.set(offset, true);
        }
    }
}

const SkipIndex&
SegmentInternalInterface::GetSkipIndex() const {
    return skip_index_;
}

void
SegmentInternalInterface::LoadPrimitiveSkipIndex(milvus::FieldId field_id,
                                                 int64_t chunk_id,
                                                 milvus::DataType data_type,
                                                 const void* chunk_data,
                                                 int64_t count) {
    skip_index_.LoadPrimitive(field_id, chunk_id, data_type, chunk_data, count);
}

void
SegmentInternalInterface::LoadStringSkipIndex(
    milvus::FieldId field_id,
    int64_t chunk_id,
    const milvus::VariableColumn<std::string>& var_column) {
    skip_index_.LoadString(field_id, chunk_id, var_column);
}

void
SegmentInternalInterface::FillPrimaryKeys(
    const query::Plan* plan, milvus::base::SearchResult& results) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size,
               "Size of result distances is not equal to size of ids");
    Assert(results.primary_keys_.size() == 0);
    results.primary_keys_.resize(size);

    auto pk_field_id_opt = get_schema().get_primary_field_id();
    AssertInfo(pk_field_id_opt.has_value(),
               "Cannot get primary key offset from schema");
    auto pk_field_id = pk_field_id_opt.value();
    AssertInfo(IsPrimaryKeyDataType(get_schema()[pk_field_id].get_data_type()),
               "Primary key field is not INT64 or VARCHAR type");

    auto field_data =
        bulk_subscript(pk_field_id, results.seg_offsets_.data(), size);
    results.pk_type_ = DataType(field_data->type());

    milvus::base::ParsePksFromFieldData(results.primary_keys_,
                                        *field_data.get());
}

void
SegmentInternalInterface::FillTargetEntry(
    const query::Plan* plan, milvus::base::SearchResult& results) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size,
               "Size of result distances is not equal to size of ids");

    // fill other entries except primary key by result_offset
    for (auto field_id : plan->target_entries_) {
        auto field_data =
            bulk_subscript(field_id, results.seg_offsets_.data(), size);
        results.output_fields_data_[field_id] = std::move(field_data);
    }
}

void
SegmentInternalInterface::check_metric_type(
    const query::Plan* plan,
    const milvus::base::IndexMetaPtr index_meta) const {
    auto& metric_str = plan->plan_node_->search_info_.metric_type_;
    auto searched_field_id = plan->plan_node_->search_info_.field_id_;
    auto field_index_meta =
        index_meta->GetFieldIndexMeta(FieldId(searched_field_id));
    if (metric_str.empty()) {
        metric_str = field_index_meta.GeMetricType();
    }
    if (metric_str != field_index_meta.GeMetricType()) {
        throw SegcoreError(
            MetricTypeNotMatch,
            fmt::format("metric type not match, expected {}, actual {}.",
                        field_index_meta.GeMetricType(),
                        metric_str));
    }
}

}  // namespace milvus::segment
