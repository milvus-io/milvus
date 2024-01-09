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

#include "QueryInterface.h"

namespace milvus {
namespace query {

std::unique_ptr<milvus::base::SearchResult>
Search(const milvus::segment::SegmentInternalInterface* segment,
       const query::Plan* plan,
       const query::PlaceholderGroup* placeholder_group,
       Timestamp timestamp) {
    std::shared_lock lck(segment->mutex_);
    milvus::tracer::AddEvent("obtained_segment_lock_mutex");
    segment->check_search(plan);
    query::ExecPlanNodeVisitor visitor(*segment, timestamp, placeholder_group);
    auto results = std::make_unique<milvus::base::SearchResult>();
    *results = visitor.get_moved_result(*plan->plan_node_);
    results->segment_ = (void*)segment;
    return results;
}

std::unique_ptr<proto::segcore::RetrieveResults>
Retrieve(const milvus::segment::SegmentInternalInterface* segment,
         const query::RetrievePlan* plan,
         Timestamp timestamp,
         int64_t limit_size) {
    std::shared_lock lck(segment->mutex_);
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    query::ExecPlanNodeVisitor visitor(*segment, timestamp);
    auto retrieve_results = visitor.get_retrieve_result(*plan->plan_node_);
    retrieve_results.segment_ = (void*)segment;

    auto result_rows = retrieve_results.result_offsets_.size();
    int64_t output_data_size = 0;
    for (auto field_id : plan->field_ids_) {
        output_data_size += segment->get_field_avg_size(field_id) * result_rows;
    }
    if (output_data_size > limit_size) {
        throw SegcoreError(
            RetrieveError,
            fmt::format("query results exceed the limit size ", limit_size));
    }

    if (plan->plan_node_->is_count_) {
        AssertInfo(retrieve_results.field_data_.size() == 1,
                   "count result should only have one column");
        *results->add_fields_data() = retrieve_results.field_data_[0];
        return results;
    }

    results->mutable_offset()->Add(retrieve_results.result_offsets_.begin(),
                                   retrieve_results.result_offsets_.end());

    auto fields_data = results->mutable_fields_data();
    auto ids = results->mutable_ids();
    auto pk_field_id = plan->schema_.get_primary_field_id();
    for (auto field_id : plan->field_ids_) {
        if (milvus::base::SystemProperty::Instance().IsSystem(field_id)) {
            auto system_type =
                milvus::base::SystemProperty::Instance().GetSystemFieldType(
                    field_id);

            auto size = retrieve_results.result_offsets_.size();
            FixedVector<int64_t> output(size);
            segment->bulk_subscript(system_type,
                                    retrieve_results.result_offsets_.data(),
                                    size,
                                    output.data());

            auto data_array = std::make_unique<DataArray>();
            data_array->set_field_id(field_id.get());
            data_array->set_type(milvus::proto::schema::DataType::Int64);

            auto scalar_array = data_array->mutable_scalars();
            auto data = reinterpret_cast<const int64_t*>(output.data());
            auto obj = scalar_array->mutable_long_data();
            obj->mutable_data()->Add(data, data + size);
            fields_data->AddAllocated(data_array.release());
            continue;
        }

        auto& field_meta = plan->schema_[field_id];

        auto col =
            segment->bulk_subscript(field_id,
                                    retrieve_results.result_offsets_.data(),
                                    retrieve_results.result_offsets_.size());
        if (field_meta.get_data_type() == DataType::ARRAY) {
            col->mutable_scalars()->mutable_array_data()->set_element_type(
                proto::schema::DataType(field_meta.get_element_type()));
        }
        auto col_data = col.release();
        fields_data->AddAllocated(col_data);
        if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
            switch (field_meta.get_data_type()) {
                case DataType::INT64: {
                    auto int_ids = ids->mutable_int_id();
                    auto& src_data = col_data->scalars().long_data();
                    int_ids->mutable_data()->Add(src_data.data().begin(),
                                                 src_data.data().end());
                    break;
                }
                case DataType::VARCHAR: {
                    auto str_ids = ids->mutable_str_id();
                    auto& src_data = col_data->scalars().string_data();
                    for (auto i = 0; i < src_data.data_size(); ++i) {
                        *(str_ids->mutable_data()->Add()) = src_data.data(i);
                    }
                    break;
                }
                default: {
                    PanicInfo(DataTypeInvalid,
                              fmt::format("unsupported datatype {}",
                                          field_meta.get_data_type()));
                }
            }
        }
    }
    return results;
}

int64_t
GetRealCount(const milvus::segment::SegmentInternalInterface* segment) {
#if 0
    auto insert_cnt = get_row_count();
    BitsetType bitset_holder;
    bitset_holder.resize(insert_cnt, false);
    mask_with_delete(bitset_holder, insert_cnt, MAX_TIMESTAMP);
    return bitset_holder.size() - bitset_holder.count();
#endif
    auto plan = std::make_unique<query::RetrievePlan>(segment->get_schema());
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->is_count_ = true;
    auto res = Retrieve(segment, plan.get(), MAX_TIMESTAMP, INT64_MAX);
    AssertInfo(res->fields_data().size() == 1,
               "count result should only have one column");
    AssertInfo(res->fields_data()[0].has_scalars(),
               "count result should match scalar");
    AssertInfo(res->fields_data()[0].scalars().has_long_data(),
               "count result should match long data");
    AssertInfo(res->fields_data()[0].scalars().long_data().data_size() == 1,
               "count result should only have one row");
    return res->fields_data()[0].scalars().long_data().data(0);
}

}  // namespace query
}  // namespace milvus