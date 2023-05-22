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

#include "Utils.h"
#include "common/SystemProperty.h"
#include "common/Types.h"
#include "query/generated/ExecPlanNodeVisitor.h"

namespace milvus::segcore {

void
SegmentInternalInterface::FillPrimaryKeys(const query::Plan* plan,
                                          SearchResult& results) const {
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

    ParsePksFromFieldData(results.primary_keys_, *field_data.get());
}

void
SegmentInternalInterface::FillTargetEntry(const query::Plan* plan,
                                          SearchResult& results) const {
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

std::unique_ptr<SearchResult>
SegmentInternalInterface::Search(
    const query::Plan* plan,
    const query::PlaceholderGroup* placeholder_group,
    Timestamp timestamp) const {
    std::shared_lock lck(mutex_);
    check_search(plan);
    query::ExecPlanNodeVisitor visitor(*this, timestamp, placeholder_group);
    auto results = std::make_unique<SearchResult>();
    *results = visitor.get_moved_result(*plan->plan_node_);
    results->segment_ = (void*)this;
    return results;
}

std::unique_ptr<RetrieveResult>
SegmentInternalInterface::Retrieve(const query::RetrievePlan* plan,
                                   Timestamp timestamp) const {
    std::shared_lock lck(mutex_);
    auto result = std::make_unique<RetrieveResult>();
    query::ExecPlanNodeVisitor visitor(*this, timestamp);
    *result = visitor.get_retrieve_result(*plan->plan_node_);
    result->segment_ = (void*)this;

    if (plan->plan_node_->is_count) {
        AssertInfo(result->field_data_.size() == 1,
                   "count result should only have one column");
        return result;
    }

    auto pk_field_id = plan->schema_.get_primary_field_id();
    for (auto field_id : plan->field_ids_) {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto system_type =
                SystemProperty::Instance().GetSystemFieldType(field_id);

            auto size = result->result_offsets_.size();
            FixedVector<int64_t> output(size);
            bulk_subscript(system_type,
                           result->result_offsets_.data(),
                           size,
                           output.data());

            auto data_array = std::make_unique<DataArray>();
            data_array->set_field_id(field_id.get());
            data_array->set_type(milvus::proto::schema::DataType::Int64);

            auto scalar_array = data_array->mutable_scalars();
            auto data = reinterpret_cast<const int64_t*>(output.data());
            auto obj = scalar_array->mutable_long_data();
            obj->mutable_data()->Add(data, data + size);
            result->field_data_.push_back(std::move(data_array));
            continue;
        }

        auto col = bulk_subscript(field_id,
                                  result->result_offsets_.data(),
                                  result->result_offsets_.size());
        if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
            result->pk_type_ = plan->schema_[field_id].get_data_type();
            auto ids = std::make_unique<IdArray>();
            switch (plan->schema_[field_id].get_data_type()) {
                case DataType::INT64: {
                    auto int_ids = ids->mutable_int_id();
                    auto& src_data = col->scalars().long_data();
                    int_ids->mutable_data()->Add(src_data.data().begin(),
                                                 src_data.data().end());
                    break;
                }
                case DataType::VARCHAR: {
                    auto str_ids = ids->mutable_str_id();
                    auto& src_data = col->scalars().string_data();
                    for (auto i = 0; i < src_data.data_size(); ++i) {
                        *(str_ids->mutable_data()->Add()) = src_data.data(i);
                    }
                    break;
                }
                default: {
                    PanicInfo("unsupported data type");
                }
            }
            result->ids_ = std::move(ids);
        }
        result->field_data_.push_back(std::move(col));
    }
    return result;
}

int64_t
SegmentInternalInterface::get_real_count() const {
#if 0
    auto insert_cnt = get_row_count();
    BitsetType bitset_holder;
    bitset_holder.resize(insert_cnt, false);
    mask_with_delete(bitset_holder, insert_cnt, MAX_TIMESTAMP);
    return bitset_holder.size() - bitset_holder.count();
#endif
    auto plan = std::make_unique<query::RetrievePlan>(get_schema());
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->is_count = true;
    auto res = Retrieve(plan.get(), MAX_TIMESTAMP);
    AssertInfo(res->field_data_.size() == 1,
               "count result should only have one column");
    AssertInfo(res->field_data_[0]->has_scalars(),
               "count result should match scalar");
    AssertInfo(res->field_data_[0]->scalars().has_long_data(),
               "count result should match long data");
    AssertInfo(res->field_data_[0]->scalars().long_data().data_size() == 1,
               "count result should only have one row");
    return res->field_data_[0]->scalars().long_data().data(0);
}

}  // namespace milvus::segcore
