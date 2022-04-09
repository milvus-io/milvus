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
#include "query/generated/ExecPlanNodeVisitor.h"
#include "Utils.h"

namespace milvus::segcore {

void
SegmentInternalInterface::FillPrimaryKeys(const query::Plan* plan, SearchResult& results) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size, "Size of result distances is not equal to size of ids");
    Assert(results.primary_keys_.size() == 0);
    results.primary_keys_.resize(size);

    auto pk_field_id_opt = get_schema().get_primary_field_id();
    AssertInfo(pk_field_id_opt.has_value(), "Cannot get primary key offset from schema");
    auto pk_field_id = pk_field_id_opt.value();
    AssertInfo(IsPrimaryKeyDataType(get_schema()[pk_field_id].get_data_type()),
               "Primary key field is not INT64 or VARCHAR type");
    auto field_data = bulk_subscript(pk_field_id, results.seg_offsets_.data(), size);
    results.pk_type_ = DataType(field_data->type());

    std::vector<PkType> pks(size);
    ParsePksFromFieldData(pks, *field_data.get());
    results.primary_keys_ = std::move(pks);
}

void
SegmentInternalInterface::FillTargetEntry(const query::Plan* plan, SearchResult& results) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size, "Size of result distances is not equal to size of ids");

    // fill other entries except primary key by result_offset
    for (auto field_id : plan->target_entries_) {
        auto field_data = bulk_subscript(field_id, results.seg_offsets_.data(), size);
        results.output_fields_data_[field_id] = std::move(field_data);
    }
}

std::unique_ptr<SearchResult>
SegmentInternalInterface::Search(const query::Plan* plan,
                                 const query::PlaceholderGroup& placeholder_group,
                                 Timestamp timestamp) const {
    std::shared_lock lck(mutex_);
    check_search(plan);
    query::ExecPlanNodeVisitor visitor(*this, timestamp, placeholder_group);
    auto results = std::make_unique<SearchResult>();
    *results = visitor.get_moved_result(*plan->plan_node_);
    results->segment_ = (void*)this;
    return results;
}

std::unique_ptr<proto::segcore::RetrieveResults>
SegmentInternalInterface::Retrieve(const query::RetrievePlan* plan, Timestamp timestamp) const {
    std::shared_lock lck(mutex_);
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    query::ExecPlanNodeVisitor visitor(*this, timestamp);
    auto retrieve_results = visitor.get_retrieve_result(*plan->plan_node_);
    retrieve_results.segment_ = (void*)this;

    results->mutable_offset()->Add(retrieve_results.result_offsets_.begin(), retrieve_results.result_offsets_.end());

    auto fields_data = results->mutable_fields_data();
    auto ids = results->mutable_ids();
    auto pk_field_id = plan->schema_.get_primary_field_id();
    for (auto field_id : plan->field_ids_) {
        auto& field_mata = plan->schema_[field_id];

        auto col =
            bulk_subscript(field_id, retrieve_results.result_offsets_.data(), retrieve_results.result_offsets_.size());
        auto col_data = col.release();
        fields_data->AddAllocated(col_data);
        if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
            switch (field_mata.get_data_type()) {
                case DataType::INT64: {
                    auto int_ids = ids->mutable_int_id();
                    auto src_data = col_data->scalars().long_data();
                    int_ids->mutable_data()->Add(src_data.data().begin(), src_data.data().end());
                    break;
                }
                case DataType::VARCHAR: {
                    auto str_ids = ids->mutable_str_id();
                    auto src_data = col_data->scalars().string_data();
                    for (auto i = 0; i < src_data.data_size(); ++i)
                        *(str_ids->mutable_data()->Add()) = src_data.data(i);
                    break;
                }
                default: {
                    PanicInfo("unsupported data type");
                }
            }
        }
    }
    return results;
}
}  // namespace milvus::segcore
