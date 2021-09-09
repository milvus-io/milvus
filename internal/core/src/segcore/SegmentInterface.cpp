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

#include "segcore/SegmentInterface.h"
#include "query/generated/ExecPlanNodeVisitor.h"
namespace milvus::segcore {
class Naive;

void
SegmentInternalInterface::FillTargetEntry(const query::Plan* plan, SearchResult& results) const {
    std::shared_lock lck(mutex_);
    AssertInfo(plan, "empty plan");
    auto size = results.result_distances_.size();
    Assert(results.internal_seg_offsets_.size() == size);
    // Assert(results.result_offsets_.size() == size);
    Assert(results.row_data_.size() == 0);

    // std::vector<int64_t> row_ids(size);
    std::vector<int64_t> element_sizeofs;
    std::vector<aligned_vector<char>> blobs;

    // fill row_ids
    {
        aligned_vector<char> blob(size * sizeof(int64_t));
        if (plan->schema_.get_is_auto_id()) {
            bulk_subscript(SystemFieldType::RowId, results.internal_seg_offsets_.data(), size, blob.data());
        } else {
            auto key_offset_opt = get_schema().get_primary_key_offset();
            Assert(key_offset_opt.has_value());
            auto key_offset = key_offset_opt.value();
            Assert(get_schema()[key_offset].get_data_type() == DataType::INT64);
            bulk_subscript(key_offset, results.internal_seg_offsets_.data(), size, blob.data());
        }
        blobs.emplace_back(std::move(blob));
        element_sizeofs.push_back(sizeof(int64_t));
    }

    // fill other entries
    for (auto field_offset : plan->target_entries_) {
        auto& field_meta = get_schema()[field_offset];
        auto element_sizeof = field_meta.get_sizeof();
        aligned_vector<char> blob(size * element_sizeof);
        bulk_subscript(field_offset, results.internal_seg_offsets_.data(), size, blob.data());
        blobs.emplace_back(std::move(blob));
        element_sizeofs.push_back(element_sizeof);
    }

    auto target_sizeof = std::accumulate(element_sizeofs.begin(), element_sizeofs.end(), 0);

    for (int64_t i = 0; i < size; ++i) {
        int64_t element_offset = 0;
        std::vector<char> target(target_sizeof);
        for (int loc = 0; loc < blobs.size(); ++loc) {
            auto element_sizeof = element_sizeofs[loc];
            auto blob_ptr = blobs[loc].data();
            auto src = blob_ptr + element_sizeof * i;
            auto dst = target.data() + element_offset;
            memcpy(dst, src, element_sizeof);
            element_offset += element_sizeof;
        }
        assert(element_offset == target_sizeof);
        results.row_data_.emplace_back(std::move(target));
    }
}

SearchResult
SegmentInternalInterface::Search(const query::Plan* plan,
                                 const query::PlaceholderGroup& placeholder_group,
                                 Timestamp timestamp) const {
    std::shared_lock lck(mutex_);
    check_search(plan);
    query::ExecPlanNodeVisitor visitor(*this, timestamp, placeholder_group);
    auto results = visitor.get_moved_result(*plan->plan_node_);
    results.segment_ = (void*)this;
    return results;
}

// Note: this is temporary solution.
// modify bulk script implement to make process more clear
static std::unique_ptr<ScalarArray>
CreateScalarArrayFrom(const void* data_raw, int64_t count, DataType data_type) {
    auto scalar_array = std::make_unique<ScalarArray>();
    switch (data_type) {
        case DataType::BOOL: {
            auto data = reinterpret_cast<const double*>(data_raw);
            auto obj = scalar_array->mutable_bool_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT8: {
            auto data = reinterpret_cast<const int8_t*>(data_raw);
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT16: {
            auto data = reinterpret_cast<const int16_t*>(data_raw);
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT32: {
            auto data = reinterpret_cast<const int32_t*>(data_raw);
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT64: {
            auto data = reinterpret_cast<const int64_t*>(data_raw);
            auto obj = scalar_array->mutable_long_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::FLOAT: {
            auto data = reinterpret_cast<const float*>(data_raw);
            auto obj = scalar_array->mutable_float_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::DOUBLE: {
            auto data = reinterpret_cast<const double*>(data_raw);
            auto obj = scalar_array->mutable_double_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        default: {
            PanicInfo("unsupported datatype");
        }
    }
    return scalar_array;
}

static std::unique_ptr<DataArray>
CreateDataArrayFrom(const void* data_raw, int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(milvus::proto::schema::DataType(field_meta.get_data_type()));

    if (!datatype_is_vector(data_type)) {
        auto scalar_array = CreateScalarArrayFrom(data_raw, count, data_type);
        data_array->set_allocated_scalars(scalar_array.release());
    } else {
        auto vector_array = data_array->mutable_vectors();
        auto dim = field_meta.get_dim();
        vector_array->set_dim(dim);
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                auto length = count * dim;
                auto data = reinterpret_cast<const float*>(data_raw);
                auto obj = vector_array->mutable_float_vector();
                obj->mutable_data()->Add(data, data + length);
                break;
            }
            case DataType::VECTOR_BINARY: {
                Assert(dim % 8 == 0);
                auto num_bytes = count * dim / 8;
                auto data = reinterpret_cast<const char*>(data_raw);
                auto obj = vector_array->mutable_binary_vector();
                obj->assign(data, num_bytes);
                break;
            }
            default: {
                PanicInfo("unsupported datatype");
            }
        }
    }
    return data_array;
}

std::unique_ptr<DataArray>
SegmentInternalInterface::BulkSubScript(FieldOffset field_offset, const SegOffset* seg_offsets, int64_t count) const {
    if (field_offset.get() >= 0) {
        auto& field_meta = get_schema()[field_offset];
        aligned_vector<char> data(field_meta.get_sizeof() * count);
        bulk_subscript(field_offset, (const int64_t*)seg_offsets, count, data.data());
        return CreateDataArrayFrom(data.data(), count, field_meta);
    } else {
        Assert(field_offset.get() == -1);
        aligned_vector<char> data(sizeof(int64_t) * count);
        bulk_subscript(SystemFieldType::RowId, (const int64_t*)seg_offsets, count, data.data());
        return CreateDataArrayFrom(data.data(), count, FieldMeta::RowIdMeta);
    }
}

std::unique_ptr<proto::segcore::RetrieveResults>
SegmentInternalInterface::Retrieve(const query::RetrievePlan* plan, Timestamp timestamp) const {
    std::shared_lock lck(mutex_);
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    query::ExecPlanNodeVisitor visitor(*this, timestamp);
    auto retrieve_results = visitor.get_retrieve_result(*plan->plan_node_);
    retrieve_results.segment_ = (void*)this;

    for (auto& seg_offset : retrieve_results.result_offsets_) {
        results->add_offset(seg_offset);
    }

    auto fields_data = results->mutable_fields_data();
    auto ids = results->mutable_ids();
    auto pk_offset = plan->schema_.get_primary_key_offset();
    for (auto field_offset : plan->field_offsets_) {
        auto col = BulkSubScript(field_offset, (SegOffset*)retrieve_results.result_offsets_.data(),
                                 retrieve_results.result_offsets_.size());
        auto col_data = col.release();
        fields_data->AddAllocated(col_data);
        if (pk_offset.has_value() && pk_offset.value() == field_offset) {
            auto int_ids = ids->mutable_int_id();
            for (int j = 0; j < col_data->scalars().long_data().data_size(); ++j) {
                int_ids->add_data(col_data->scalars().long_data().data(j));
            }
        }
    }
    return results;
}
}  // namespace milvus::segcore
