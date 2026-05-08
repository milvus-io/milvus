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

//
// Created by zilliz on 2024/3/26.
//

#include "ReduceUtils.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "fmt/core.h"
#include "pb/schema.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"

namespace milvus::segcore {

namespace {
// Safe variant accessor: asserts the variant holds the expected alternative
// before returning it. Converts latent std::bad_variant_access (terminate) into
// a reported AssertInfo failure with row/type context.
//
// Returns const reference to avoid copying non-trivial alternatives (std::string).
// For trivially copyable alternatives (ints, bool) the compiler will inline and
// fold the access away, so the overhead of the check is a single integer
// comparison per row.
template <typename T>
const T&
SafeGetVariant(const GroupByValueType& value, size_t row, DataType dtype) {
    AssertInfo(std::holds_alternative<T>(value.value()),
               "variant alternative mismatch at row {} for effective_type {}: "
               "variant.index()={}",
               row,
               static_cast<int>(dtype),
               value.value().index());
    return std::get<T>(value.value());
}

// Helper function to assemble a single field's values from composite keys.
// For JSON fields, effective_type should be the json_type (the actual stored variant type),
// not DataType::JSON itself.
void
AssembleSingleFieldFromCompositeKeys(
    milvus::proto::schema::FieldData* field_data,
    const std::vector<CompositeGroupKey>& composite_keys,
    size_t field_index,
    DataType effective_type) {
    auto data_type = effective_type;
    auto valid_data = std::make_unique<google::protobuf::RepeatedField<bool>>();
    valid_data->Resize(composite_keys.size(), true);
    auto scalar_field = std::make_unique<milvus::proto::schema::ScalarField>();
    size_t count = composite_keys.size();

    switch (data_type) {
        case DataType::INT8: {
            field_data->set_type(milvus::proto::schema::DataType::Int8);
            auto int_data = scalar_field->mutable_int_data();
            int_data->mutable_data()->Resize(count, 0);
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    int_data->mutable_data()->Set(
                        i, SafeGetVariant<int8_t>(value, i, data_type));
                } else {
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        case DataType::INT16: {
            field_data->set_type(milvus::proto::schema::DataType::Int16);
            auto int_data = scalar_field->mutable_int_data();
            int_data->mutable_data()->Resize(count, 0);
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    int_data->mutable_data()->Set(
                        i, SafeGetVariant<int16_t>(value, i, data_type));
                } else {
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        case DataType::INT32: {
            field_data->set_type(milvus::proto::schema::DataType::Int32);
            auto int_data = scalar_field->mutable_int_data();
            int_data->mutable_data()->Resize(count, 0);
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    int_data->mutable_data()->Set(
                        i, SafeGetVariant<int32_t>(value, i, data_type));
                } else {
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        case DataType::INT64: {
            field_data->set_type(milvus::proto::schema::DataType::Int64);
            auto long_data = scalar_field->mutable_long_data();
            long_data->mutable_data()->Resize(count, 0);
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    long_data->mutable_data()->Set(
                        i, SafeGetVariant<int64_t>(value, i, data_type));
                } else {
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        case DataType::TIMESTAMPTZ: {
            field_data->set_type(milvus::proto::schema::DataType::Timestamptz);
            auto timestamptz_data = scalar_field->mutable_timestamptz_data();
            timestamptz_data->mutable_data()->Resize(count, 0);
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    timestamptz_data->mutable_data()->Set(
                        i, SafeGetVariant<int64_t>(value, i, data_type));
                } else {
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        case DataType::BOOL: {
            field_data->set_type(milvus::proto::schema::DataType::Bool);
            auto bool_data = scalar_field->mutable_bool_data();
            bool_data->mutable_data()->Resize(count, false);
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    bool_data->mutable_data()->Set(
                        i, SafeGetVariant<bool>(value, i, data_type));
                } else {
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            field_data->set_type(milvus::proto::schema::DataType::VarChar);
            auto string_data = scalar_field->mutable_string_data();
            for (size_t i = 0; i < count; i++) {
                const auto& value = composite_keys[i][field_index];
                if (value.has_value()) {
                    *(string_data->mutable_data()->Add()) =
                        SafeGetVariant<std::string>(value, i, data_type);
                } else {
                    *(string_data->mutable_data()->Add()) = "";
                    valid_data->Set(i, false);
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported datatype {} for composite group_by operations",
                    data_type));
        }
    }

    field_data->mutable_valid_data()->Swap(valid_data.get());
    field_data->mutable_scalars()->Swap(scalar_field.get());
}
}  // namespace

void
AssembleCompositeGroupByValues(
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_result,
    const std::vector<CompositeGroupKey>& composite_group_by_vals,
    milvus::query::Plan* plan) {
    const auto& group_by_field_ids =
        plan->plan_node_->search_info_.group_by_field_ids_;

    if (group_by_field_ids.empty()) {
        return;
    }
    // Note: do NOT early-return on empty composite_group_by_vals.
    // Even with 0 hits we must still emit field metadata (field_id, name, type)
    // so downstream consumers can distinguish "group_by requested but empty"
    // from "no group_by at all" — matches pre-PR AssembleGroupByValues behavior.
    // The single-field backward-compat CopyFrom at the bottom also propagates
    // the type-only FieldData to field 8 for legacy Go consumers.

    // Invariant: every CompositeGroupKey must have exactly
    // group_by_field_ids.size() columns. A mismatch indicates an upstream
    // producer bug (version skew / partial construction) — fail loud here with
    // a contextual message instead of tripping UB via operator[] in the loop.
    for (size_t i = 0; i < composite_group_by_vals.size(); i++) {
        AssertInfo(
            composite_group_by_vals[i].Size() == group_by_field_ids.size(),
            "CompositeGroupKey column count mismatch at row {}: "
            "key.Size()={}, group_by_field_ids.size()={}",
            i,
            composite_group_by_vals[i].Size(),
            group_by_field_ids.size());
    }

    auto json_type = plan->plan_node_->search_info_.json_type_;

    // For each field in the composite key, create a FieldData
    for (size_t field_idx = 0; field_idx < group_by_field_ids.size();
         field_idx++) {
        auto field_id = group_by_field_ids[field_idx];
        auto& field = plan->schema_->operator[](field_id);
        auto data_type = field.get_data_type();

        // For JSON fields, use json_type as the effective serialization type
        auto effective_type = data_type;
        if (data_type == DataType::JSON) {
            effective_type =
                json_type.has_value() ? json_type.value() : DataType::VARCHAR;
        }

        auto* field_data =
            search_result->mutable_group_by_field_values()->Add();
        field_data->set_field_id(field_id.get());
        field_data->set_field_name(field.get_name().get());

        AssembleSingleFieldFromCompositeKeys(
            field_data, composite_group_by_vals, field_idx, effective_type);
    }

    // Backward compatibility: the existing Go layer reads GetGroupByFieldValue()
    // (field 8, singular) and is unaware of group_by_field_values (field 17).
    // Populate field 8 from the already-assembled first entry until the Go-layer
    // PR catches up.  Field 8 is singular so this only applies when size == 1.
    if (group_by_field_ids.size() == 1) {
        search_result->mutable_group_by_field_value()->CopyFrom(
            search_result->group_by_field_values(0));
    }
}

}  // namespace milvus::segcore