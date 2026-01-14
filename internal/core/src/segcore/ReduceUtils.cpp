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
#include "google/protobuf/repeated_field.h"
#include "pb/schema.pb.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include <cmath>
#include <variant>

namespace milvus::segcore {

using milvus::exec::GetDataGetter;

int
CompareOrderByValue(const OrderByValueType& lhs, const OrderByValueType& rhs) {
    if (!lhs.has_value() && !rhs.has_value()) {
        return 0;
    }
    if (!lhs.has_value()) {
        return -1;  // null < non-null
    }
    if (!rhs.has_value()) {
        return 1;  // non-null > null
    }

    const auto& lv = lhs.value();
    const auto& rv = rhs.value();

    // Compare based on variant type
    if (std::holds_alternative<bool>(lv) && std::holds_alternative<bool>(rv)) {
        auto l = std::get<bool>(lv);
        auto r = std::get<bool>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int8_t>(lv) &&
        std::holds_alternative<int8_t>(rv)) {
        auto l = std::get<int8_t>(lv);
        auto r = std::get<int8_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int16_t>(lv) &&
        std::holds_alternative<int16_t>(rv)) {
        auto l = std::get<int16_t>(lv);
        auto r = std::get<int16_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int32_t>(lv) &&
        std::holds_alternative<int32_t>(rv)) {
        auto l = std::get<int32_t>(lv);
        auto r = std::get<int32_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int64_t>(lv) &&
        std::holds_alternative<int64_t>(rv)) {
        auto l = std::get<int64_t>(lv);
        auto r = std::get<int64_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<float>(lv) &&
        std::holds_alternative<float>(rv)) {
        auto l = std::get<float>(lv);
        auto r = std::get<float>(rv);
        if (std::isnan(l) && std::isnan(r))
            return 0;
        if (std::isnan(l))
            return -1;  // NaN < non-NaN
        if (std::isnan(r))
            return 1;  // non-NaN > NaN
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<double>(lv) &&
        std::holds_alternative<double>(rv)) {
        auto l = std::get<double>(lv);
        auto r = std::get<double>(rv);
        if (std::isnan(l) && std::isnan(r))
            return 0;
        if (std::isnan(l))
            return -1;  // NaN < non-NaN
        if (std::isnan(r))
            return 1;  // non-NaN > NaN
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<std::string>(lv) &&
        std::holds_alternative<std::string>(rv)) {
        const auto& l = std::get<std::string>(lv);
        const auto& r = std::get<std::string>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    // Type mismatch or unsupported
    return 0;
}

OrderByValueType
ReadOrderByFieldValue(OpContext* op_ctx,
                      const SegmentInternalInterface& segment,
                      const plan::OrderByField& field,
                      int64_t seg_offset) {
    auto data_type = segment.GetFieldDataType(field.field_id_);

    switch (data_type) {
        case DataType::BOOL: {
            auto getter = GetDataGetter<bool>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::INT8: {
            auto getter = GetDataGetter<int8_t>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::INT16: {
            auto getter = GetDataGetter<int16_t>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::INT32: {
            auto getter = GetDataGetter<int32_t>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::INT64: {
            auto getter = GetDataGetter<int64_t>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::FLOAT: {
            auto getter = GetDataGetter<float>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::DOUBLE: {
            auto getter = GetDataGetter<double>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            auto getter = GetDataGetter<std::string>(
                op_ctx, segment, field.field_id_, field.json_path_);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        case DataType::JSON: {
            if (!field.json_path_.has_value()) {
                return std::nullopt;
            }
            auto getter =
                GetDataGetter<std::string, milvus::Json>(op_ctx,
                                                         segment,
                                                         field.field_id_,
                                                         field.json_path_,
                                                         std::nullopt,
                                                         false);
            return MakeOrderByValue(getter->Get(seg_offset));
        }
        default:
            // Unsupported type, return null
            return std::nullopt;
    }
}

std::vector<OrderByValueType>
ReadAllOrderByFieldValues(
    OpContext* op_ctx,
    const SegmentInternalInterface& segment,
    const std::vector<plan::OrderByField>& order_by_fields,
    int64_t seg_offset) {
    std::vector<OrderByValueType> order_by_vals;
    order_by_vals.reserve(order_by_fields.size());

    for (const auto& field : order_by_fields) {
        order_by_vals.push_back(
            ReadOrderByFieldValue(op_ctx, segment, field, seg_offset));
    }

    return order_by_vals;
}

// OrderByFieldReader implementation

std::vector<OrderByValueType>
OrderByFieldReader::Read(const SegmentInternalInterface& segment,
                         int64_t seg_offset) {
    if (order_by_fields_.empty()) {
        return {};
    }

    const auto& getters = GetOrCreateGetters(segment);
    std::vector<OrderByValueType> result;
    result.reserve(getters.size());

    for (const auto& getter : getters) {
        result.push_back(getter(seg_offset));
    }

    return result;
}

const std::vector<OrderByGetterFunc>&
OrderByFieldReader::GetOrCreateGetters(
    const SegmentInternalInterface& segment) {
    const void* segment_key = static_cast<const void*>(&segment);

    auto it = segment_getters_cache_.find(segment_key);
    if (it != segment_getters_cache_.end()) {
        return it->second;
    }

    // Create and cache getters for this segment
    auto getters = CreateGetters(segment);
    auto [inserted_it, _] =
        segment_getters_cache_.emplace(segment_key, std::move(getters));
    return inserted_it->second;
}

std::vector<OrderByGetterFunc>
OrderByFieldReader::CreateGetters(const SegmentInternalInterface& segment) {
    std::vector<OrderByGetterFunc> getters;
    getters.reserve(order_by_fields_.size());

    for (const auto& field : order_by_fields_) {
        auto data_type = segment.GetFieldDataType(field.field_id_);

        switch (data_type) {
            case DataType::BOOL: {
                auto getter = GetDataGetter<bool>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::INT8: {
                auto getter = GetDataGetter<int8_t>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::INT16: {
                auto getter = GetDataGetter<int16_t>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::INT32: {
                auto getter = GetDataGetter<int32_t>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::INT64: {
                auto getter = GetDataGetter<int64_t>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::FLOAT: {
                auto getter = GetDataGetter<float>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::DOUBLE: {
                auto getter = GetDataGetter<double>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::VARCHAR:
            case DataType::STRING: {
                auto getter = GetDataGetter<std::string>(
                    &op_ctx_, segment, field.field_id_, field.json_path_);
                getters.push_back([getter](int64_t offset) -> OrderByValueType {
                    return MakeOrderByValue(getter->Get(offset));
                });
                break;
            }
            case DataType::JSON: {
                if (!field.json_path_.has_value()) {
                    getters.push_back([](int64_t) -> OrderByValueType {
                        return std::nullopt;
                    });
                } else {
                    auto getter = GetDataGetter<std::string, milvus::Json>(
                        &op_ctx_,
                        segment,
                        field.field_id_,
                        field.json_path_,
                        std::nullopt,
                        false);
                    getters.push_back(
                        [getter](int64_t offset) -> OrderByValueType {
                            return MakeOrderByValue(getter->Get(offset));
                        });
                }
                break;
            }
            default:
                // Unsupported type, return null getter
                getters.push_back(
                    [](int64_t) -> OrderByValueType { return std::nullopt; });
                break;
        }
    }

    return getters;
}

void
AssembleGroupByValues(
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_result,
    const std::vector<GroupByValueType>& group_by_vals,
    milvus::query::Plan* plan) {
    auto group_by_field_id = plan->plan_node_->search_info_.group_by_field_id_;
    if (group_by_field_id.has_value()) {
        auto valid_data =
            std::make_unique<google::protobuf::RepeatedField<bool>>();
        valid_data->Resize(group_by_vals.size(), true);
        auto group_by_res_values =
            std::make_unique<milvus::proto::schema::ScalarField>();
        auto group_by_field =
            plan->schema_->operator[](group_by_field_id.value());
        auto group_by_data_type = group_by_field.get_data_type();
        auto mutable_group_by_field_value =
            search_result->mutable_group_by_field_value();
        int group_by_val_size = group_by_vals.size();
        switch (group_by_data_type) {
            case DataType::INT8: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Int8);
                auto field_data = group_by_res_values->mutable_int_data();
                field_data->mutable_data()->Resize(group_by_val_size, 0);
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        int8_t val =
                            std::get<int8_t>(group_by_vals[idx].value());
                        field_data->mutable_data()->Set(idx, val);
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::INT16: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Int16);
                auto field_data = group_by_res_values->mutable_int_data();
                field_data->mutable_data()->Resize(group_by_val_size, 0);
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        int16_t val =
                            std::get<int16_t>(group_by_vals[idx].value());
                        field_data->mutable_data()->Set(idx, val);
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::INT32: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Int32);
                auto field_data = group_by_res_values->mutable_int_data();
                field_data->mutable_data()->Resize(group_by_val_size, 0);
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        int32_t val =
                            std::get<int32_t>(group_by_vals[idx].value());
                        field_data->mutable_data()->Set(idx, val);
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::INT64: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Int64);
                auto field_data = group_by_res_values->mutable_long_data();
                field_data->mutable_data()->Resize(group_by_val_size, 0);
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        int64_t val =
                            std::get<int64_t>(group_by_vals[idx].value());
                        field_data->mutable_data()->Set(idx, val);
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::TIMESTAMPTZ: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Timestamptz);
                auto field_data =
                    group_by_res_values->mutable_timestamptz_data();
                field_data->mutable_data()->Resize(group_by_val_size, 0);
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        int64_t val =
                            std::get<int64_t>(group_by_vals[idx].value());
                        field_data->mutable_data()->Set(idx, val);
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::BOOL: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Bool);
                auto field_data = group_by_res_values->mutable_bool_data();
                field_data->mutable_data()->Resize(group_by_val_size, 0);
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        bool val = std::get<bool>(group_by_vals[idx].value());
                        field_data->mutable_data()->Set(idx, val);
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::VARCHAR: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::VarChar);
                auto field_data = group_by_res_values->mutable_string_data();
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        std::string val =
                            std::get<std::string>(group_by_vals[idx].value());
                        *(field_data->mutable_data()->Add()) = val;
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::GEOMETRY: {
                mutable_group_by_field_value->set_type(
                    milvus::proto::schema::DataType::Geometry);
                auto field_data = group_by_res_values->mutable_geometry_data();
                for (std::size_t idx = 0; idx < group_by_val_size; idx++) {
                    if (group_by_vals[idx].has_value()) {
                        std::string val =
                            std::get<std::string>(group_by_vals[idx].value());
                        *(field_data->mutable_data()->Add()) = val;
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
                break;
            }
            case DataType::JSON: {
                auto json_path = plan->plan_node_->search_info_.json_path_;
                auto json_type = plan->plan_node_->search_info_.json_type_;
                if (!json_type.has_value()) {
                    json_type = DataType::VARCHAR;
                }
                switch (json_type.value()) {
                    case DataType::BOOL: {
                        mutable_group_by_field_value->set_type(
                            milvus::proto::schema::DataType::Bool);
                        auto field_data =
                            group_by_res_values->mutable_bool_data();
                        field_data->mutable_data()->Resize(group_by_val_size,
                                                           0);
                        for (std::size_t idx = 0; idx < group_by_val_size;
                             idx++) {
                            if (group_by_vals[idx].has_value()) {
                                bool val =
                                    std::get<bool>(group_by_vals[idx].value());
                                field_data->mutable_data()->Set(idx, val);
                            } else {
                                valid_data->Set(idx, false);
                            }
                        }
                        break;
                    }
                    case DataType::INT8: {
                        mutable_group_by_field_value->set_type(
                            milvus::proto::schema::DataType::Int8);
                        auto field_data =
                            group_by_res_values->mutable_int_data();
                        field_data->mutable_data()->Resize(group_by_val_size,
                                                           0);
                        for (std::size_t idx = 0; idx < group_by_val_size;
                             idx++) {
                            if (group_by_vals[idx].has_value()) {
                                int8_t val = std::get<int8_t>(
                                    group_by_vals[idx].value());
                                field_data->mutable_data()->Set(idx, val);
                            } else {
                                valid_data->Set(idx, false);
                            }
                        }
                        break;
                    }
                    case DataType::INT16: {
                        mutable_group_by_field_value->set_type(
                            milvus::proto::schema::DataType::Int16);
                        auto field_data =
                            group_by_res_values->mutable_int_data();
                        field_data->mutable_data()->Resize(group_by_val_size,
                                                           0);
                        for (std::size_t idx = 0; idx < group_by_val_size;
                             idx++) {
                            if (group_by_vals[idx].has_value()) {
                                int16_t val = std::get<int16_t>(
                                    group_by_vals[idx].value());
                                field_data->mutable_data()->Set(idx, val);
                            } else {
                                valid_data->Set(idx, false);
                            }
                        }
                        break;
                    }
                    case DataType::INT32: {
                        mutable_group_by_field_value->set_type(
                            milvus::proto::schema::DataType::Int32);
                        auto field_data =
                            group_by_res_values->mutable_int_data();
                        field_data->mutable_data()->Resize(group_by_val_size,
                                                           0);
                        for (std::size_t idx = 0; idx < group_by_val_size;
                             idx++) {
                            if (group_by_vals[idx].has_value()) {
                                int32_t val = std::get<int32_t>(
                                    group_by_vals[idx].value());
                                field_data->mutable_data()->Set(idx, val);
                            } else {
                                valid_data->Set(idx, false);
                            }
                        }
                        break;
                    }
                    case DataType::INT64: {
                        mutable_group_by_field_value->set_type(
                            milvus::proto::schema::DataType::Int64);
                        auto field_data =
                            group_by_res_values->mutable_long_data();
                        field_data->mutable_data()->Resize(group_by_val_size,
                                                           0);
                        for (std::size_t idx = 0; idx < group_by_val_size;
                             idx++) {
                            if (group_by_vals[idx].has_value()) {
                                int64_t val = std::get<int64_t>(
                                    group_by_vals[idx].value());
                                field_data->mutable_data()->Set(idx, val);
                            } else {
                                valid_data->Set(idx, false);
                            }
                        }
                        break;
                    }
                    case DataType::VARCHAR: {
                        mutable_group_by_field_value->set_type(
                            milvus::proto::schema::DataType::VarChar);
                        auto field_data =
                            group_by_res_values->mutable_string_data();
                        for (std::size_t idx = 0; idx < group_by_val_size;
                             idx++) {
                            if (group_by_vals[idx].has_value()) {
                                std::string val = std::get<std::string>(
                                    group_by_vals[idx].value());
                                *(field_data->mutable_data()->Add()) = val;
                            } else {
                                valid_data->Set(idx, false);
                            }
                        }
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format("unsupported json cast type for "
                                              "json field group_by operations ",
                                              json_type.value()));
                    }
                }
                break;
            }
            default: {
                ThrowInfo(
                    DataTypeInvalid,
                    fmt::format("unsupported datatype for group_by operations ",
                                group_by_data_type));
            }
        }

        mutable_group_by_field_value->mutable_valid_data()->MergeFrom(
            *valid_data);
        mutable_group_by_field_value->mutable_scalars()->MergeFrom(
            *group_by_res_values.get());
    }
}

// Helper function to populate one FieldData for a single order_by field
static void
AssembleOneOrderByField(milvus::proto::schema::FieldData* field_data,
                        const std::vector<OrderByValueType>& field_vals,
                        DataType data_type) {
    auto valid_data = std::make_unique<google::protobuf::RepeatedField<bool>>();
    valid_data->Resize(field_vals.size(), true);
    auto scalar_field = std::make_unique<milvus::proto::schema::ScalarField>();
    int val_size = field_vals.size();

    switch (data_type) {
        case DataType::INT8: {
            field_data->set_type(milvus::proto::schema::DataType::Int8);
            auto data = scalar_field->mutable_int_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<int8_t>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::INT16: {
            field_data->set_type(milvus::proto::schema::DataType::Int16);
            auto data = scalar_field->mutable_int_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<int16_t>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::INT32: {
            field_data->set_type(milvus::proto::schema::DataType::Int32);
            auto data = scalar_field->mutable_int_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<int32_t>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::INT64: {
            field_data->set_type(milvus::proto::schema::DataType::Int64);
            auto data = scalar_field->mutable_long_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<int64_t>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::TIMESTAMPTZ: {
            field_data->set_type(milvus::proto::schema::DataType::Timestamptz);
            auto data = scalar_field->mutable_timestamptz_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<int64_t>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::BOOL: {
            field_data->set_type(milvus::proto::schema::DataType::Bool);
            auto data = scalar_field->mutable_bool_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<bool>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::VARCHAR:
        case DataType::JSON: {
            // For JSON, treat as string (json_path values are serialized as strings)
            field_data->set_type(milvus::proto::schema::DataType::VarChar);
            auto data = scalar_field->mutable_string_data();
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    *(data->mutable_data()->Add()) =
                        std::get<std::string>(field_vals[idx].value());
                } else {
                    *(data->mutable_data()->Add()) = "";
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::FLOAT: {
            field_data->set_type(milvus::proto::schema::DataType::Float);
            auto data = scalar_field->mutable_float_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<float>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::DOUBLE: {
            field_data->set_type(milvus::proto::schema::DataType::Double);
            auto data = scalar_field->mutable_double_data();
            data->mutable_data()->Resize(val_size, 0);
            for (std::size_t idx = 0; idx < val_size; idx++) {
                if (field_vals[idx].has_value()) {
                    data->mutable_data()->Set(
                        idx, std::get<double>(field_vals[idx].value()));
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported datatype for order_by operations: {}",
                            static_cast<int>(data_type)));
        }
    }

    field_data->mutable_valid_data()->MergeFrom(*valid_data);
    field_data->mutable_scalars()->MergeFrom(*scalar_field);
}

void
AssembleOrderByValues(
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_result,
    const std::vector<std::vector<OrderByValueType>>& order_by_vals_list,
    milvus::query::Plan* plan) {
    auto order_by_fields = plan->plan_node_->search_info_.order_by_fields_;
    if (!order_by_fields.has_value() || order_by_fields.value().empty()) {
        return;
    }

    const auto& fields = order_by_fields.value();
    int num_results = order_by_vals_list.size();

    // Populate each order_by field separately
    for (std::size_t field_idx = 0; field_idx < fields.size(); field_idx++) {
        const auto& order_by_field = fields[field_idx];
        auto field_id = order_by_field.field_id_;
        auto schema_field = plan->schema_->operator[](field_id);
        auto data_type = schema_field.get_data_type();

        // Extract values for this field from order_by_vals_list
        std::vector<OrderByValueType> field_vals;
        field_vals.reserve(num_results);
        for (const auto& order_by_vals : order_by_vals_list) {
            if (field_idx < order_by_vals.size()) {
                field_vals.push_back(order_by_vals[field_idx]);
            } else {
                field_vals.push_back(std::nullopt);
            }
        }

        // Add a new FieldData to the repeated field
        auto* field_data = search_result->add_order_by_field_values();
        field_data->set_field_id(field_id.get());
        AssembleOneOrderByField(field_data, field_vals, data_type);
    }
}

}  // namespace milvus::segcore