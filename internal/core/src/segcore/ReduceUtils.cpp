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

namespace milvus::segcore {

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

void
AssembleOrderByValues(
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_result,
    const std::vector<std::vector<OrderByValueType>>& order_by_vals_list,
    milvus::query::Plan* plan) {
    auto order_by_fields = plan->plan_node_->search_info_.order_by_fields_;
    if (!order_by_fields.has_value() || order_by_fields.value().empty()) {
        return;
    }

    // Use the first order_by field (since OrderByFieldValue is a single FieldData)
    const auto& first_order_by_field = order_by_fields.value()[0];
    auto order_by_field_id = first_order_by_field.field_id_;
    auto order_by_field = plan->schema_->operator[](order_by_field_id);
    auto order_by_data_type = order_by_field.get_data_type();

    // Extract values for the first order_by field from order_by_vals_list
    std::vector<OrderByValueType> first_field_vals;
    first_field_vals.reserve(order_by_vals_list.size());
    for (const auto& order_by_vals : order_by_vals_list) {
        if (!order_by_vals.empty()) {
            first_field_vals.push_back(order_by_vals[0]);
        } else {
            first_field_vals.push_back(std::nullopt);
        }
    }

    auto valid_data =
        std::make_unique<google::protobuf::RepeatedField<bool>>();
    valid_data->Resize(first_field_vals.size(), true);
    auto order_by_res_values =
        std::make_unique<milvus::proto::schema::ScalarField>();
    auto mutable_order_by_field_value =
        search_result->mutable_order_by_field_value();
    int order_by_val_size = first_field_vals.size();

    switch (order_by_data_type) {
        case DataType::INT8: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Int8);
            auto field_data = order_by_res_values->mutable_int_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    int8_t val =
                        std::get<int8_t>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::INT16: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Int16);
            auto field_data = order_by_res_values->mutable_int_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    int16_t val =
                        std::get<int16_t>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::INT32: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Int32);
            auto field_data = order_by_res_values->mutable_int_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    int32_t val =
                        std::get<int32_t>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::INT64: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Int64);
            auto field_data = order_by_res_values->mutable_long_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    int64_t val =
                        std::get<int64_t>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::TIMESTAMPTZ: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Timestamptz);
            auto field_data =
                order_by_res_values->mutable_timestamptz_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    int64_t val =
                        std::get<int64_t>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::BOOL: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Bool);
            auto field_data = order_by_res_values->mutable_bool_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    bool val = std::get<bool>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::VarChar);
            auto field_data = order_by_res_values->mutable_string_data();
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    std::string val =
                        std::get<std::string>(first_field_vals[idx].value());
                    *(field_data->mutable_data()->Add()) = val;
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::FLOAT: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Float);
            auto field_data = order_by_res_values->mutable_float_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    float val = std::get<float>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::DOUBLE: {
            mutable_order_by_field_value->set_type(
                milvus::proto::schema::DataType::Double);
            auto field_data = order_by_res_values->mutable_double_data();
            field_data->mutable_data()->Resize(order_by_val_size, 0);
            for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                if (first_field_vals[idx].has_value()) {
                    double val = std::get<double>(first_field_vals[idx].value());
                    field_data->mutable_data()->Set(idx, val);
                } else {
                    valid_data->Set(idx, false);
                }
            }
            break;
        }
        case DataType::JSON: {
            // For JSON, use the json_path from the first order_by field
            auto json_path = first_order_by_field.json_path_;
            if (!json_path.has_value()) {
                // If no json_path, treat as string
                mutable_order_by_field_value->set_type(
                    milvus::proto::schema::DataType::VarChar);
                auto field_data = order_by_res_values->mutable_string_data();
                for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                    if (first_field_vals[idx].has_value()) {
                        std::string val = std::get<std::string>(
                            first_field_vals[idx].value());
                        *(field_data->mutable_data()->Add()) = val;
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
            } else {
                // JSON with path - determine type from the actual values
                // For simplicity, assume string type for JSON paths
                mutable_order_by_field_value->set_type(
                    milvus::proto::schema::DataType::VarChar);
                auto field_data = order_by_res_values->mutable_string_data();
                for (std::size_t idx = 0; idx < order_by_val_size; idx++) {
                    if (first_field_vals[idx].has_value()) {
                        std::string val = std::get<std::string>(
                            first_field_vals[idx].value());
                        *(field_data->mutable_data()->Add()) = val;
                    } else {
                        valid_data->Set(idx, false);
                    }
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported datatype for order_by operations ",
                            order_by_data_type));
        }
    }

    mutable_order_by_field_value->mutable_valid_data()->MergeFrom(
        *valid_data);
    mutable_order_by_field_value->mutable_scalars()->MergeFrom(
        *order_by_res_values.get());
}

}  // namespace milvus::segcore