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

}  // namespace milvus::segcore