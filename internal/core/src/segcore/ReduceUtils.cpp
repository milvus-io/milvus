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
    if (group_by_field_id.has_value() && group_by_vals.size() > 0) {
        auto group_by_values_field =
            std::make_unique<milvus::proto::schema::ScalarField>();
        auto valid_data =
            std::make_unique<google::protobuf::RepeatedField<bool>>();
        valid_data->Resize(group_by_vals.size(), true);
        auto group_by_field =
            plan->schema_.operator[](group_by_field_id.value());
        auto group_by_data_type = group_by_field.get_data_type();

        int group_by_val_size = group_by_vals.size();
        switch (group_by_data_type) {
            case DataType::INT8: {
                auto field_data = group_by_values_field->mutable_int_data();
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
                auto field_data = group_by_values_field->mutable_int_data();
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
                auto field_data = group_by_values_field->mutable_int_data();
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
                auto field_data = group_by_values_field->mutable_long_data();
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
                auto field_data = group_by_values_field->mutable_bool_data();
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
                auto field_data = group_by_values_field->mutable_string_data();
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
            default: {
                PanicInfo(
                    DataTypeInvalid,
                    fmt::format("unsupported datatype for group_by operations ",
                                group_by_data_type));
            }
        }

        auto group_by_field_value =
            search_result->mutable_group_by_field_value();
        group_by_field_value->set_type(
            milvus::proto::schema::DataType(group_by_data_type));
        group_by_field_value->mutable_valid_data()->MergeFrom(*valid_data);
        group_by_field_value->mutable_scalars()->MergeFrom(
            *group_by_values_field.get());
        return;
    }
}

}  // namespace milvus::segcore