// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"
#include "query/Utils.h"
#include "common/bson_view.h"
namespace milvus {
namespace exec {

inline bool
IsCompareOp(proto::plan::OpType op) {
    return op == proto::plan::OpType::Equal ||
           op == proto::plan::OpType::NotEqual ||
           op == proto::plan::OpType::GreaterEqual ||
           op == proto::plan::OpType::GreaterThan ||
           op == proto::plan::OpType::LessEqual ||
           op == proto::plan::OpType::LessThan;
}

static ColumnVectorPtr
GetColumnVector(const VectorPtr& result) {
    ColumnVectorPtr res;
    if (auto convert_vector = std::dynamic_pointer_cast<ColumnVector>(result)) {
        res = convert_vector;
    } else if (auto convert_vector =
                   std::dynamic_pointer_cast<RowVector>(result)) {
        if (auto convert_flat_vector = std::dynamic_pointer_cast<ColumnVector>(
                convert_vector->child(0))) {
            res = convert_flat_vector;
        } else {
            ThrowInfo(
                UnexpectedError,
                "RowVector result must have a first ColumnVector children");
        }
    } else {
        ThrowInfo(UnexpectedError,
                  "expr result must have a ColumnVector or RowVector result");
    }
    return res;
}

template <typename T>
bool
CompareTwoJsonArray(T arr1, const proto::plan::Array& arr2) {
    int json_array_length = 0;
    if constexpr (std::is_same_v<
                      T,
                      simdjson::simdjson_result<simdjson::ondemand::array>>) {
        json_array_length = arr1.count_elements();
    }
    if constexpr (std::is_same_v<T,
                                 std::vector<simdjson::simdjson_result<
                                     simdjson::ondemand::value>>>) {
        json_array_length = arr1.size();
    }

    if constexpr (std::is_same_v<
                      T,
                      simdjson::simdjson_result<simdjson::dom::array>>) {
        json_array_length = arr1.size();
    }

    if constexpr (std::is_same_v<T, simdjson::dom::array>) {
        json_array_length = arr1.size();
    }
    if (arr2.array_size() != json_array_length) {
        return false;
    }
    int i = 0;
    for (auto&& it : arr1) {
        switch (arr2.array(i).val_case()) {
            case proto::plan::GenericValue::kBoolVal: {
                auto val = it.template get<bool>();
                if (val.error() || val.value() != arr2.array(i).bool_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kInt64Val: {
                auto val = it.template get<int64_t>();
                if (val.error() || val.value() != arr2.array(i).int64_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kFloatVal: {
                auto val = it.template get<double>();
                if (val.error() || val.value() != arr2.array(i).float_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kStringVal: {
                auto val = it.template get<std::string_view>();
                if (val.error() || val.value() != arr2.array(i).string_val()) {
                    return false;
                }
                break;
            }
            default:
                ThrowInfo(DataTypeInvalid,
                          "unsupported data type {}",
                          arr2.array(i).val_case());
        }
        i++;
    }
    return true;
}

template <>
inline bool
CompareTwoJsonArray<bsoncxx::array::view>(bsoncxx::array::view arr1,
                                          const proto::plan::Array& arr2) {
    size_t bson_array_length = std::distance(arr1.begin(), arr1.end());

    if (arr2.array_size() != bson_array_length) {
        return false;
    }

    auto bson_it = arr1.begin();
    for (int i = 0; i < arr2.array_size(); ++i, ++bson_it) {
        if (bson_it == arr1.end()) {
            return false;
        }

        const auto& bson_elem = *bson_it;
        const auto& proto_elem = arr2.array(i);

        switch (proto_elem.val_case()) {
            case proto::plan::GenericValue::kBoolVal: {
                if (bson_elem.type() != bsoncxx::type::k_bool) {
                    return false;
                }
                if (bson_elem.get_bool().value != proto_elem.bool_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kInt64Val: {
                if (bson_elem.type() == bsoncxx::type::k_int32) {
                    const int32_t val = bson_elem.get_int32().value;
                    if (val != proto_elem.int64_val()) {
                        return false;
                    }
                } else if (bson_elem.type() == bsoncxx::type::k_int64) {
                    const int64_t val = bson_elem.get_int64().value;
                    if (val != proto_elem.int64_val()) {
                        return false;
                    }
                } else {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kFloatVal: {
                double bson_val;
                switch (bson_elem.type()) {
                    case bsoncxx::type::k_int32:
                        bson_val = bson_elem.get_int32().value;
                        break;
                    case bsoncxx::type::k_int64:
                        bson_val = bson_elem.get_int64().value;
                        break;
                    case bsoncxx::type::k_double:
                        bson_val = bson_elem.get_double().value;
                        break;
                    default:
                        return false;
                }
                if (bson_val != proto_elem.float_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kStringVal: {
                if (bson_elem.type() != bsoncxx::type::k_string) {
                    return false;
                }
                auto bson_str_view = bson_elem.get_string().value;
                if (std::string(bson_str_view.data(), bson_str_view.size()) !=
                    proto_elem.string_val()) {
                    return false;
                }
                break;
            }
            default:
                return false;
        }
    }

    if (bson_it != arr1.end()) {
        return false;
    }

    return true;
}

template <typename T>
T
GetValueFromProtoInternal(const milvus::proto::plan::GenericValue& value_proto,
                          bool& overflowed) {
    if constexpr (std::is_same_v<T, bool>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kBoolVal);
        return static_cast<T>(value_proto.bool_val());
    } else if constexpr (std::is_integral_v<T>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kInt64Val);
        auto val = value_proto.int64_val();
        if (milvus::query::out_of_range<T>(val)) {
            overflowed = true;
            return T();
        } else {
            return static_cast<T>(val);
        }
    } else if constexpr (std::is_floating_point_v<T>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kFloatVal);
        return static_cast<T>(value_proto.float_val());
    } else if constexpr (std::is_same_v<T, std::string> ||
                         std::is_same_v<T, std::string_view>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kStringVal);
        return static_cast<T>(value_proto.string_val());
    } else if constexpr (std::is_same_v<T, proto::plan::Array>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kArrayVal);
        return static_cast<T>(value_proto.array_val());
    } else if constexpr (std::is_same_v<T, milvus::proto::plan::GenericValue>) {
        return static_cast<T>(value_proto);
    } else {
        ThrowInfo(Unsupported,
                  "unsupported generic value {}",
                  value_proto.DebugString());
    }
}

template <typename T>
T
GetValueFromProto(const milvus::proto::plan::GenericValue& value_proto) {
    bool dummy_overflowed = false;
    return GetValueFromProtoInternal<T>(value_proto, dummy_overflowed);
}

template <typename T>
T
GetValueFromProtoWithOverflow(
    const milvus::proto::plan::GenericValue& value_proto, bool& overflowed) {
    return GetValueFromProtoInternal<T>(value_proto, overflowed);
}

template <typename T>
T
GetValueWithCastNumber(const milvus::proto::plan::GenericValue& value_proto) {
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
        Assert(value_proto.val_case() ==
                   milvus::proto::plan::GenericValue::kFloatVal ||
               value_proto.val_case() ==
                   milvus::proto::plan::GenericValue::kInt64Val);
        if (value_proto.val_case() ==
            milvus::proto::plan::GenericValue::kInt64Val) {
            return static_cast<T>(value_proto.int64_val());
        } else {
            return static_cast<T>(value_proto.float_val());
        }
    } else {
        return GetValueFromProto<T>(value_proto);
    }
}

}  // namespace exec
}  // namespace milvus