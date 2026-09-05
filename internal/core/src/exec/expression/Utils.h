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
#include "exec/expression/JsonNumberComparison.h"
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

// Ops served by the per-field text index (segment_->GetTextIndex()) instead of
// the scalar index path; add a new text-index op here and the dispatch follows.
inline bool
IsTextIndexOpType(proto::plan::OpType op) {
    return op == proto::plan::OpType::TextMatch ||
           op == proto::plan::OpType::PhraseMatch ||
           op == proto::plan::OpType::TextMatchFuzzy;
}

[[maybe_unused]] static ColumnVectorPtr
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

template <typename JsonValue>
std::optional<int>
CompareJsonArrayNumberToBound(JsonValue&& value,
                              const proto::plan::GenericValue& bound) {
    using ValueType = std::remove_cv_t<std::remove_reference_t<JsonValue>>;
    if constexpr (std::is_same_v<ValueType, simdjson::dom::element>) {
        switch (value.type()) {
            case simdjson::dom::element_type::INT64: {
                auto number = value.get_int64();
                return number.error()
                           ? std::nullopt
                           : CompareJsonNumberToBound(number.value(), bound);
            }
            case simdjson::dom::element_type::UINT64: {
                auto number = value.get_uint64();
                return number.error()
                           ? std::nullopt
                           : CompareJsonNumberToBound(
                                 static_cast<double>(number.value()), bound);
            }
            case simdjson::dom::element_type::DOUBLE: {
                auto number = value.get_double();
                return number.error()
                           ? std::nullopt
                           : CompareJsonNumberToBound(number.value(), bound);
            }
            default:
                return std::nullopt;
        }
    } else {
        auto number = value.get_number();
        return number.error()
                   ? std::nullopt
                   : CompareJsonNumberToBoundWithUint64DoubleFallback(
                         number.value(), bound);
    }
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
            case proto::plan::GenericValue::kInt64Val:
            case proto::plan::GenericValue::kFloatVal: {
                auto comparison =
                    CompareJsonArrayNumberToBound(it, arr2.array(i));
                if (!comparison.has_value() || *comparison != 0) {
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
                ThrowInfo(UnexpectedError,
                          "unsupported data type {}",
                          arr2.array(i).val_case());
        }
        i++;
    }
    return true;
}

template <>
inline bool
CompareTwoJsonArray<milvus::bson::array_view>(milvus::bson::array_view arr1,
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
                if (bson_elem.type() != milvus::bson::type::k_bool) {
                    return false;
                }
                if (bson_elem.get_bool().value != proto_elem.bool_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kInt64Val:
            case proto::plan::GenericValue::kFloatVal: {
                auto comparison =
                    CompareBsonNumberToBound(bson_elem.get_value(), proto_elem);
                if (!comparison.has_value() || *comparison != 0) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kStringVal: {
                if (bson_elem.type() != milvus::bson::type::k_string) {
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
                  value_proto.ShortDebugString());
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
