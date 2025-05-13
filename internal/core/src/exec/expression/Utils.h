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

namespace milvus {
namespace exec {

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
            PanicInfo(
                UnexpectedError,
                "RowVector result must have a first ColumnVector children");
        }
    } else {
        PanicInfo(UnexpectedError,
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
                PanicInfo(DataTypeInvalid,
                          "unsupported data type {}",
                          arr2.array(i).val_case());
        }
        i++;
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
        PanicInfo(Unsupported,
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

enum class MatchType {
    ExactMatch,
    PrefixMatch,
    PostfixMatch,
    // The different between InnerMatch and Match is that InnerMatch is used for
    // %xxx% while Match could be %xxx%xxx%
    InnerMatch,
    Match
};
struct ParsedResult {
    std::string literal;
    MatchType type;
};

inline std::optional<ParsedResult>
parse_ngram_pattern(const std::string& pattern) {
    if (pattern.empty()) {
        return std::nullopt;
    }

    std::vector<size_t> percent_indices;
    bool was_escaped = false;
    for (size_t i = 0; i < pattern.length(); ++i) {
        char c = pattern[i];
        if (c == '%' && !was_escaped) {
            percent_indices.push_back(i);
        } else if (c == '_' && !was_escaped) {
            // todo: now not support '_'
            return std::nullopt;
        }
        was_escaped = (c == '\\' && !was_escaped);
    }

    MatchType match_type;
    size_t core_start = 0;
    size_t core_length = 0;
    size_t n = percent_indices.size();

    if (n == 0) {
        match_type = MatchType::ExactMatch;
        core_start = 0;
        core_length = pattern.length();
    } else if (n == 1) {
        if (pattern.length() == 1) {
            return std::nullopt;
        }

        size_t idx = percent_indices[0];
        // case: %xxx
        if (idx == 0 && pattern.length() > 1) {
            match_type = MatchType::PrefixMatch;
            core_start = 1;
            core_length = pattern.length() - 1;
        } else if (idx == pattern.length() - 1 && pattern.length() > 1) {
            // case: xxx%
            match_type = MatchType::PostfixMatch;
            core_start = 0;
            core_length = pattern.length() - 1;
        } else {
            // case: xxx%xxx
            match_type = MatchType::Match;
        }
    } else if (n == 2) {
        size_t idx1 = percent_indices[0];
        size_t idx2 = percent_indices[1];
        if (idx1 == 0 && idx2 == pattern.length() - 1 && pattern.length() > 2) {
            // case: %xxx%
            match_type = MatchType::InnerMatch;
            core_start = 1;
            core_length = pattern.length() - 2;
        } else {
            match_type = MatchType::Match;
        }
    } else {
        match_type = MatchType::Match;
    }

    if (match_type == MatchType::Match) {
        // not supported now
        return std::nullopt;
    }

    // Extract the literal from the pattern
    std::string_view core_pattern =
        std::string_view(pattern).substr(core_start, core_length);

    std::string r;
    r.reserve(2 * core_pattern.size());
    bool escape_mode = false;
    for (char c : core_pattern) {
        if (escape_mode) {
            if (is_special(c)) {
                // todo(SpadeA): may not be suitable for ngram? Not use ngram in this case for now.
                return std::nullopt;
            }
            r += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                escape_mode = true;
            } else if (c == '%') {
                // unreachable
            } else if (c == '_') {
                // unreachable
                return std::nullopt;
            } else {
                if (is_special(c)) {
                    r += '\\';
                }
                r += c;
            }
        }
    }
    return std::optional<ParsedResult>{ParsedResult{std::move(r), match_type}};
}

}  // namespace exec
}  // namespace milvus