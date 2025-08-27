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

#include <optional>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "segcore/SegmentInterface.h"
#include "query/Utils.h"
#include "common/RegexQuery.h"
#include "exec/expression/Utils.h"
#include "common/bson_view.h"

namespace milvus {
namespace exec {

template <typename T, typename U>
bool
UnaryCompare(const T& get_value, const U& val, proto::plan::OpType op_type) {
    switch (op_type) {
        case proto::plan::GreaterThan:
            return get_value > val;
        case proto::plan::GreaterEqual:
            return get_value >= val;
        case proto::plan::LessThan:
            return get_value < val;
        case proto::plan::LessEqual:
            return get_value <= val;
        case proto::plan::Equal:
            return get_value == val;
        case proto::plan::NotEqual:
            return get_value != val;
        case proto::plan::InnerMatch:
        case proto::plan::PostfixMatch:
        case proto::plan::PrefixMatch:
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                return milvus::query::Match(get_value, val, op_type);
            } else {
                return false;
            }
        case proto::plan::Match:
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                PatternMatchTranslator translator;
                auto regex_pattern = translator(val);
                RegexMatcher matcher(regex_pattern);
                return matcher(get_value);
            } else {
                return false;
            }
        default:
            return false;
    }
}

template <typename T, FilterType filter_type = FilterType::sequential>
struct UnaryElementFuncForMatch {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

    void
    operator()(const T* src,
               size_t size,
               IndexInnerType val,
               TargetBitmapView res) {
        static_assert(
            filter_type == FilterType::sequential,
            "this override operator() of UnaryElementFuncForMatch does "
            "not support FilterType::random");

        PatternMatchTranslator translator;
        auto regex_pattern = translator(val);
        RegexMatcher matcher(regex_pattern);

        for (int i = 0; i < size; ++i) {
            res[i] = matcher(src[i]);
        }
    }

    void
    operator()(const T* src,
               size_t size,
               IndexInnerType val,
               TargetBitmapView res,
               const TargetBitmap& bitmap_input,
               int start_cursor,
               const int32_t* offsets = nullptr) {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(val);
        RegexMatcher matcher(regex_pattern);
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            if (has_bitmap_input && !bitmap_input[i + start_cursor]) {
                continue;
            }
            if constexpr (filter_type == FilterType::random) {
                res[i] = matcher(src[offsets ? offsets[i] : i]);
            } else {
                res[i] = matcher(src[i]);
            }
        }
    }
};

template <typename T,
          proto::plan::OpType op,
          FilterType filter_type = FilterType::sequential>
struct UnaryElementFunc {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

    void
    operator()(const T* src,
               size_t size,
               TargetBitmapView res,
               IndexInnerType val) {
        static_assert(filter_type == FilterType::sequential,
                      "this override operator() of UnaryElementFunc does not "
                      "support FilterType::random");
        if constexpr (op == proto::plan::OpType::Match) {
            UnaryElementFuncForMatch<T> func;
            func(src, size, val, res);
            return;
        }

        if constexpr (std::is_same_v<T, std::string_view> ||
                      std::is_same_v<T, std::string>) {
            for (int i = 0; i < size; ++i) {
                if constexpr (op == proto::plan::OpType::Equal) {
                    res[i] = src[i] == val;
                } else if constexpr (op == proto::plan::OpType::NotEqual) {
                    res[i] = src[i] != val;
                } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                    res[i] = src[i] > val;
                } else if constexpr (op == proto::plan::OpType::LessThan) {
                    res[i] = src[i] < val;
                } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                    res[i] = src[i] >= val;
                } else if constexpr (op == proto::plan::OpType::LessEqual) {
                    res[i] = src[i] <= val;
                } else if constexpr (op == proto::plan::OpType::PrefixMatch ||
                                     op == proto::plan::OpType::PostfixMatch ||
                                     op == proto::plan::OpType::InnerMatch) {
                    res[i] = milvus::query::Match(src[i], val, op);
                } else {
                    ThrowInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported op_type:{} for UnaryElementFunc", op));
                }
            }
            return;
        }

        if constexpr (op == proto::plan::OpType::Equal) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::EQ>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::NotEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::NE>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::GreaterThan) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::GT>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::LessThan) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::LT>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::GE>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::LessEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::LE>(
                src, size, val);
        } else {
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("unsupported op_type:{} for UnaryElementFunc", op));
        }
    }

    void
    operator()(const T* src,
               size_t size,
               IndexInnerType val,
               TargetBitmapView res,
               const TargetBitmap& bitmap_input,
               size_t start_cursor,
               const int32_t* offsets = nullptr) {
        bool has_bitmap_input = !bitmap_input.empty();
        if constexpr (op == proto::plan::OpType::Match) {
            UnaryElementFuncForMatch<T, filter_type> func;
            func(src, size, val, res, bitmap_input, start_cursor, offsets);
            return;
        }

        // This is the original code, which is kept for the documentation purposes
        // also, for iterative filter
        if constexpr (filter_type == FilterType::random) {
            for (int i = 0; i < size; ++i) {
                auto offset = (offsets != nullptr) ? offsets[i] : i;
                if constexpr (op == proto::plan::OpType::Equal) {
                    res[i] = src[offset] == val;
                } else if constexpr (op == proto::plan::OpType::NotEqual) {
                    res[i] = src[offset] != val;
                } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                    res[i] = src[offset] > val;
                } else if constexpr (op == proto::plan::OpType::LessThan) {
                    res[i] = src[offset] < val;
                } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                    res[i] = src[offset] >= val;
                } else if constexpr (op == proto::plan::OpType::LessEqual) {
                    res[i] = src[offset] <= val;
                } else if constexpr (op == proto::plan::OpType::PrefixMatch ||
                                     op == proto::plan::OpType::PostfixMatch ||
                                     op == proto::plan::OpType::InnerMatch) {
                    res[i] = milvus::query::Match(src[offset], val, op);
                } else {
                    ThrowInfo(OpTypeInvalid,
                              "unsupported op_type:{} for UnaryElementFunc",
                              op);
                }
            }
            return;
        }

        if (has_bitmap_input) {
            if constexpr (std::is_same_v<T, std::string_view> ||
                          std::is_same_v<T, std::string>) {
                for (int i = 0; i < size; ++i) {
                    if (!bitmap_input[i + start_cursor]) {
                        continue;
                    }
                    if constexpr (op == proto::plan::OpType::Equal) {
                        res[i] = src[i] == val;
                    } else if constexpr (op == proto::plan::OpType::NotEqual) {
                        res[i] = src[i] != val;
                    } else if constexpr (op ==
                                         proto::plan::OpType::GreaterThan) {
                        res[i] = src[i] > val;
                    } else if constexpr (op == proto::plan::OpType::LessThan) {
                        res[i] = src[i] < val;
                    } else if constexpr (op ==
                                         proto::plan::OpType::GreaterEqual) {
                        res[i] = src[i] >= val;
                    } else if constexpr (op == proto::plan::OpType::LessEqual) {
                        res[i] = src[i] <= val;
                    } else if constexpr (op ==
                                             proto::plan::OpType::PrefixMatch ||
                                         op == proto::plan::OpType::
                                                   PostfixMatch ||
                                         op ==
                                             proto::plan::OpType::InnerMatch) {
                        res[i] = milvus::query::Match(src[i], val, op);
                    } else {
                        ThrowInfo(OpTypeInvalid,
                                  "unsupported op_type:{} for UnaryElementFunc",
                                  op);
                    }
                }
                return;
            }
        }

        if constexpr (op == proto::plan::OpType::PrefixMatch ||
                      op == proto::plan::OpType::PostfixMatch ||
                      op == proto::plan::OpType::InnerMatch) {
            for (int i = 0; i < size; ++i) {
                res[i] = milvus::query::Match(src[i], val, op);
            }
            return;
        }

        if constexpr (op == proto::plan::OpType::Equal) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::EQ>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::NotEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::NE>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::GreaterThan) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::GT>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::LessThan) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::LT>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::GE>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::LessEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::LE>(
                src, size, val);
        } else {
            ThrowInfo(OpTypeInvalid,
                      "unsupported op_type:{} for UnaryElementFunc",
                      op);
        }
    }
};

#define UnaryArrayCompare(cmp)                                          \
    do {                                                                \
        if constexpr (std::is_same_v<GetType, proto::plan::Array>) {    \
            res[i] = false;                                             \
        } else {                                                        \
            if (index >= src[i].length()) {                             \
                res[i] = false;                                         \
                continue;                                               \
            }                                                           \
            auto array_data = src[i].template get_data<GetType>(index); \
            res[i] = (cmp);                                             \
        }                                                               \
    } while (false)

template <typename ValueType, proto::plan::OpType op, FilterType filter_type>
struct UnaryElementFuncForArray {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    void
    operator()(const ArrayView* src,
               const bool* valid_data,
               size_t size,
               ValueType val,
               int index,
               TargetBitmapView res,
               TargetBitmapView valid_res,
               const TargetBitmap& bitmap_input,
               size_t start_cursor,
               const int32_t* offsets = nullptr) {
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[i + start_cursor]) {
                continue;
            }
            if constexpr (op == proto::plan::OpType::Equal) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    res[i] = src[offset].is_same_array(val);
                } else {
                    if (index >= src[offset].length()) {
                        res[i] = false;
                        continue;
                    }
                    auto array_data =
                        src[offset].template get_data<GetType>(index);
                    res[i] = array_data == val;
                }
            } else if constexpr (op == proto::plan::OpType::NotEqual) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    res[i] = !src[offset].is_same_array(val);
                } else {
                    if (index >= src[offset].length()) {
                        res[i] = false;
                        continue;
                    }
                    auto array_data =
                        src[offset].template get_data<GetType>(index);
                    res[i] = array_data != val;
                }
            } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                UnaryArrayCompare(array_data > val);
            } else if constexpr (op == proto::plan::OpType::LessThan) {
                UnaryArrayCompare(array_data < val);
            } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                UnaryArrayCompare(array_data >= val);
            } else if constexpr (op == proto::plan::OpType::LessEqual) {
                UnaryArrayCompare(array_data <= val);
            } else if constexpr (op == proto::plan::OpType::PrefixMatch ||
                                 op == proto::plan::OpType::PostfixMatch ||
                                 op == proto::plan::OpType::InnerMatch) {
                UnaryArrayCompare(milvus::query::Match(array_data, val, op));
            } else if constexpr (op == proto::plan::OpType::Match) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    res[i] = false;
                } else {
                    if (index >= src[offset].length()) {
                        res[i] = false;
                        continue;
                    }
                    PatternMatchTranslator translator;
                    auto regex_pattern = translator(val);
                    RegexMatcher matcher(regex_pattern);
                    auto array_data =
                        src[offset].template get_data<GetType>(index);
                    res[i] = matcher(array_data);
                }
            } else {
                ThrowInfo(OpTypeInvalid,
                          "unsupported op_type:{} for "
                          "UnaryElementFuncForArray",
                          op);
            }
        }
    }
};

template <typename T>
struct UnaryIndexFuncForMatch {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, IndexInnerType val, proto::plan::OpType op) {
        AssertInfo(op == proto::plan::OpType::Match ||
                       op == proto::plan::OpType::PostfixMatch ||
                       op == proto::plan::OpType::InnerMatch ||
                       op == proto::plan::OpType::PrefixMatch,
                   "op must be one of the following: Match, PrefixMatch, "
                   "PostfixMatch, InnerMatch");

        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            if (index->SupportPatternMatch()) {
                return index->PatternMatch(val, op);
            }

            if (!index->HasRawData()) {
                ThrowInfo(Unsupported,
                          "index don't support regex query and don't have "
                          "raw data");
            }
            // retrieve raw data to do brute force query, may be very slow.
            auto cnt = index->Count();
            TargetBitmap res(cnt);
            if (op == proto::plan::OpType::InnerMatch ||
                op == proto::plan::OpType::PostfixMatch ||
                op == proto::plan::OpType::PrefixMatch) {
                for (int64_t i = 0; i < cnt; i++) {
                    auto raw = index->Reverse_Lookup(i);
                    if (!raw.has_value()) {
                        res[i] = false;
                        continue;
                    }
                    res[i] = milvus::query::Match(raw.value(), val, op);
                }
                return res;
            } else {
                PatternMatchTranslator translator;
                auto regex_pattern = translator(val);
                RegexMatcher matcher(regex_pattern);
                for (int64_t i = 0; i < cnt; i++) {
                    auto raw = index->Reverse_Lookup(i);
                    if (!raw.has_value()) {
                        res[i] = false;
                        continue;
                    }
                    res[i] = matcher(raw.value());
                }
                return res;
            }
        }
        ThrowInfo(ErrorCode::Unsupported,
                  "UnaryIndexFuncForMatch is only supported on string types");
    }
};

template <typename T, proto::plan::OpType op>
struct UnaryIndexFunc {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, IndexInnerType val) {
        if constexpr (op == proto::plan::OpType::Equal) {
            return index->In(1, &val);
        } else if constexpr (op == proto::plan::OpType::NotEqual) {
            return index->NotIn(1, &val);
        } else if constexpr (op == proto::plan::OpType::GreaterThan) {
            return index->Range(val, OpType::GreaterThan);
        } else if constexpr (op == proto::plan::OpType::LessThan) {
            return index->Range(val, OpType::LessThan);
        } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
            return index->Range(val, OpType::GreaterEqual);
        } else if constexpr (op == proto::plan::OpType::LessEqual) {
            return index->Range(val, OpType::LessEqual);
        } else if constexpr (op == proto::plan::OpType::PrefixMatch ||
                             op == proto::plan::OpType::Match ||
                             op == proto::plan::OpType::PostfixMatch ||
                             op == proto::plan::OpType::InnerMatch) {
            UnaryIndexFuncForMatch<T> func;
            return func(index, val, op);
        } else {
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("unsupported op_type:{} for UnaryIndexFunc", op));
        }
    }
};

template <typename T, typename U>
void
BatchUnaryCompare(const T* src,
                  size_t size,
                  U& val,
                  proto::plan::OpType op_type,
                  TargetBitmapView res) {
    if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
        using milvus::bitset::CompareOpType;
        switch (op_type) {
            case proto::plan::GreaterThan: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::GT);
                return;
            }
            case proto::plan::GreaterEqual: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::GE);
                return;
            }
            case proto::plan::LessThan: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::LT);
                return;
            }
            case proto::plan::LessEqual: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::LE);
                return;
            }
            case proto::plan::Equal: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::EQ);
                return;
            }
            case proto::plan::NotEqual: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::NE);
                return;
            }
            default:
                break;
        }
    }
    switch (op_type) {
        case proto::plan::GreaterThan: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] > val;
            }
            break;
        }
        case proto::plan::GreaterEqual: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] >= val;
            }
            break;
        }
        case proto::plan::LessThan: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] < val;
            }
            break;
        }
        case proto::plan::LessEqual: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] <= val;
            }
            break;
        }
        case proto::plan::Equal: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] == val;
            }
            break;
        }
        case proto::plan::NotEqual: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] != val;
            }
            break;
        }
        case proto::plan::InnerMatch:
        case proto::plan::PostfixMatch:
        case proto::plan::PrefixMatch: {
            for (int i = 0; i < size; ++i) {
                res[i] = milvus::query::Match(src[i], val, op_type);
            }
            break;
        }
        case proto::plan::Match: {
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                PatternMatchTranslator translator;
                auto regex_pattern = translator(val);
                RegexMatcher matcher(regex_pattern);
                for (int i = 0; i < size; ++i) {
                    res[i] = matcher(src[i]);
                }
                break;
            }
        }
        default: {
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("unsupported op_type:{} for BatchUnaryCompare",
                            op_type));
        }
    }
}

template <typename GetType, typename ValType>
class ShreddingExecutor {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<GetType, std::string_view>,
                           std::string,
                           GetType>;

 public:
    ShreddingExecutor(proto::plan::OpType op_type,
                      const std::string& pointer,
                      ValType val)
        : op_type_(op_type), val_(val), pointer_(pointer) {
    }

    void
    operator()(const GetType* src,
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "need using ShreddingArrayBsonExecutor for array type in "
                      "shredding data");
        } else {
            ExecuteOperation(src, size, res);
            HandleValidData(valid, size, res, valid_res);
        }
    }

 private:
    void
    ExecuteOperation(const GetType* src, size_t size, TargetBitmapView res) {
        BatchUnaryCompare<GetType, ValType>(src, size, val_, op_type_, res);
    }

    void
    HandleValidData(const bool* valid,
                    size_t size,
                    TargetBitmapView res,
                    TargetBitmapView valid_res) {
        if (valid != nullptr) {
            for (int i = 0; i < size; ++i) {
                if (!valid[i]) {
                    res[i] = valid_res[i] = false;
                }
            }
        }
    }

    proto::plan::OpType op_type_;
    ValType val_;
    std::string pointer_;
};

// Executor for shredding ARRAY type stored as BSON binary in variable-length
// columns (std::string_view). Only supports Equal/NotEqual.
class ShreddingArrayBsonExecutor {
 public:
    ShreddingArrayBsonExecutor(proto::plan::OpType op_type,
                               const std::string& pointer,
                               const proto::plan::Array& val)
        : op_type_(op_type), val_(val), pointer_(pointer) {
    }

    void
    operator()(const std::string_view* src,
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = false;
                continue;
            }
            bool equal = CompareTwoJsonArray(array_view.value(), val_);
            switch (op_type_) {
                case proto::plan::Equal:
                    res[i] = equal;
                    break;
                case proto::plan::NotEqual:
                    res[i] = !equal;
                    break;
                default:
                    ThrowInfo(OpTypeInvalid,
                              fmt::format("unsupported op_type:{} for ARRAY in "
                                          "ShreddingArrayBsonExecutor",
                                          op_type_));
            }
        }
    }

 private:
    proto::plan::OpType op_type_;
    const proto::plan::Array& val_;
    std::string pointer_;
};

class PhyUnaryRangeFilterExpr : public SegmentExpr {
 public:
    PhyUnaryRangeFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr>& expr,
        const std::string& name,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      FromValCase(expr->val_.val_case()),
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    bool
    SupportOffsetInput() override {
        if (expr_->op_type_ == proto::plan::OpType::TextMatch ||
            expr_->op_type_ == proto::plan::OpType::PhraseMatch) {
            return false;
        }
        return true;
    }

    std::string
    ToString() const {
        return fmt::format("{}", expr_->ToString());
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    bool
    IsSource() const override {
        return true;
    }

    std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr>
    GetLogicalExpr() {
        return expr_;
    }

    proto::plan::OpType
    GetOpType() {
        return expr_->op_type_;
    }

    FieldId
    GetFieldId() {
        return expr_->column_.field_id_;
    }

    DataType
    GetFieldType() {
        return expr_->column_.data_type_;
    }

 private:
    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplJson(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplJsonByStats();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForPk(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplArray(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplArrayForIndex(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecArrayEqualForIndex(EvalCtx& context, bool reverse);

    // Check overflow and cache result for performace
    template <typename T>
    ColumnVectorPtr
    PreCheckOverflow(OffsetVector* input = nullptr);

    template <typename T>
    bool
    CanUseIndex();

    template <typename T>
    bool
    CanUseIndexForArray();

    bool
    CanUseIndexForJson(DataType val_type);

    VectorPtr
    ExecTextMatch();

    bool
    CanExecNgramMatch(proto::plan::OpType op_type);

    bool
    CanExecNgramMatchForJson(DataType val_type);

    std::optional<VectorPtr>
    ExecNgramMatch();

    std::pair<std::string, std::string>
    SplitAtFirstSlashDigit(std::string input);

 private:
    std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr> expr_;
    int64_t overflow_check_pos_{0};
    bool arg_inited_{false};
    SingleElement value_arg_;
};
}  // namespace exec
}  // namespace milvus
