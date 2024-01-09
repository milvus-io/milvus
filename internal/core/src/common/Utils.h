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

#pragma once

#include <fcntl.h>
#include <fmt/core.h>
#include <google/protobuf/text_format.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <functional>

#include "common/Consts.h"
#include "base/FieldMeta.h"
#include "base/LoadInfo.h"
#include "common/Types.h"
#include "common/EasyAssert.h"
#include "common/VectorTrait.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "simdjson.h"
#include "simd/hook.h"

namespace milvus {
#define FIELD_DATA(data_array, type) \
    (data_array->scalars().type##_data().data())

#define VEC_FIELD_DATA(data_array, type) \
    (data_array->vectors().type##_vector().data())

inline DatasetPtr
GenDataset(const int64_t nb, const int64_t dim, const void* xb) {
    return knowhere::GenDataSet(nb, dim, xb);
}

inline const float*
GetDatasetDistance(const DatasetPtr& dataset) {
    return dataset->GetDistance();
}

inline const int64_t*
GetDatasetIDs(const DatasetPtr& dataset) {
    return dataset->GetIds();
}

inline int64_t
GetDatasetRows(const DatasetPtr& dataset) {
    return dataset->GetRows();
}

inline const void*
GetDatasetTensor(const DatasetPtr& dataset) {
    return dataset->GetTensor();
}

inline int64_t
GetDatasetDim(const DatasetPtr& dataset) {
    return dataset->GetDim();
}

inline const size_t*
GetDatasetLims(const DatasetPtr& dataset) {
    return dataset->GetLims();
}

inline bool
PrefixMatch(const std::string_view str, const std::string_view prefix) {
    if (prefix.length() > str.length()) {
        return false;
    }
    auto ret = strncmp(str.data(), prefix.data(), prefix.length());
    if (ret != 0) {
        return false;
    }

    return true;
}

inline DatasetPtr
GenIdsDataset(const int64_t count, const int64_t* ids) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(count);
    ret_ds->SetDim(1);
    ret_ds->SetIds(ids);
    ret_ds->SetIsOwner(false);
    return ret_ds;
}

inline DatasetPtr
GenResultDataset(const int64_t nq,
                 const int64_t topk,
                 const int64_t* ids,
                 const float* distance) {
    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->SetRows(nq);
    ret_ds->SetDim(topk);
    ret_ds->SetIds(ids);
    ret_ds->SetDistance(distance);
    ret_ds->SetIsOwner(true);
    return ret_ds;
}

inline bool
PostfixMatch(const std::string_view str, const std::string_view postfix) {
    if (postfix.length() > str.length()) {
        return false;
    }

    int offset = str.length() - postfix.length();
    auto ret = strncmp(str.data() + offset, postfix.data(), postfix.length());
    if (ret != 0) {
        return false;
    }
    //
    //    int i = postfix.length() - 1;
    //    int j = str.length() - 1;
    //    for (; i >= 0; i--, j--) {
    //        if (postfix[i] != str[j]) {
    //            return false;
    //        }
    //    }
    return true;
}

inline int64_t
upper_align(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups * align;
}

inline int64_t
upper_div(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = value / align + (value % align != 0);
    return groups;
}

inline bool
IsMetricType(const std::string_view str,
             const knowhere::MetricType& metric_type) {
    return !strcasecmp(str.data(), metric_type.c_str());
}

inline bool
IsFloatMetricType(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::L2) ||
           IsMetricType(metric_type, knowhere::metric::IP) ||
           IsMetricType(metric_type, knowhere::metric::COSINE);
}

inline bool
PositivelyRelated(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::IP) ||
           IsMetricType(metric_type, knowhere::metric::COSINE);
}

inline std::string
KnowhereStatusString(knowhere::Status status) {
    return knowhere::Status2String(status);
}

inline std::vector<IndexType>
DISK_INDEX_LIST() {
    static std::vector<IndexType> ret{
        knowhere::IndexEnum::INDEX_DISKANN,
    };
    return ret;
}

template <typename T>
inline bool
is_in_list(const T& t, std::function<std::vector<T>()> list_func) {
    auto l = list_func();
    return std::find(l.begin(), l.end(), t) != l.end();
}

inline bool
is_in_disk_list(const IndexType& index_type) {
    return is_in_list<IndexType>(index_type, DISK_INDEX_LIST);
}

template <typename T>
std::string
Join(const std::vector<T>& items, const std::string& delimiter) {
    std::stringstream ss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) {
            ss << delimiter;
        }
        ss << items[i];
    }
    return ss.str();
}

inline std::string
GetCommonPrefix(const std::string& str1, const std::string& str2) {
    size_t len = std::min(str1.length(), str2.length());
    size_t i = 0;
    while (i < len && str1[i] == str2[i]) ++i;
    return str1.substr(0, i);
}

template <typename T, typename U>
inline bool
Match(const T& x, const U& y, OpType op) {
    PanicInfo(NotImplemented, "not supported");
}

template <>
inline bool
Match<std::string>(const std::string& str, const std::string& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        default:
            PanicInfo(OpTypeInvalid, "not supported");
    }
}

template <>
inline bool
Match<std::string_view>(const std::string_view& str,
                        const std::string& val,
                        OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        default:
            PanicInfo(OpTypeInvalid, "not supported");
    }
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
gt_ub(int64_t t) {
    return t > std::numeric_limits<T>::max();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
lt_lb(int64_t t) {
    return t < std::numeric_limits<T>::min();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
out_of_range(int64_t t) {
    return gt_ub<T>(t) || lt_lb<T>(t);
}

inline void
AppendOneChunk(BitsetType& result, const bool* chunk_ptr, size_t chunk_len) {
    // Append a value once instead of BITSET_BLOCK_BIT_SIZE times.
    auto AppendBlock = [&result](const bool* ptr, int n) {
        for (int i = 0; i < n; ++i) {
#if defined(USE_DYNAMIC_SIMD)
            auto val = milvus::simd::get_bitset_block(ptr);
#else
            BitsetBlockType val = 0;
            // This can use CPU SIMD optimzation
            uint8_t vals[BITSET_BLOCK_SIZE] = {0};
            for (size_t j = 0; j < 8; ++j) {
                for (size_t k = 0; k < BITSET_BLOCK_SIZE; ++k) {
                    vals[k] |= uint8_t(*(ptr + k * 8 + j)) << j;
                }
            }
            for (size_t j = 0; j < BITSET_BLOCK_SIZE; ++j) {
                val |= BitsetBlockType(vals[j]) << (8 * j);
            }
#endif
            result.append(val);
            ptr += BITSET_BLOCK_SIZE * 8;
        }
    };
    // Append bit for these bits that can not be union as a block
    // Usually n less than BITSET_BLOCK_BIT_SIZE.
    auto AppendBit = [&result](const bool* ptr, int n) {
        for (int i = 0; i < n; ++i) {
            bool bit = *ptr++;
            result.push_back(bit);
        }
    };

    size_t res_len = result.size();

    int n_prefix =
        res_len % BITSET_BLOCK_BIT_SIZE == 0
            ? 0
            : std::min(BITSET_BLOCK_BIT_SIZE - res_len % BITSET_BLOCK_BIT_SIZE,
                       chunk_len);

    AppendBit(chunk_ptr, n_prefix);

    if (n_prefix == chunk_len)
        return;

    size_t n_block = (chunk_len - n_prefix) / BITSET_BLOCK_BIT_SIZE;
    size_t n_suffix = (chunk_len - n_prefix) % BITSET_BLOCK_BIT_SIZE;

    AppendBlock(chunk_ptr + n_prefix, n_block);

    AppendBit(chunk_ptr + n_prefix + n_block * BITSET_BLOCK_BIT_SIZE, n_suffix);

    return;
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, FundamentalTag, FundamentalTag) {
    return Op{}(t, u);
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, FundamentalTag, StringTag) {
    PanicInfo(DataTypeInvalid, "incompitible data type");
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, StringTag, FundamentalTag) {
    PanicInfo(DataTypeInvalid, "incompitible data type");
}

template <typename Op, typename T, typename U>
bool
RelationalImpl(const T& t, const U& u, StringTag, StringTag) {
    return Op{}(t, u);
}

template <typename Op>
struct Relational {
    template <typename T, typename U>
    bool
    operator()(const T& t, const U& u) const {
        return RelationalImpl<Op, T, U>(t,
                                        u,
                                        typename TagDispatchTrait<T>::Tag{},
                                        typename TagDispatchTrait<U>::Tag{});
    }

    template <typename... T>
    bool
    operator()(const T&...) const {
        PanicInfo(OpTypeInvalid, "incompatible operands");
    }
};

template <OpType op>
struct MatchOp {
    template <typename T, typename U>
    bool
    operator()(const T& t, const U& u) {
        return Match(t, u, op);
    }
};

}  // namespace milvus
