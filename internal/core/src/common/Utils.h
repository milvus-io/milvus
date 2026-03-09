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
#include <cmath>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/EasyAssert.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/sparse_utils.h"
#include "simdjson.h"

namespace milvus {
#define FIELD_DATA(data_array, type) \
    (data_array->scalars().type##_data().data())

#define VEC_FIELD_DATA(data_array, type) \
    (data_array->vectors().type##_vector().data())

using CheckDataValid = std::function<bool(size_t)>;
using SparseValueType = typename knowhere::sparse_u32_f32::ValueType;

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

inline bool
InnerMatch(const std::string_view str, const std::string_view pattern) {
    if (pattern.length() > str.length()) {
        return false;
    }
    return str.find(pattern) != std::string::npos;
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
PositivelyRelated(const knowhere::MetricType& metric_type) {
    return IsMetricType(metric_type, knowhere::metric::IP) ||
           IsMetricType(metric_type, knowhere::metric::COSINE) ||
           IsMetricType(metric_type, knowhere::metric::BM25) ||
           IsMetricType(metric_type, knowhere::metric::MHJACCARD) ||
           IsMetricType(metric_type, knowhere::metric::MAX_SIM) ||
           IsMetricType(metric_type, knowhere::metric::MAX_SIM_IP) ||
           IsMetricType(metric_type, knowhere::metric::MAX_SIM_COSINE);
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

inline bool
IsInteger(const std::string& str) {
    if (str.empty())
        return false;

    try {
        size_t pos;
        std::stoi(str, &pos);
        return pos == str.length();
    } catch (...) {
        return false;
    }
}

inline std::string
PrintBitsetTypeView(const BitsetTypeView& view) {
    std::stringstream ss;
    for (auto i = 0; i < view.size(); ++i) {
        ss << int(view[i]);
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

// Escape braces in the input string,
// used for fmt::format json string
inline std::string
EscapeBraces(const std::string& input) {
    std::string result;
    for (char ch : input) {
        if (ch == '{')
            result += "{{";
        else if (ch == '}')
            result += "}}";
        else
            result += ch;
    }
    return result;
}

inline knowhere::sparse::SparseRow<SparseValueType>
CopyAndWrapSparseRow(const void* data,
                     size_t size,
                     const bool validate = false) {
    size_t num_elements =
        size / knowhere::sparse::SparseRow<SparseValueType>::element_size();
    knowhere::sparse::SparseRow<SparseValueType> row(num_elements);
    std::memcpy(row.data(), data, size);
    if (validate) {
        AssertInfo(size % knowhere::sparse::SparseRow<
                              SparseValueType>::element_size() ==
                       0,
                   "Invalid size for sparse row data");
        for (size_t i = 0; i < num_elements; ++i) {
            auto element = row[i];
            AssertInfo(std::isfinite(element.val),
                       "Invalid sparse row: NaN or Inf value");
            AssertInfo(element.val >= 0, "Invalid sparse row: negative value");
            AssertInfo(
                element.id < std::numeric_limits<uint32_t>::max(),
                "Invalid sparse row: id should be smaller than uint32 max");
            if (i > 0) {
                AssertInfo(row[i - 1].id < element.id,
                           "Invalid sparse row: id should be strict ascending");
            }
        }
    }
    return row;
}

// Iterable is a list of bytes, each is a byte array representation of a single
// sparse float row. This helper function converts such byte arrays into a list
// of knowhere::sparse::SparseRow<SparseValueType>. The resulting list is a deep copy of
// the source data.
//
// Here in segcore we validate the sparse row data only for search requests,
// as the insert/upsert data are already validated in go code.
template <typename Iterable>
std::unique_ptr<knowhere::sparse::SparseRow<SparseValueType>[]>
SparseBytesToRows(const Iterable& rows, const bool validate = false) {
    if (rows.size() == 0) {
        return nullptr;
    }
    auto res = std::make_unique<knowhere::sparse::SparseRow<SparseValueType>[]>(
        rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        res[i] = std::move(
            CopyAndWrapSparseRow(rows[i].data(), rows[i].size(), validate));
    }
    return res;
}

// SparseRowsToProto converts a list of knowhere::sparse::SparseRow<SparseValueType> to
// a milvus::proto::schema::SparseFloatArray. The resulting proto is a deep copy
// of the source data. source(i) returns the i-th row to be copied.
inline void
SparseRowsToProto(
    const std::function<
        const knowhere::sparse::SparseRow<SparseValueType>*(size_t)>& source,
    int64_t rows,
    milvus::proto::schema::SparseFloatArray* proto) {
    int64_t max_dim = 0;
    for (size_t i = 0; i < rows; ++i) {
        const auto* row = source(i);
        if (row == nullptr) {
            // empty row
            proto->add_contents();
            continue;
        }
        max_dim = std::max(max_dim, row->dim());
        proto->add_contents(row->data(), row->data_byte_size());
    }
    proto->set_dim(max_dim);
}

class Defer {
 public:
    Defer(std::function<void()> fn) : fn_(fn) {
    }
    ~Defer() {
        fn_();
    }

 private:
    std::function<void()> fn_;
};

#define DeferLambda(fn) Defer Defer_##__COUNTER__(fn);

template <typename T>
FOLLY_ALWAYS_INLINE int
comparePrimitiveAsc(const T& left, const T& right) {
    if constexpr (std::is_floating_point<T>::value) {
        bool leftNan = std::isnan(left);
        bool rightNan = std::isnan(right);
        if (leftNan) {
            return rightNan ? 0 : 1;
        }
        if (rightNan) {
            return -1;
        }
    }
    return left < right ? -1 : left == right ? 0 : 1;
}

inline std::string
lowerString(const std::string& str) {
    std::string ret;
    ret.resize(str.size());
    std::transform(str.begin(), str.end(), ret.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return ret;
}

template <typename T>
T
checkPlus(const T& a, const T& b, const char* typeName = "integer") {
    static_assert(std::is_integral_v<T>, "checkPlus requires integral type");
#if defined(__GNUC__) || defined(__clang__)
    // Use compiler builtin for GCC/Clang
    T result;
    bool overflow = __builtin_add_overflow(a, b, &result);
    if (UNLIKELY(overflow)) {
        ThrowInfo(DataTypeInvalid, "{} overflow: {} + {}", typeName, a, b);
    }
    return result;
#else
    // Portable fallback for MSVC and other compilers
    constexpr T max_val = std::numeric_limits<T>::max();
    constexpr T min_val = std::numeric_limits<T>::min();

    bool overflow = false;
    if (b > 0) {
        // Positive addition: check if a > max - b
        if (a > max_val - b) {
            overflow = true;
        }
    } else if (b < 0) {
        // Negative addition: check if a < min - b
        if (a < min_val - b) {
            overflow = true;
        }
    }
    // If b == 0, no overflow possible

    if (UNLIKELY(overflow)) {
        ThrowInfo(DataTypeInvalid, "{} overflow: {} + {}", typeName, a, b);
    }
    return a + b;
#endif
}

template <typename T>
T
checkedMultiply(const T& a, const T& b, const char* typeName = "integer") {
    static_assert(std::is_integral_v<T>,
                  "checkedMultiply requires integral type");
#if defined(__GNUC__) || defined(__clang__)
    // Use compiler builtin for GCC/Clang
    T result;
    bool overflow = __builtin_mul_overflow(a, b, &result);
    if (UNLIKELY(overflow)) {
        ThrowInfo(DataTypeInvalid, "{} overflow: {} * {}", typeName, a, b);
    }
    return result;
#else
    // Portable fallback for MSVC and other compilers
    constexpr T max_val = std::numeric_limits<T>::max();
    constexpr T min_val = std::numeric_limits<T>::min();

    // Handle zero case: no overflow if either operand is zero
    if (a == 0 || b == 0) {
        return 0;
    }

    bool overflow = false;

    if constexpr (std::is_signed_v<T>) {
        // Signed type: handle MIN edge case and use absolute values
        if (a == min_val) {
            // Special case: if a is MIN, only safe multiplications are with
            // 0 (already handled), 1, or -1
            if (b != 1 && b != -1) {
                overflow = true;
            }
        } else if (b == min_val) {
            // Special case: if b is MIN, only safe multiplications are with
            // 0 (already handled), 1, or -1
            if (a != 1 && a != -1) {
                overflow = true;
            }
        } else {
            // Use division-based check: |a| > max_val / |b| implies overflow
            // Compute absolute values carefully to avoid overflow
            T abs_a = (a < 0) ? -a : a;
            T abs_b = (b < 0) ? -b : b;

            // Check: abs_a > max_val / abs_b
            // This works because both operands are not MIN (handled above)
            // and not zero (handled above)
            if (abs_a > max_val / abs_b) {
                overflow = true;
            }
        }
    } else {
        // Unsigned type: simpler case
        // Check: a > max_val / b
        if (a > max_val / b) {
            overflow = true;
        }
    }

    if (UNLIKELY(overflow)) {
        ThrowInfo(DataTypeInvalid, "{} overflow: {} * {}", typeName, a, b);
    }
    return a * b;
#endif
}

inline const char* const KSum = "sum";
inline const char* const KMin = "min";
inline const char* const KMax = "max";
inline const char* const KCount = "count";
inline const char* const KAvg = "avg";

inline DataType
GetAggResultType(std::string func_name, DataType input_type) {
    if (func_name == KSum) {
        switch (input_type) {
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64: {
                return DataType::INT64;
            }
            case DataType::TIMESTAMPTZ: {
                return DataType::TIMESTAMPTZ;
            }
            case DataType::FLOAT: {
                return DataType::DOUBLE;
            }
            case DataType::DOUBLE: {
                return DataType::DOUBLE;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          "Unsupported data type for type:{}",
                          input_type);
            }
        }
    }
    if (func_name == KAvg) {
        switch (input_type) {
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE: {
                return DataType::DOUBLE;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          "Unsupported data type for {} aggregation: {}",
                          func_name,
                          input_type);
            }
        }
    }
    if (func_name == KMin || func_name == KMax) {
        // min/max keep the original scalar type.
        switch (input_type) {
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::FLOAT:
            case DataType::DOUBLE:
            case DataType::VARCHAR:
            case DataType::STRING:
            case DataType::TEXT:
            case DataType::TIMESTAMPTZ: {
                return input_type;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          "Unsupported data type for {} aggregation: {}",
                          func_name,
                          input_type);
            }
        }
    }
    if (func_name == KCount) {
        return DataType::INT64;
    }
    ThrowInfo(OpTypeInvalid, "Unsupported func type:{}", func_name);
}

inline int32_t
Align(int32_t number, int32_t alignment) {
    AssertInfo(alignment > 0 && (alignment & (alignment - 1)) == 0,
               "Alignment must be a power of 2, got {}",
               alignment);
    return (number + alignment - 1) & ~(alignment - 1);
}

inline bool
IsStructSubField(const std::string& fieldName) {
    return fieldName.find('[') != std::string::npos;
}

}  // namespace milvus
