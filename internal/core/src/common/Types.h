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

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>
#include <nlohmann/json.hpp>
#include <NamedType/named_type.hpp>
#include <boost/align/aligned_allocator.hpp>
#include <boost/container/vector.hpp>
#include <boost/dynamic_bitset.hpp>
#include <folly/FBVector.h>
#include <arrow/type.h>

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "fmt/core.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/operands.h"
#include "simdjson.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "Json.h"

#include "CustomBitset.h"

namespace milvus {

using idx_t = int64_t;
using offset_t = int32_t;
using date_t = int32_t;
using distance_t = float;

using float16 = knowhere::fp16;
using bfloat16 = knowhere::bf16;
using bin1 = knowhere::bin1;
using int8 = knowhere::int8;

// See also: https://github.com/milvus-io/milvus-proto/blob/master/proto/schema.proto
enum class DataType {
    NONE = 0,
    BOOL = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,

    FLOAT = 10,
    DOUBLE = 11,

    STRING = 20,
    VARCHAR = 21,
    ARRAY = 22,
    JSON = 23,
    // GEOMETRY = 24 // reserved in proto
    TEXT = 25,

    // Some special Data type, start from after 50
    // just for internal use now, may sync proto in future
    ROW = 50,

    VECTOR_BINARY = 100,
    VECTOR_FLOAT = 101,
    VECTOR_FLOAT16 = 102,
    VECTOR_BFLOAT16 = 103,
    VECTOR_SPARSE_FLOAT = 104,
    VECTOR_INT8 = 105,
};

using Timestamp = uint64_t;  // TODO: use TiKV-like timestamp
constexpr auto MAX_TIMESTAMP = std::numeric_limits<Timestamp>::max();
constexpr auto MAX_ROW_COUNT = std::numeric_limits<idx_t>::max();

using OpType = proto::plan::OpType;
using ArithOpType = proto::plan::ArithOpType;
using ScalarArray = proto::schema::ScalarField;
using DataArray = proto::schema::FieldData;
using VectorArray = proto::schema::VectorField;
using IdArray = proto::schema::IDs;
using InsertRecordProto = proto::segcore::InsertRecord;
using PkType = std::variant<std::monostate, int64_t, std::string>;
using DefaultValueType = proto::schema::ValueField;

inline size_t
GetDataTypeSize(DataType data_type, int dim = 1) {
    switch (data_type) {
        case DataType::BOOL:
            return sizeof(bool);
        case DataType::INT8:
            return sizeof(int8_t);
        case DataType::INT16:
            return sizeof(int16_t);
        case DataType::INT32:
            return sizeof(int32_t);
        case DataType::INT64:
            return sizeof(int64_t);
        case DataType::FLOAT:
            return sizeof(float);
        case DataType::DOUBLE:
            return sizeof(double);
        case DataType::VECTOR_FLOAT:
            return sizeof(float) * dim;
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0, "dim={}", dim);
            return dim / 8;
        }
        case DataType::VECTOR_FLOAT16:
            return sizeof(float16) * dim;
        case DataType::VECTOR_BFLOAT16:
            return sizeof(bfloat16) * dim;
        case DataType::VECTOR_INT8:
            return sizeof(int8) * dim;
        // Not supporting variable length types(such as VECTOR_SPARSE_FLOAT and
        // VARCHAR) here intentionally. We can't easily estimate the size of
        // them. Caller of this method must handle this case themselves and must
        // not pass variable length types to this method.
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format("failed to get data type size, invalid type {}",
                            data_type));
        }
    }
}

inline std::shared_ptr<arrow::DataType>
GetArrowDataType(DataType data_type, int dim = 1) {
    switch (data_type) {
        case DataType::BOOL:
            return arrow::boolean();
        case DataType::INT8:
            return arrow::int8();
        case DataType::INT16:
            return arrow::int16();
        case DataType::INT32:
            return arrow::int32();
        case DataType::INT64:
            return arrow::int64();
        case DataType::FLOAT:
            return arrow::float32();
        case DataType::DOUBLE:
            return arrow::float64();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return arrow::utf8();
        case DataType::ARRAY:
        case DataType::JSON:
            return arrow::binary();
        case DataType::VECTOR_FLOAT:
            return arrow::fixed_size_binary(dim * 4);
        case DataType::VECTOR_BINARY: {
            return arrow::fixed_size_binary((dim + 7) / 8);
        }
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
            return arrow::fixed_size_binary(dim * 2);
        case DataType::VECTOR_SPARSE_FLOAT:
            return arrow::binary();
        case DataType::VECTOR_INT8:
            return arrow::fixed_size_binary(dim);
        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("failed to get data type, invalid type {}",
                                  data_type));
        }
    }
}

template <typename T>
inline size_t
GetVecRowSize(int64_t dim) {
    if constexpr (std::is_same_v<T, bin1>) {
        return (dim / 8) * sizeof(bin1);
    } else {
        return dim * sizeof(T);
    }
}

// TODO: use magic_enum when available
inline std::string
GetDataTypeName(DataType data_type) {
    switch (data_type) {
        case DataType::NONE:
            return "none";
        case DataType::BOOL:
            return "bool";
        case DataType::INT8:
            return "int8_t";
        case DataType::INT16:
            return "int16_t";
        case DataType::INT32:
            return "int32_t";
        case DataType::INT64:
            return "int64_t";
        case DataType::FLOAT:
            return "float";
        case DataType::DOUBLE:
            return "double";
        case DataType::STRING:
            return "string";
        case DataType::VARCHAR:
            return "varChar";
        case DataType::ARRAY:
            return "array";
        case DataType::JSON:
            return "json";
        case DataType::TEXT:
            return "text";
        case DataType::VECTOR_FLOAT:
            return "vector_float";
        case DataType::VECTOR_BINARY:
            return "vector_binary";
        case DataType::VECTOR_FLOAT16:
            return "vector_float16";
        case DataType::VECTOR_BFLOAT16:
            return "vector_bfloat16";
        case DataType::VECTOR_SPARSE_FLOAT:
            return "vector_sparse_float";
        case DataType::VECTOR_INT8:
            return "vector_int8";
        default:
            PanicInfo(DataTypeInvalid, "Unsupported DataType({})", data_type);
    }
}

inline size_t
CalcPksSize(const PkType* data, size_t n) {
    size_t size = 0;
    for (size_t i = 0; i < n; ++i) {
        size += sizeof(data[i]);
        if (std::holds_alternative<std::string>(data[i])) {
            size += std::get<std::string>(data[i]).size();
        }
    }
    return size;
}

using GroupByValueType = std::optional<std::variant<std::monostate,
                                                    int8_t,
                                                    int16_t,
                                                    int32_t,
                                                    int64_t,
                                                    bool,
                                                    std::string>>;
using ContainsType = proto::plan::JSONContainsExpr_JSONOp;
using NullExprType = proto::plan::NullExpr_NullOp;

inline bool
IsPrimaryKeyDataType(DataType data_type) {
    return data_type == DataType::INT64 || data_type == DataType::VARCHAR;
}

inline bool
IsIntegerDataType(DataType data_type) {
    switch (data_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
            return true;
        default:
            return false;
    }
}

inline bool
IsFloatDataType(DataType data_type) {
    switch (data_type) {
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return true;
        default:
            return false;
    }
}

inline bool
IsNumericDataType(DataType data_type) {
    return IsIntegerDataType(data_type) || IsFloatDataType(data_type);
}

inline bool
IsStringDataType(DataType data_type) {
    switch (data_type) {
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TEXT:
            return true;
        default:
            return false;
    }
}

inline bool
IsJsonDataType(DataType data_type) {
    return data_type == DataType::JSON;
}

inline bool
IsArrayDataType(DataType data_type) {
    return data_type == DataType::ARRAY;
}

inline bool
IsBinaryDataType(DataType data_type) {
    return IsJsonDataType(data_type) || IsArrayDataType(data_type);
}

inline bool
IsPrimitiveType(proto::schema::DataType type) {
    switch (type) {
        case proto::schema::DataType::Bool:
        case proto::schema::DataType::Int8:
        case proto::schema::DataType::Int16:
        case proto::schema::DataType::Int32:
        case proto::schema::DataType::Int64:
        case proto::schema::DataType::Float:
        case proto::schema::DataType::Double:
        case proto::schema::DataType::String:
        case proto::schema::DataType::VarChar:
            return true;
        default:
            return false;
    }
}

inline bool
IsJsonType(proto::schema::DataType type) {
    return type == proto::schema::DataType::JSON;
}

inline bool
IsArrayType(proto::schema::DataType type) {
    return type == proto::schema::DataType::Array;
}

inline bool
IsBinaryVectorDataType(DataType data_type) {
    return data_type == DataType::VECTOR_BINARY;
}

inline bool
IsDenseFloatVectorDataType(DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
            return true;
        default:
            return false;
    }
}

inline bool
IsSparseFloatVectorDataType(DataType data_type) {
    return data_type == DataType::VECTOR_SPARSE_FLOAT;
}

inline bool
IsIntVectorDataType(DataType data_type) {
    return data_type == DataType::VECTOR_INT8;
}

inline bool
IsFloatVectorDataType(DataType data_type) {
    return IsDenseFloatVectorDataType(data_type) ||
           IsSparseFloatVectorDataType(data_type);
}

inline bool
IsVectorDataType(DataType data_type) {
    return IsBinaryVectorDataType(data_type) ||
           IsFloatVectorDataType(data_type) || IsIntVectorDataType(data_type);
}

inline bool
IsVariableDataType(DataType data_type) {
    return IsStringDataType(data_type) || IsBinaryDataType(data_type) ||
           IsSparseFloatVectorDataType(data_type);
}

// NOTE: dependent type
// used at meta-template programming
template <class...>
constexpr std::true_type always_true{};

template <class...>
constexpr std::false_type always_false{};

template <typename T>
using aligned_vector =
    std::vector<T, boost::alignment::aligned_allocator<T, 64>>;

namespace impl {
// hide identifier name to make auto-completion happy
struct FieldIdTag;
struct FieldNameTag;
struct FieldOffsetTag;
struct SegOffsetTag;
};  // namespace impl

using FieldId = fluent::
    NamedType<int64_t, impl::FieldIdTag, fluent::Comparable, fluent::Hashable>;
using FieldName = fluent::NamedType<std::string,
                                    impl::FieldNameTag,
                                    fluent::Comparable,
                                    fluent::Hashable>;

// field id -> (field name, field type, binlog paths)
using OptFieldT = std::unordered_map<
    int64_t,
    std::tuple<std::string, milvus::DataType, std::vector<std::string>>>;

// using FieldOffset = fluent::NamedType<int64_t, impl::FieldOffsetTag, fluent::Comparable, fluent::Hashable>;
using SegOffset =
    fluent::NamedType<int64_t, impl::SegOffsetTag, fluent::Arithmetic>;

//using BitsetType = boost::dynamic_bitset<>;
using BitsetType = CustomBitset;
using BitsetTypeView = CustomBitsetView;
using BitsetTypePtr = std::shared_ptr<BitsetType>;
using BitsetTypeOpt = std::optional<BitsetType>;

template <typename Type>
using FixedVector = folly::fbvector<
    Type>;  // boost::container::vector has memory leak when version > 1.79, so use folly::fbvector instead

using Config = nlohmann::json;
//using TargetBitmap = std::vector<bool>;
//using TargetBitmapPtr = std::unique_ptr<TargetBitmap>;
using TargetBitmap = CustomBitset;
using TargetBitmapView = CustomBitsetView;
using TargetBitmapPtr = std::unique_ptr<TargetBitmap>;

using BinaryPtr = knowhere::BinaryPtr;
using BinarySet = knowhere::BinarySet;
using Dataset = knowhere::DataSet;
using DatasetPtr = knowhere::DataSetPtr;
using MetricType = knowhere::MetricType;
using IndexVersion = knowhere::IndexVersion;
// TODO :: type define milvus index type(vector index type and scalar index type)
using IndexType = knowhere::IndexType;

inline bool
IndexIsSparse(const IndexType& index_type) {
    return index_type == knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
           index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND;
}

inline bool
IsFloatVectorMetricType(const MetricType& metric_type) {
    return metric_type == knowhere::metric::L2 ||
           metric_type == knowhere::metric::IP ||
           metric_type == knowhere::metric::COSINE ||
           metric_type == knowhere::metric::BM25;
}

inline bool
IsBinaryVectorMetricType(const MetricType& metric_type) {
    return metric_type == knowhere::metric::HAMMING ||
           metric_type == knowhere::metric::JACCARD ||
           metric_type == knowhere::metric::SUPERSTRUCTURE ||
           metric_type == knowhere::metric::SUBSTRUCTURE;
}

inline bool
IsIntVectorMetricType(const MetricType& metric_type) {
    return metric_type == knowhere::metric::L2 ||
           metric_type == knowhere::metric::IP ||
           metric_type == knowhere::metric::COSINE;
}

// Plus 1 because we can't use greater(>) symbol
constexpr size_t REF_SIZE_THRESHOLD = 16 + 1;

//using BitsetBlockType = BitsetType::block_type;
//constexpr size_t BITSET_BLOCK_SIZE = sizeof(BitsetType::block_type);
//constexpr size_t BITSET_BLOCK_BIT_SIZE = sizeof(BitsetType::block_type) * 8;
template <typename T>
using MayConstRef = std::conditional_t<std::is_same_v<T, std::string> ||
                                           std::is_same_v<T, milvus::Json>,
                                       const T&,
                                       T>;
static_assert(std::is_same_v<const std::string&, MayConstRef<std::string>>);

template <DataType T>
struct TypeTraits {};

template <>
struct TypeTraits<DataType::NONE> {
    static constexpr const char* Name = "NONE";
};
template <>
struct TypeTraits<DataType::BOOL> {
    using NativeType = bool;
    static constexpr DataType TypeKind = DataType::BOOL;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "BOOL";
};

template <>
struct TypeTraits<DataType::INT8> {
    using NativeType = int8_t;
    static constexpr DataType TypeKind = DataType::INT8;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "INT8";
};

template <>
struct TypeTraits<DataType::INT16> {
    using NativeType = int16_t;
    static constexpr DataType TypeKind = DataType::INT16;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "INT16";
};

template <>
struct TypeTraits<DataType::INT32> {
    using NativeType = int32_t;
    static constexpr DataType TypeKind = DataType::INT32;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "INT32";
};

template <>
struct TypeTraits<DataType::INT64> {
    using NativeType = int64_t;
    static constexpr DataType TypeKind = DataType::INT64;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "INT64";
};

template <>
struct TypeTraits<DataType::FLOAT> {
    using NativeType = float;
    static constexpr DataType TypeKind = DataType::FLOAT;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "FLOAT";
};

template <>
struct TypeTraits<DataType::DOUBLE> {
    using NativeType = double;
    static constexpr DataType TypeKind = DataType::DOUBLE;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = true;
    static constexpr const char* Name = "DOUBLE";
};

template <>
struct TypeTraits<DataType::VARCHAR> {
    using NativeType = std::string;
    static constexpr DataType TypeKind = DataType::VARCHAR;
    static constexpr bool IsPrimitiveType = true;
    static constexpr bool IsFixedWidth = false;
    static constexpr const char* Name = "VARCHAR";
};

template <>
struct TypeTraits<DataType::STRING> : public TypeTraits<DataType::VARCHAR> {
    static constexpr DataType TypeKind = DataType::STRING;
    static constexpr const char* Name = "STRING";
};

template <>
struct TypeTraits<DataType::TEXT> : public TypeTraits<DataType::VARCHAR> {
    static constexpr DataType TypeKind = DataType::TEXT;
    static constexpr const char* Name = "TEXT";
};

template <>
struct TypeTraits<DataType::ARRAY> {
    using NativeType = void;
    static constexpr DataType TypeKind = DataType::ARRAY;
    static constexpr bool IsPrimitiveType = false;
    static constexpr bool IsFixedWidth = false;
    static constexpr const char* Name = "ARRAY";
};

template <>
struct TypeTraits<DataType::JSON> {
    using NativeType = void;
    static constexpr DataType TypeKind = DataType::JSON;
    static constexpr bool IsPrimitiveType = false;
    static constexpr bool IsFixedWidth = false;
    static constexpr const char* Name = "JSON";
};

template <>
struct TypeTraits<DataType::ROW> {
    using NativeType = void;
    static constexpr DataType TypeKind = DataType::ROW;
    static constexpr bool IsPrimitiveType = false;
    static constexpr bool IsFixedWidth = false;
    static constexpr const char* Name = "ROW";
};

template <>
struct TypeTraits<DataType::VECTOR_BINARY> {
    using NativeType = uint8_t;
    static constexpr DataType TypeKind = DataType::VECTOR_BINARY;
    static constexpr bool IsPrimitiveType = false;
    static constexpr bool IsFixedWidth = false;
    static constexpr const char* Name = "VECTOR_BINARY";
};

template <>
struct TypeTraits<DataType::VECTOR_FLOAT> {
    using NativeType = float;
    static constexpr DataType TypeKind = DataType::VECTOR_FLOAT;
    static constexpr bool IsPrimitiveType = false;
    static constexpr bool IsFixedWidth = false;
    static constexpr const char* Name = "VECTOR_FLOAT";
};

inline DataType
FromValCase(milvus::proto::plan::GenericValue::ValCase val_case) {
    switch (val_case) {
        case milvus::proto::plan::GenericValue::ValCase::kBoolVal:
            return milvus::DataType::BOOL;
        case milvus::proto::plan::GenericValue::ValCase::kInt64Val:
            return DataType::INT64;
        case milvus::proto::plan::GenericValue::ValCase::kFloatVal:
            return DataType::DOUBLE;
        case milvus::proto::plan::GenericValue::ValCase::kStringVal:
            return DataType::VARCHAR;
        case milvus::proto::plan::GenericValue::ValCase::kArrayVal:
            return DataType::ARRAY;
        default:
            return DataType::NONE;
    }
}
}  // namespace milvus
template <>
struct fmt::formatter<milvus::DataType> : formatter<string_view> {
    auto
    format(milvus::DataType c, format_context& ctx) const {
        string_view name = "unknown";
        switch (c) {
            case milvus::DataType::NONE:
                name = "NONE";
                break;
            case milvus::DataType::BOOL:
                name = "BOOL";
                break;
            case milvus::DataType::INT8:
                name = "INT8";
                break;
            case milvus::DataType::INT16:
                name = "INT16";
                break;
            case milvus::DataType::INT32:
                name = "INT32";
                break;
            case milvus::DataType::INT64:
                name = "INT64";
                break;
            case milvus::DataType::FLOAT:
                name = "FLOAT";
                break;
            case milvus::DataType::DOUBLE:
                name = "DOUBLE";
                break;
            case milvus::DataType::STRING:
                name = "STRING";
                break;
            case milvus::DataType::VARCHAR:
                name = "VARCHAR";
                break;
            case milvus::DataType::TEXT:
                name = "TEXT";
                break;
            case milvus::DataType::ARRAY:
                name = "ARRAY";
                break;
            case milvus::DataType::JSON:
                name = "JSON";
                break;
            case milvus::DataType::ROW:
                name = "ROW";
                break;
            case milvus::DataType::VECTOR_BINARY:
                name = "VECTOR_BINARY";
                break;
            case milvus::DataType::VECTOR_FLOAT:
                name = "VECTOR_FLOAT";
                break;
            case milvus::DataType::VECTOR_FLOAT16:
                name = "VECTOR_FLOAT16";
                break;
            case milvus::DataType::VECTOR_BFLOAT16:
                name = "VECTOR_BFLOAT16";
                break;
            case milvus::DataType::VECTOR_SPARSE_FLOAT:
                name = "VECTOR_SPARSE_FLOAT";
                break;
            case milvus::DataType::VECTOR_INT8:
                name = "VECTOR_INT8";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<milvus::OpType> : formatter<string_view> {
    auto
    format(milvus::OpType c, format_context& ctx) const {
        string_view name = "unknown";
        switch (c) {
            case milvus::OpType::Invalid:
                name = "Invalid";
                break;
            case milvus::OpType::GreaterThan:
                name = "GreaterThan";
                break;
            case milvus::OpType::GreaterEqual:
                name = "GreaterEqual";
                break;
            case milvus::OpType::LessThan:
                name = "LessThan";
                break;
            case milvus::OpType::LessEqual:
                name = "LessEqual";
                break;
            case milvus::OpType::Equal:
                name = "Equal";
                break;
            case milvus::OpType::NotEqual:
                name = "NotEqual";
                break;
            case milvus::OpType::PrefixMatch:
                name = "PrefixMatch";
                break;
            case milvus::OpType::PostfixMatch:
                name = "PostfixMatch";
                break;
            case milvus::OpType::Match:
                name = "Match";
                break;
            case milvus::OpType::Range:
                name = "Range";
                break;
            case milvus::OpType::In:
                name = "In";
                break;
            case milvus::OpType::NotIn:
                name = "NotIn";
                break;
            case milvus::OpType::OpType_INT_MIN_SENTINEL_DO_NOT_USE_:
                name = "OpType_INT_MIN_SENTINEL_DO_NOT_USE";
                break;
            case milvus::OpType::OpType_INT_MAX_SENTINEL_DO_NOT_USE_:
                name = "OpType_INT_MAX_SENTINEL_DO_NOT_USE";
                break;
            case milvus::OpType::TextMatch:
                name = "TextMatch";
                break;
            case milvus::OpType::PhraseMatch:
                name = "PhraseMatch";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<milvus::proto::schema::DataType>
    : formatter<std::string> {
    auto
    format(milvus::proto::schema::DataType dt, format_context& ctx) const {
        std::string name;
        switch (dt) {
            case milvus::proto::schema::DataType::None:
                name = "None";
                break;
            case milvus::proto::schema::DataType::Bool:
                name = "Bool";
                break;
            case milvus::proto::schema::DataType::Int8:
                name = "Int8";
                break;
            case milvus::proto::schema::DataType::Int16:
                name = "Int16";
                break;
            case milvus::proto::schema::DataType::Int32:
                name = "Int32";
                break;
            case milvus::proto::schema::DataType::Int64:
                name = "Int64";
                break;
            case milvus::proto::schema::DataType::Float:
                name = "Float";
                break;
            case milvus::proto::schema::DataType::Double:
                name = "Double";
                break;
            case milvus::proto::schema::DataType::String:
                name = "String";
                break;
            case milvus::proto::schema::DataType::VarChar:
                name = "VarChar";
                break;
            case milvus::proto::schema::DataType::Array:
                name = "Array";
                break;
            case milvus::proto::schema::DataType::JSON:
                name = "JSON";
                break;
            case milvus::proto::schema::DataType::Geometry:
                name = "Geometry";
                break;
            case milvus::proto::schema::DataType::Text:
                name = "Text";
                break;
            case milvus::proto::schema::DataType::BinaryVector:
                name = "BinaryVector";
                break;
            case milvus::proto::schema::DataType::FloatVector:
                name = "FloatVector";
                break;
            case milvus::proto::schema::DataType::Float16Vector:
                name = "Float16Vector";
                break;
            case milvus::proto::schema::DataType::BFloat16Vector:
                name = "BFloat16Vector";
                break;
            case milvus::proto::schema::DataType::Int8Vector:
                name = "Int8Vector";
                break;
            default:
                name = "Unknown";
        }
        return formatter<std::string>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<arrow::Type::type> : fmt::formatter<std::string> {
    auto
    format(arrow::Type::type type, fmt::format_context& ctx) const {
        std::string name;
        switch (type) {
            case arrow::Type::NA:
                name = "Null";
                break;
            case arrow::Type::BOOL:
                name = "Bool";
                break;
            case arrow::Type::UINT8:
                name = "UInt8";
                break;
            case arrow::Type::INT8:
                name = "Int8";
                break;
            case arrow::Type::UINT16:
                name = "UInt16";
                break;
            case arrow::Type::INT16:
                name = "Int16";
                break;
            case arrow::Type::UINT32:
                name = "UInt32";
                break;
            case arrow::Type::INT32:
                name = "Int32";
                break;
            case arrow::Type::UINT64:
                name = "UInt64";
                break;
            case arrow::Type::INT64:
                name = "Int64";
                break;
            case arrow::Type::HALF_FLOAT:
                name = "HalfFloat";
                break;
            case arrow::Type::FLOAT:
                name = "Float";
                break;
            case arrow::Type::DOUBLE:
                name = "Double";
                break;
            case arrow::Type::STRING:
                name = "String";
                break;
            case arrow::Type::BINARY:
                name = "Binary";
                break;
            case arrow::Type::FIXED_SIZE_BINARY:
                name = "FixedSizeBinary";
                break;
            case arrow::Type::DATE32:
                name = "Date32";
                break;
            case arrow::Type::DATE64:
                name = "Date64";
                break;
            case arrow::Type::TIMESTAMP:
                name = "Timestamp";
                break;
            case arrow::Type::TIME32:
                name = "Time32";
                break;
            case arrow::Type::TIME64:
                name = "Time64";
                break;
            case arrow::Type::INTERVAL_MONTHS:
                name = "IntervalMonths";
                break;
            case arrow::Type::INTERVAL_DAY_TIME:
                name = "IntervalDayTime";
                break;
            case arrow::Type::DECIMAL128:
                name = "Decimal128";
                break;
            case arrow::Type::DECIMAL256:
                name = "Decimal256";
                break;
            case arrow::Type::LIST:
                name = "List";
                break;
            case arrow::Type::STRUCT:
                name = "Struct";
                break;
            case arrow::Type::SPARSE_UNION:
                name = "SparseUnion";
                break;
            case arrow::Type::DENSE_UNION:
                name = "DenseUnion";
                break;
            case arrow::Type::DICTIONARY:
                name = "Dictionary";
                break;
            case arrow::Type::MAP:
                name = "Map";
                break;
            case arrow::Type::EXTENSION:
                name = "Extension";
                break;
            case arrow::Type::FIXED_SIZE_LIST:
                name = "FixedSizeList";
                break;
            case arrow::Type::DURATION:
                name = "Duration";
                break;
            case arrow::Type::LARGE_STRING:
                name = "LargeString";
                break;
            case arrow::Type::LARGE_BINARY:
                name = "LargeBinary";
                break;
            case arrow::Type::LARGE_LIST:
                name = "LargeList";
                break;
            case arrow::Type::INTERVAL_MONTH_DAY_NANO:
                name = "IntervalMonthDayNano";
                break;
            case arrow::Type::RUN_END_ENCODED:
                name = "RunEndEncoded";
                break;
            case arrow::Type::STRING_VIEW:
                name = "StringView";
                break;
            case arrow::Type::BINARY_VIEW:
                name = "BinaryView";
                break;
            case arrow::Type::LIST_VIEW:
                name = "ListView";
                break;
            case arrow::Type::LARGE_LIST_VIEW:
                name = "LargeListView";
                break;
            default:
                name = "Unknown";
        }
        return fmt::formatter<std::string>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<SegmentType> : fmt::formatter<std::string> {
    auto
    format(SegmentType type, fmt::format_context& ctx) const {
        std::string name;
        switch (type) {
            case Invalid:
                name = "Invalid";
                break;
            case Growing:
                name = "Growing";
                break;
            case Sealed:
                name = "Sealed";
                break;
            case Indexing:
                name = "Indexing";
                break;
            default:
                name = "Unknown";
        }
        return fmt::formatter<std::string>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<milvus::ErrorCode> : fmt::formatter<std::string> {
    auto
    format(milvus::ErrorCode code, fmt::format_context& ctx) const {
        std::string name;
        switch (code) {
            case milvus::Success:
                name = "Success";
                break;
            case milvus::UnexpectedError:
                name = "UnexpectedError";
                break;
            case milvus::NotImplemented:
                name = "NotImplemented";
                break;
            case milvus::Unsupported:
                name = "Unsupported";
                break;
            case milvus::IndexBuildError:
                name = "IndexBuildError";
                break;
            case milvus::IndexAlreadyBuild:
                name = "IndexAlreadyBuild";
                break;
            case milvus::ConfigInvalid:
                name = "ConfigInvalid";
                break;
            case milvus::DataTypeInvalid:
                name = "DataTypeInvalid";
                break;
            case milvus::PathInvalid:
                name = "PathInvalid";
                break;
            case milvus::PathAlreadyExist:
                name = "PathAlreadyExist";
                break;
            case milvus::PathNotExist:
                name = "PathNotExist";
                break;
            case milvus::FileOpenFailed:
                name = "FileOpenFailed";
                break;
            case milvus::FileCreateFailed:
                name = "FileCreateFailed";
                break;
            case milvus::FileReadFailed:
                name = "FileReadFailed";
                break;
            case milvus::FileWriteFailed:
                name = "FileWriteFailed";
                break;
            case milvus::BucketInvalid:
                name = "BucketInvalid";
                break;
            case milvus::ObjectNotExist:
                name = "ObjectNotExist";
                break;
            case milvus::S3Error:
                name = "S3Error";
                break;
            case milvus::RetrieveError:
                name = "RetrieveError";
                break;
            case milvus::FieldIDInvalid:
                name = "FieldIDInvalid";
                break;
            case milvus::FieldAlreadyExist:
                name = "FieldAlreadyExist";
                break;
            case milvus::OpTypeInvalid:
                name = "OpTypeInvalid";
                break;
            case milvus::DataIsEmpty:
                name = "DataIsEmpty";
                break;
            case milvus::DataFormatBroken:
                name = "DataFormatBroken";
                break;
            case milvus::JsonKeyInvalid:
                name = "JsonKeyInvalid";
                break;
            case milvus::MetricTypeInvalid:
                name = "MetricTypeInvalid";
                break;
            case milvus::FieldNotLoaded:
                name = "FieldNotLoaded";
                break;
            case milvus::ExprInvalid:
                name = "ExprInvalid";
                break;
            case milvus::UnistdError:
                name = "UnistdError";
                break;
            case milvus::MetricTypeNotMatch:
                name = "MetricTypeNotMatch";
                break;
            case milvus::DimNotMatch:
                name = "DimNotMatch";
                break;
            case milvus::ClusterSkip:
                name = "ClusterSkip";
                break;
            case milvus::MemAllocateFailed:
                name = "MemAllocateFailed";
                break;
            case milvus::MemAllocateSizeNotMatch:
                name = "MemAllocateSizeNotMatch";
                break;
            case milvus::MmapError:
                name = "MmapError";
                break;
            case milvus::FollyOtherException:
                name = "FollyOtherException";
                break;
            case milvus::FollyCancel:
                name = "FollyCancel";
                break;
            case milvus::OutOfRange:
                name = "OutOfRange";
                break;
            case milvus::GcpNativeError:
                name = "GcpNativeError";
                break;
            case milvus::TextIndexNotFound:
                name = "TextIndexNotFound";
                break;
            case milvus::InvalidParameter:
                name = "InvalidParameter";
                break;
            case milvus::KnowhereError:
                name = "KnowhereError";
                break;
            default:
                name = "UnknownError";
                break;
        }
        return fmt::formatter<std::string>::format(name, ctx);
    }
};
