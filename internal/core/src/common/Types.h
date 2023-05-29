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

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "simdjson.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "Json.h"

namespace milvus {

using idx_t = int64_t;
using offset_t = int32_t;
using date_t = int32_t;
using distance_t = float;

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

    VECTOR_BINARY = 100,
    VECTOR_FLOAT = 101,
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
using InsertData = proto::segcore::InsertRecord;
using PkType = std::variant<std::monostate, int64_t, std::string>;

inline bool
IsPrimaryKeyDataType(DataType data_type) {
    return data_type == DataType::INT64 || data_type == DataType::VARCHAR;
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
// using FieldOffset = fluent::NamedType<int64_t, impl::FieldOffsetTag, fluent::Comparable, fluent::Hashable>;
using SegOffset =
    fluent::NamedType<int64_t, impl::SegOffsetTag, fluent::Arithmetic>;

using BitsetType = boost::dynamic_bitset<>;
using BitsetTypePtr = std::shared_ptr<boost::dynamic_bitset<>>;
using BitsetTypeOpt = std::optional<BitsetType>;

template <typename Type>
using FixedVector = boost::container::vector<Type>;

using Config = nlohmann::json;
using TargetBitmap = FixedVector<bool>;
using TargetBitmapPtr = std::unique_ptr<TargetBitmap>;

using BinaryPtr = knowhere::BinaryPtr;
using BinarySet = knowhere::BinarySet;
using Dataset = knowhere::DataSet;
using DatasetPtr = knowhere::DataSetPtr;
using MetricType = knowhere::MetricType;
// TODO :: type define milvus index type(vector index type and scalar index type)
using IndexType = knowhere::IndexType;

// Plus 1 because we can't use greater(>) symbol
constexpr size_t REF_SIZE_THRESHOLD = 16 + 1;

using BitsetBlockType = BitsetType::block_type;
constexpr size_t BITSET_BLOCK_SIZE = sizeof(BitsetType::block_type);
constexpr size_t BITSET_BLOCK_BIT_SIZE = sizeof(BitsetType::block_type) * 8;
template <typename T>
using MayConstRef = std::conditional_t<std::is_same_v<T, std::string> ||
                                           std::is_same_v<T, milvus::Json>,
                                       const T&,
                                       T>;
static_assert(std::is_same_v<const std::string&, MayConstRef<std::string>>);
}  // namespace milvus
   //
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
            case milvus::DataType::ARRAY:
                name = "ARRAY";
                break;
            case milvus::DataType::JSON:
                name = "JSON";
                break;
            case milvus::DataType::VECTOR_BINARY:
                name = "VECTOR_BINARY";
                break;
            case milvus::DataType::VECTOR_FLOAT:
                name = "VECTOR_FLOAT";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
