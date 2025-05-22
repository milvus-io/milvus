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

#include <string>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "tantivy-binding.h"
#include "fmt/core.h"

namespace milvus {

using MilvusDataType = milvus::DataType;
class JsonCastType {
 public:
    enum class DataType { UNKNOWN, BOOL, DOUBLE, VARCHAR, ARRAY };

    static const JsonCastType UNKNOWN;

    static JsonCastType
    FromString(const std::string& str);

    DataType
    data_type() const;

    // Returns the element type if it's an array, otherwise the data type itself.
    DataType
    element_type() const;

    TantivyDataType
    ToTantivyType() const;

    MilvusDataType
    ToMilvusDataType() const;

 private:
    JsonCastType(DataType data_type, DataType element_type);

    static const std::unordered_map<std::string, const JsonCastType>
        json_cast_type_map_;

    DataType data_type_;
    DataType element_type_;
};

}  // namespace milvus

template <>
struct fmt::formatter<milvus::JsonCastType::DataType> : formatter<string_view> {
    auto
    format(milvus::JsonCastType::DataType c, format_context& ctx) const {
        string_view name = "unknown";
        switch (c) {
            case milvus::JsonCastType::DataType::BOOL:
                name = "BOOL";
                break;
            case milvus::JsonCastType::DataType::DOUBLE:
                name = "DOUBLE";
                break;
            case milvus::JsonCastType::DataType::VARCHAR:
                name = "VARCHAR";
                break;
            case milvus::JsonCastType::DataType::ARRAY:
                name = "ARRAY";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<milvus::JsonCastType> : fmt::formatter<string_view> {
    auto
    format(const milvus::JsonCastType& c, format_context& ctx) const {
        if (c.data_type() == milvus::JsonCastType::DataType::ARRAY) {
            return format_to(ctx.out(), "ARRAY_{}", c.element_type());
        }
        return format_to(ctx.out(), "{}", c.data_type());
    }
};