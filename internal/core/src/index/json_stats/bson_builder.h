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
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <tuple>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <simdjson.h>
#include <bsoncxx/types.hpp>
#include <bson/bson.h>
#include "index/json_stats/utils.h"
#include "common/EasyAssert.h"
namespace milvus::index {

class DomNode {
 public:
    enum class Type { DOCUMENT, VALUE };
    Type type;

    std::map<std::string, DomNode> document_children;
    std::optional<bsoncxx::types::bson_value::value> bson_value;

    DomNode(Type t = Type::DOCUMENT) : type(t) {
    }
    DomNode(bsoncxx::types::bson_value::value v)
        : type(Type::VALUE), bson_value(std::move(v)) {
    }
};

// Parse a JSON array string and return a self-owned buffer containing the
// BSON array bytes (length-prefixed, including terminator). The returned
// vector's data() can be used directly with bson_view.
std::vector<uint8_t>
BuildBsonArrayBytesFromJsonString(const std::string& json_array);

class BsonBuilder {
 public:
    static void
    AppendToDom(DomNode& root,
                const std::vector<std::string>& keys,
                const std::string& value,
                const JSONType& type);

    static DomNode
    CreateValueNode(const std::string& value, JSONType type);

    static void
    ConvertDomToBson(const DomNode& node,
                     bsoncxx::builder::basic::document& builder);

    // helper function to recursively extract keys with offset
    static void
    ExtractOffsetsRecursive(
        const uint8_t* base_ptr,
        const uint8_t* current_base_ptr,
        const std::string& current_path,
        std::vector<std::pair<std::string, size_t>>& result);

    static std::vector<std::pair<std::string, size_t>>
    ExtractBsonKeyOffsets(const bsoncxx::document::view& view) {
        std::vector<std::pair<std::string, size_t>> result;
        const uint8_t* raw_data = view.data();
        size_t raw_len = view.length();

        ExtractOffsetsRecursive(raw_data, raw_data, "", result);
        return result;
    }

    static std::vector<std::pair<std::string, size_t>>
    ExtractBsonKeyOffsets(const uint8_t* data, size_t size) {
        std::vector<std::pair<std::string, size_t>> result;
        ExtractOffsetsRecursive(data, data, "", result);
        return result;
    }
};
}  // namespace milvus::index
