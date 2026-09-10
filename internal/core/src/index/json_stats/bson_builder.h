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
#include <bson/bson.h>
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/protobuf_utils.h"
#include "index/json_stats/utils.h"

namespace milvus::index {

// An owned scalar (or pre-serialized array) value held by a DomNode. Replaces
// the former bsoncxx::types::bson_value::value: we keep the raw payload here and
// append it to a libbson document at conversion time.
struct DomScalar {
    JSONType type{JSONType::NONE};
    bool b{false};
    int32_t i32{0};
    int64_t i64{0};
    double d{0.0};
    std::string str;                 // STRING payload
    std::vector<uint8_t> arr_bytes;  // ARRAY payload: raw BSON array bytes
};

class DomNode {
 public:
    enum class Type { DOCUMENT, VALUE };
    Type type;

    std::map<std::string, DomNode> document_children;
    std::optional<DomScalar> value;

    DomNode(Type t = Type::DOCUMENT) : type(t) {
    }
    DomNode(DomScalar v) : type(Type::VALUE), value(std::move(v)) {
    }
};

// RAII owner of a libbson document buffer, replacing the former
// bsoncxx::builder::basic::document. Exposes the serialized BSON bytes.
class BsonDocument {
 public:
    BsonDocument() {
        bson_init(&bson_);
    }
    ~BsonDocument() {
        bson_destroy(&bson_);
    }
    BsonDocument(const BsonDocument&) = delete;
    BsonDocument&
    operator=(const BsonDocument&) = delete;

    bson_t*
    get() {
        return &bson_;
    }
    const uint8_t*
    data() const {
        return bson_get_data(&bson_);
    }
    uint32_t
    length() const {
        return bson_.len;
    }

 private:
    bson_t bson_;
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
    ConvertDomToBson(const DomNode& node, bson_t* builder);

    // helper function to recursively extract keys with offset
    static void
    ExtractOffsetsRecursive(
        const uint8_t* base_ptr,
        const uint8_t* current_base_ptr,
        const std::string& current_path,
        std::vector<std::pair<std::string, size_t>>& result);

    static std::vector<std::pair<std::string, size_t>>
    ExtractBsonKeyOffsets(const uint8_t* data, size_t size) {
        std::vector<std::pair<std::string, size_t>> result;
        ExtractOffsetsRecursive(data, data, "", result);
        return result;
    }
};
}  // namespace milvus::index
