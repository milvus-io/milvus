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

#include <bson/bson.h>
#include <simdjson.h>
#include "common/FastMem.h"
#include <string.h>
#include <cstdint>
#include <exception>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "common/bson_shim.h"
#include "glog/logging.h"
#include "index/json_stats/bson_builder.h"
#include "log/Log.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/element.h"
#include "simdjson/dom/object.h"
#include "simdjson/dom/parser.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace {

// Append a simdjson DOM element to a libbson document/array target under the
// given key (for arrays the key is the stringified index).
void
AppendJsonElementToBson(simdjson::dom::element elem,
                        bson_t* out,
                        const char* key,
                        int key_len) {
    using simdjson::dom::element_type;

    auto type = elem.type();
    switch (type) {
        case element_type::STRING: {
            auto sv = std::string_view(elem.get_string());
            bson_append_utf8(
                out, key, key_len, sv.data(), static_cast<int>(sv.size()));
            break;
        }
        case element_type::INT64:
            bson_append_int64(out, key, key_len, int64_t(elem.get_int64()));
            break;
        case element_type::UINT64:
            bson_append_double(out, key, key_len, double(elem.get_uint64()));
            break;
        case element_type::DOUBLE:
            bson_append_double(out, key, key_len, double(elem.get_double()));
            break;
        case element_type::BOOL:
            bson_append_bool(out, key, key_len, bool(elem.get_bool()));
            break;
        case element_type::NULL_VALUE:
            bson_append_null(out, key, key_len);
            break;
        case element_type::OBJECT: {
            bson_t child;
            bson_append_document_begin(out, key, key_len, &child);
            for (auto [k, v] : elem.get_object()) {
                AppendJsonElementToBson(
                    v, &child, k.data(), static_cast<int>(k.size()));
            }
            bson_append_document_end(out, &child);
            break;
        }
        case element_type::ARRAY: {
            bson_t child;
            bson_append_array_begin(out, key, key_len, &child);
            uint32_t i = 0;
            char buf[16];
            const char* idx_key = nullptr;
            for (simdjson::dom::element v : elem.get_array()) {
                size_t klen =
                    bson_uint32_to_string(i, &idx_key, buf, sizeof(buf));
                AppendJsonElementToBson(
                    v, &child, idx_key, static_cast<int>(klen));
                i++;
            }
            bson_append_array_end(out, &child);
            break;
        }
        default:
            bson_append_null(out, key, key_len);
            break;
    }
}

}  // namespace

std::vector<uint8_t>
BuildBsonArrayBytesFromJsonString(const std::string& json_array) {
    simdjson::dom::parser parser;
    simdjson::dom::element root = parser.parse(json_array);
    if (root.type() != simdjson::dom::element_type::ARRAY) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "input is not a JSON array: {}",
                  json_array);
    }

    bson_t arr;
    bson_init(&arr);
    uint32_t i = 0;
    char buf[16];
    const char* idx_key = nullptr;
    for (simdjson::dom::element elem : root.get_array()) {
        size_t klen = bson_uint32_to_string(i, &idx_key, buf, sizeof(buf));
        AppendJsonElementToBson(elem, &arr, idx_key, static_cast<int>(klen));
        i++;
    }
    std::vector<uint8_t> out(bson_get_data(&arr),
                             bson_get_data(&arr) + arr.len);
    bson_destroy(&arr);
    return out;
}

void
BsonBuilder::AppendToDom(DomNode& root,
                         const std::vector<std::string>& keys,
                         const std::string& value,
                         const JSONType& type) {
    LOG_TRACE("append to dom: {} with value {} and type {}",
              Join(keys, "."),
              value,
              ToString(type));
    DomNode* current = &root;
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string& key = keys[i];
        if (i == keys.size() - 1) {
            current->document_children[key] = CreateValueNode(value, type);
        } else {
            auto& children = current->document_children;
            auto it = children.find(key);
            if (it != children.end()) {
                if (it->second.type != DomNode::Type::DOCUMENT) {
                    it->second = DomNode(DomNode::Type::DOCUMENT);
                }
                current = &it->second;
            } else {
                children[key] = DomNode(DomNode::Type::DOCUMENT);
                current = &children[key];
            }
        }
    }
}

DomNode
BsonBuilder::CreateValueNode(const std::string& value, JSONType type) {
    switch (type) {
        case JSONType::NONE: {
            DomScalar s;
            s.type = JSONType::NONE;
            return DomNode(std::move(s));
        }
        case JSONType::BOOL: {
            DomScalar s;
            s.type = JSONType::BOOL;
            s.b = (value == "true" || value == "1");
            return DomNode(std::move(s));
        }
        case JSONType::INT32: {
            DomScalar s;
            s.type = JSONType::INT32;
            s.i32 = std::stoi(value);
            return DomNode(std::move(s));
        }
        case JSONType::INT64: {
            DomScalar s;
            s.type = JSONType::INT64;
            s.i64 = std::stoll(value);
            return DomNode(std::move(s));
        }
        case JSONType::DOUBLE: {
            DomScalar s;
            s.type = JSONType::DOUBLE;
            s.d = std::stod(value);
            return DomNode(std::move(s));
        }
        case JSONType::STRING: {
            DomScalar s;
            s.type = JSONType::STRING;
            s.str = value;
            return DomNode(std::move(s));
        }
        case JSONType::ARRAY: {
            try {
                DomScalar s;
                s.type = JSONType::ARRAY;
                s.arr_bytes = BuildBsonArrayBytesFromJsonString(value);
                return DomNode(std::move(s));
            } catch (const simdjson::simdjson_error& e) {
                ThrowInfo(
                    ErrorCode::UnexpectedError,
                    "Failed to build bson array (simdjson) from string: {}, {}",
                    value,
                    e.what());
            } catch (const std::exception& e) {
                ThrowInfo(
                    ErrorCode::UnexpectedError,
                    "Failed to build bson array (generic) from string: {}, {}",
                    value,
                    e.what());
            }
        }
        case JSONType::OBJECT: {
            AssertInfo(value == "{}",
                       "object value should be empty but got {}",
                       value);
            // return an empty json object as a document node
            return DomNode(DomNode::Type::DOCUMENT);
        }
        default:
            ThrowInfo(ErrorCode::Unsupported, "Unsupported JSON type {}", type);
    }
}

void
BsonBuilder::ConvertDomToBson(const DomNode& node, bson_t* builder) {
    for (const auto& [key, child] : node.document_children) {
        const char* k = key.c_str();
        const int klen = static_cast<int>(key.size());
        switch (child.type) {
            case DomNode::Type::VALUE: {
                const DomScalar& s = child.value.value();
                switch (s.type) {
                    case JSONType::NONE:
                        bson_append_null(builder, k, klen);
                        break;
                    case JSONType::BOOL:
                        bson_append_bool(builder, k, klen, s.b);
                        break;
                    case JSONType::INT32:
                        bson_append_int32(builder, k, klen, s.i32);
                        break;
                    case JSONType::INT64:
                        bson_append_int64(builder, k, klen, s.i64);
                        break;
                    case JSONType::DOUBLE:
                        bson_append_double(builder, k, klen, s.d);
                        break;
                    case JSONType::STRING:
                        bson_append_utf8(builder,
                                         k,
                                         klen,
                                         s.str.data(),
                                         static_cast<int>(s.str.size()));
                        break;
                    case JSONType::ARRAY: {
                        bson_t arr;
                        if (bson_init_static(
                                &arr, s.arr_bytes.data(), s.arr_bytes.size())) {
                            bson_append_array(builder, k, klen, &arr);
                        }
                        break;
                    }
                    default:
                        ThrowInfo(ErrorCode::Unsupported,
                                  "Unsupported scalar JSON type {}",
                                  static_cast<int>(s.type));
                }
                break;
            }
            case DomNode::Type::DOCUMENT: {
                bson_t child_doc;
                bson_append_document_begin(builder, k, klen, &child_doc);
                ConvertDomToBson(child, &child_doc);
                bson_append_document_end(builder, &child_doc);
                break;
            }
            default: {
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported DOM node type {}",
                          static_cast<int>(child.type));
            }
        }
    }
}

void
BsonBuilder::ExtractOffsetsRecursive(
    const uint8_t* root_base_ptr,
    const uint8_t* current_base_ptr,
    const std::string& current_path,
    std::vector<std::pair<std::string, size_t>>& result) {
    uint32_t length;
    milvus::fastmem::FastMemcpy(&length, current_base_ptr, 4);

    const uint8_t* end_ptr = current_base_ptr + length - 1;
    AssertInfo(*(end_ptr) == 0x00, "miss bson document terminator");

    const uint8_t* ptr = current_base_ptr + 4;

    while (ptr <= end_ptr && *ptr != 0x00) {
        // record key offset
        size_t key_offset = ptr - root_base_ptr;

        // read key type
        auto type = static_cast<milvus::bson::type>(*ptr++);

        // read key
        auto key_name = reinterpret_cast<const char*>(ptr);
        ptr += strlen(key_name) + 1;

        // construct key path
        std::string key_path = AppendJsonPointer(current_path, key_name);

        // do not record key offset pair for null value
        // because null value is not a valid key
        if (type != milvus::bson::type::k_null) {
            result.emplace_back(key_path, key_offset);
        }

        // handle value
        switch (type) {
            case milvus::bson::type::k_document: {
                ExtractOffsetsRecursive(root_base_ptr, ptr, key_path, result);
                // skip sub doc
                uint32_t child_len;
                milvus::fastmem::FastMemcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case milvus::bson::type::k_array: {
                // not parse array
                // skip sub doc
                uint32_t child_len;
                milvus::fastmem::FastMemcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case milvus::bson::type::k_string: {
                uint32_t str_len;
                milvus::fastmem::FastMemcpy(&str_len, ptr, 4);
                ptr += 4 + str_len;
                break;
            }
            case milvus::bson::type::k_int32: {
                ptr += 4;
                break;
            }
            case milvus::bson::type::k_int64: {
                ptr += 8;
                break;
            }
            case milvus::bson::type::k_double: {
                ptr += 8;
                break;
            }
            case milvus::bson::type::k_bool: {
                ptr += 1;
                break;
            }
            case milvus::bson::type::k_null: {
                break;
            }
            default: {
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported BSON type {}",
                          static_cast<int>(type));
            }
        }
    }
}

}  // namespace milvus::index
