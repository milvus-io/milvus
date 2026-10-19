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

#include <iostream>
#include <vector>
#include <string>
#include <map>

#include "common/EasyAssert.h"
#include "common/Json.h"
#include "index/json_stats/bson_builder.h"

namespace milvus::index {

namespace {

using bsoncxx::builder::basic::kvp;

void
AppendDomElementToBsonArray(simdjson::dom::element elem,
                            bsoncxx::builder::basic::array& out);

void
AppendDomElementToBsonDocument(simdjson::dom::element elem,
                               std::string_view key,
                               bsoncxx::builder::basic::document& out) {
    using simdjson::dom::element_type;

    auto type = elem.type();
    if (type == element_type::STRING) {
        std::string s(std::string_view(elem.get_string()));
        out.append(kvp(std::string(key), std::move(s)));
    } else if (type == element_type::INT64) {
        out.append(kvp(std::string(key), int64_t(elem.get_int64())));
    } else if (type == element_type::UINT64) {
        out.append(kvp(std::string(key), int64_t(elem.get_uint64())));
    } else if (type == element_type::DOUBLE) {
        out.append(kvp(std::string(key), elem.get_double()));
    } else if (type == element_type::BOOL) {
        out.append(kvp(std::string(key), bool(elem.get_bool())));
    } else if (type == element_type::NULL_VALUE) {
        out.append(kvp(std::string(key), bsoncxx::types::b_null{}));
    } else if (type == element_type::OBJECT) {
        bsoncxx::builder::basic::document sub;
        for (auto [k, v] : elem.get_object()) {
            AppendDomElementToBsonDocument(v, k, sub);
        }
        out.append(kvp(std::string(key), sub.extract()));
    } else if (type == element_type::ARRAY) {
        bsoncxx::builder::basic::array subarr;
        for (simdjson::dom::element v : elem.get_array()) {
            AppendDomElementToBsonArray(v, subarr);
        }
        out.append(kvp(std::string(key), subarr.extract()));
    } else {
        out.append(kvp(std::string(key), bsoncxx::types::b_null{}));
    }
}

void
AppendDomElementToBsonArray(simdjson::dom::element elem,
                            bsoncxx::builder::basic::array& out) {
    using simdjson::dom::element_type;

    auto type = elem.type();
    if (type == element_type::STRING) {
        out.append(std::string(std::string_view(elem.get_string())));
    } else if (type == element_type::INT64) {
        out.append(int64_t(elem.get_int64()));
    } else if (type == element_type::UINT64) {
        out.append(int64_t(elem.get_uint64()));
    } else if (type == element_type::DOUBLE) {
        out.append(elem.get_double());
    } else if (type == element_type::BOOL) {
        out.append(bool(elem.get_bool()));
    } else if (type == element_type::NULL_VALUE) {
        out.append(bsoncxx::types::b_null{});
    } else if (type == element_type::OBJECT) {
        bsoncxx::builder::basic::document sub;
        for (auto [k, v] : elem.get_object()) {
            AppendDomElementToBsonDocument(v, k, sub);
        }
        out.append(sub.extract());
    } else if (type == element_type::ARRAY) {
        bsoncxx::builder::basic::array subarr;
        for (simdjson::dom::element v : elem.get_array()) {
            AppendDomElementToBsonArray(v, subarr);
        }
        out.append(subarr.extract());
    } else {
        out.append(bsoncxx::types::b_null{});
    }
}
}  // namespace

// Classification lives here rather than at the call sites: every simdjson access
// below -- the parse, the array iteration, and the element accessors inside
// AppendDomElementToBsonArray -- throws simdjson_error, and a caller that forgets
// to guard would let it escape to the cgo boundary and be flattened into the
// generic UnexpectedError, which the build scheduler retries forever. The
// simdjson error is mapped by the shared SimdjsonParseErrorToErrorCode, exactly
// as UnescapeJsonString does; no code is chosen here.
bsoncxx::array::value
BuildBsonArrayFromJsonString(const std::string& json_array) {
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element root = parser.parse(json_array);
        if (root.type() != simdjson::dom::element_type::ARRAY) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "input is not a JSON array: {}",
                      json_array);
        }

        bsoncxx::builder::basic::array out;
        for (simdjson::dom::element elem : root.get_array()) {
            AppendDomElementToBsonArray(elem, out);
        }
        return out.extract();
    } catch (const SegcoreError&) {
        // Already classified above; SegcoreError derives from std::runtime_error
        // and would otherwise be swallowed by a generic handler.
        throw;
    } catch (const simdjson::simdjson_error& e) {
        ThrowInfo(SimdjsonParseErrorToErrorCode(e.error()),
                  "Failed to build bson array from json string: {}, {}",
                  json_array,
                  e.what());
    }
    return bsoncxx::builder::basic::array{}.extract();
}

// BuildBsonArrayFromJsonString classifies its own failures, so a call site that
// forgets to guard cannot leak a simdjson_error to the cgo boundary.
std::vector<uint8_t>
BuildBsonArrayBytesFromJsonString(const std::string& json_array) {
    auto arr_value = BuildBsonArrayFromJsonString(json_array);
    auto view = arr_value.view();
    return std::vector<uint8_t>(view.data(), view.data() + view.length());
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
            return DomNode(bsoncxx::types::b_null{});
        }
        case JSONType::BOOL: {
            bool b = (value == "true" || value == "1");
            return DomNode(bsoncxx::types::b_bool{b});
        }
        case JSONType::INT32: {
            int32_t i = std::stoi(value);
            return DomNode(bsoncxx::types::b_int32{i});
        }
        case JSONType::INT64: {
            int64_t l = std::stoll(value);
            return DomNode(bsoncxx::types::b_int64{l});
        }
        case JSONType::DOUBLE: {
            double d = std::stod(value);
            return DomNode(bsoncxx::types::b_double{d});
        }
        case JSONType::STRING: {
            return DomNode(bsoncxx::types::b_string{value});
        }
        case JSONType::ARRAY: {
            // BuildBsonArrayFromJsonString classifies its own failures; a
            // generic catch here would rewrap the SegcoreError and lose the code.
            auto arr_value = BuildBsonArrayFromJsonString(value);
            return DomNode(bsoncxx::types::b_array{arr_value.view()});
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
BsonBuilder::ConvertDomToBson(const DomNode& node,
                              bsoncxx::builder::basic::document& builder) {
    for (const auto& [key, child] : node.document_children) {
        switch (child.type) {
            case DomNode::Type::VALUE: {
                builder.append(bsoncxx::builder::basic::kvp(
                    key, child.bson_value.value()));
                break;
            }
            case DomNode::Type::DOCUMENT: {
                bsoncxx::builder::basic::document sub_doc;
                ConvertDomToBson(child, sub_doc);
                builder.append(bsoncxx::builder::basic::kvp(key, sub_doc));
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
    memcpy(&length, current_base_ptr, 4);

    const uint8_t* end_ptr = current_base_ptr + length - 1;
    AssertInfo(*(end_ptr) == 0x00, "miss bson document terminator");

    const uint8_t* ptr = current_base_ptr + 4;

    while (ptr <= end_ptr && *ptr != 0x00) {
        // record key offset
        size_t key_offset = ptr - root_base_ptr;

        // read key type
        auto type = static_cast<bsoncxx::type>(*ptr++);

        // read key
        auto key_name = reinterpret_cast<const char*>(ptr);
        ptr += strlen(key_name) + 1;

        // construct key path
        std::string key_path = AppendJsonPointer(current_path, key_name);

        // do not record key offset pair for null value
        // because null value is not a valid key
        if (type != bsoncxx::type::k_null) {
            result.emplace_back(key_path, key_offset);
        }

        // handle value
        switch (type) {
            case bsoncxx::type::k_document: {
                ExtractOffsetsRecursive(root_base_ptr, ptr, key_path, result);
                // skip sub doc
                uint32_t child_len;
                memcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case bsoncxx::type::k_array: {
                // not parse array
                // skip sub doc
                uint32_t child_len;
                memcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case bsoncxx::type::k_string: {
                uint32_t str_len;
                memcpy(&str_len, ptr, 4);
                ptr += 4 + str_len;
                break;
            }
            case bsoncxx::type::k_int32: {
                ptr += 4;
                break;
            }
            case bsoncxx::type::k_int64: {
                ptr += 8;
                break;
            }
            case bsoncxx::type::k_double: {
                ptr += 8;
                break;
            }
            case bsoncxx::type::k_bool: {
                ptr += 1;
                break;
            }
            case bsoncxx::type::k_null: {
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
