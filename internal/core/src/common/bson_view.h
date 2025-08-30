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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <string>
#include <vector>
#include <optional>

#include "fmt/format.h"
#include "log/Log.h"
#include "common/EasyAssert.h"

template <>
struct fmt::formatter<bsoncxx::type> : fmt::formatter<std::string> {
    auto
    format(bsoncxx::type type, fmt::format_context& ctx) const {
        std::string name;
        switch (type) {
            case bsoncxx::type::k_int32:
                name = "int32";
                break;
            case bsoncxx::type::k_int64:
                name = "int64";
                break;
            case bsoncxx::type::k_double:
                name = "double";
                break;
            case bsoncxx::type::k_string:
                name = "string";
                break;
            case bsoncxx::type::k_bool:
                name = "bool";
                break;
            case bsoncxx::type::k_null:
                name = "null";
                break;
            case bsoncxx::type::k_document:
                name = "document";
                break;
            case bsoncxx::type::k_array:
                name = "array";
                break;
            default:
                name = "Unknown";
        }
        return fmt::formatter<std::string>::format(name, ctx);
    }
};

namespace milvus {

struct BsonRawField {
    bsoncxx::type type;
    std::string key;
    const uint8_t* value_ptr;  // points to value (not including type/key)
};

inline int32_t
ReadInt32(const uint8_t* ptr) {
    return *reinterpret_cast<const int32_t*>(ptr);
}

inline int64_t
ReadInt64(const uint8_t* ptr) {
    return *reinterpret_cast<const int64_t*>(ptr);
}

inline double
ReadDouble(const uint8_t* ptr) {
    return *reinterpret_cast<const double*>(ptr);
}

inline std::string
ReadUtf8(const uint8_t* ptr) {
    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
    return std::string(reinterpret_cast<const char*>(ptr + 4),
                       len - 1);  // exclude trailing '\0'
}

inline std::string_view
ReadUtf8View(const uint8_t* ptr) {
    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
    return std::string_view(reinterpret_cast<const char*>(ptr + 4),
                            len - 1);  // exclude trailing '\0'
}

inline bool
ReadBool(const uint8_t* ptr) {
    return *ptr != 0;
}

inline std::vector<uint8_t>
ReadRawDocOrArray(const uint8_t* ptr) {
    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
    return std::vector<uint8_t>(ptr, ptr + len);
}

inline bsoncxx::document::view
ParseAsDocument(const uint8_t* ptr) {
    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
    return bsoncxx::document::view(ptr, len);
}

inline bsoncxx::array::view
ParseAsArray(const uint8_t* ptr) {
    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
    return bsoncxx::array::view(ptr, len);
}

template <typename T>
T
GetValue(const uint8_t* ptr);

template <>
inline int32_t
GetValue<int32_t>(const uint8_t* ptr) {
    return ReadInt32(ptr);
}

template <>
inline int64_t
GetValue<int64_t>(const uint8_t* ptr) {
    return ReadInt64(ptr);
}

template <>
inline double
GetValue<double>(const uint8_t* ptr) {
    return ReadDouble(ptr);
}

template <>
inline bool
GetValue<bool>(const uint8_t* ptr) {
    return ReadBool(ptr);
}

template <>
inline std::string
GetValue<std::string>(const uint8_t* ptr) {
    return ReadUtf8(ptr);
}

template <>
inline std::string_view
GetValue<std::string_view>(const uint8_t* ptr) {
    return ReadUtf8View(ptr);
}

template <>
inline std::vector<uint8_t>
GetValue<std::vector<uint8_t>>(const uint8_t* ptr) {
    return ReadRawDocOrArray(ptr);
}

inline std::string
BsonHexDebugString(const uint8_t* data, size_t size) {
    std::ostringstream oss;
    oss << "BSON hex dump (" << size << " bytes): ";

    for (size_t i = 0; i < size; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(data[i]) << " ";
    }
    return oss.str();
}

// may lose precision for big int64_t more than 2^53
inline bool
CanConvertToInt64(double x) {
    return std::trunc(x) == x && x >= std::numeric_limits<int64_t>::min() &&
           x <= std::numeric_limits<int64_t>::max();
}

class BsonView {
 public:
    explicit BsonView(const std::vector<uint8_t>& data)
        : data_(data.data()), size_(data.size()) {
    }

    explicit BsonView(const uint8_t* data, size_t size)
        : data_(data), size_(size) {
    }

    ~BsonView() {
    }

    std::string
    ToString() const {
        bsoncxx::document::view view(data_, size_);
        return bsoncxx::to_json(view);
    }

    bool
    IsNullAtOffset(size_t offset) {
        const uint8_t* ptr = data_ + offset;
        AssertInfo(offset < size_, "bson offset out of range");

        auto type_tag = static_cast<bsoncxx::type>(*ptr++);
        return type_tag == bsoncxx::type::k_null;
    }

    template <typename T>
    std::optional<T>
    ParseAsValueAtOffset(size_t offset) {
        LOG_TRACE("bson hex: {}",
                  BsonHexDebugString(data_ + offset, size_ - offset));
        const uint8_t* ptr = data_ + offset;

        // check offset
        AssertInfo(
            offset < size_, "bson offset:{} out of range:{}", offset, size_);

        // parse type tag
        auto type_tag = static_cast<bsoncxx::type>(*ptr++);

        // parse key
        const char* key_cstr = reinterpret_cast<const char*>(ptr);
        size_t key_len = strlen(key_cstr);
        ptr += key_len + 1;  // +1 for null terminator

        // parse value
        switch (type_tag) {
            case bsoncxx::type::k_int32:
                if constexpr (std::is_same_v<T, int32_t>) {
                    return GetValue<int32_t>(ptr);
                }
                if constexpr (std::is_same_v<T, int64_t>) {
                    return static_cast<int64_t>(GetValue<int32_t>(ptr));
                }
                break;
            case bsoncxx::type::k_int64:
                if constexpr (std::is_same_v<T, int64_t>) {
                    return GetValue<int64_t>(ptr);
                } else if constexpr (std::is_same_v<T, double>) {
                    return static_cast<double>(GetValue<int64_t>(ptr));
                }
                break;
            case bsoncxx::type::k_double:
                if constexpr (std::is_same_v<T, double>) {
                    return GetValue<double>(ptr);
                }
                // if constexpr (std::is_same_v<T, int64_t>) {
                //     if (CanConvertToInt64(GetValue<double>(ptr))) {
                //         return static_cast<int64_t>(GetValue<double>(ptr));
                //     }
                // }
                break;
            case bsoncxx::type::k_bool:
                if constexpr (std::is_same_v<T, bool>) {
                    return GetValue<bool>(ptr);
                }
                break;
            case bsoncxx::type::k_string:
                if (ptr + 4 > data_ + size_) {
                    return std::nullopt;
                }
                if constexpr (std::is_same_v<T, std::string>) {
                    return GetValue<std::string>(ptr);
                }
                if constexpr (std::is_same_v<T, std::string_view>) {
                    return GetValue<std::string_view>(ptr);
                }
                break;
            case bsoncxx::type::k_null:
            case bsoncxx::type::k_document:
            case bsoncxx::type::k_array:
                break;
            default:
                ThrowInfo(
                    ErrorCode::Unsupported, "unknown BSON type {}", type_tag);
        }
        return std::nullopt;
    }

    std::optional<bsoncxx::array::view>
    ParseAsArrayAtOffset(size_t offset) {
        if (offset == 0) {
            // if offset is 0, it means the array is the whole bson_view
            return bsoncxx::array::view(data_, size_);
        }

        // check offset
        AssertInfo(offset < size_, "bson offset out of range");
        const uint8_t* ptr = data_ + offset;

        // check type
        AssertInfo(static_cast<bsoncxx::type>(*ptr) == bsoncxx::type::k_array,
                   "ParseAsArrayAtOffset expects an array at offset {}",
                   offset);
        ptr++;

        // skip key
        const char* key_start = reinterpret_cast<const char*>(ptr);
        size_t key_len = strlen(key_start);
        ptr += key_len + 1;  // +1 for null terminator
        const uint8_t* view_start = ptr;
        // parse length
        int32_t len = *reinterpret_cast<const int32_t*>(ptr);
        if (ptr + len > data_ + size_) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "ParseAsArrayAtOffset out of range");
        }
        return bsoncxx::array::view(view_start, len);
    }

    inline std::optional<bsoncxx::types::bson_value::view>
    FindByPath(const bsoncxx::document::view& doc_view,
               const std::vector<std::string>& path,
               size_t idx = 0) {
        if (idx >= path.size())
            return std::nullopt;

        for (const auto& elem : doc_view) {
            const std::string_view key{elem.key().data(), elem.key().length()};
            if (key != path[idx])
                continue;

            const auto& value = elem.get_value();
            if (idx == path.size() - 1)
                return value;  // found the target

            // Recursively process nested structures
            switch (value.type()) {
                case bsoncxx::type::k_document: {
                    auto sub_doc = value.get_document();
                    return FindByPath(sub_doc.view(), path, idx + 1);
                }
                case bsoncxx::type::k_array:
                    // TODO: may support array index from parent json for now
                    break;
                default:
                    break;
            }
        }
        return std::nullopt;
    }

    // extract pointer to the N-th value in a BSON array
    template <typename T>
    static std::optional<T>
    GetNthElementInArray(const uint8_t* array_ptr, size_t index) {
        const uint8_t* ptr = array_ptr + 4;
        for (size_t i = 0; i <= index; ++i) {
            auto type_tag = *ptr++;
            if (type_tag == 0x00)
                break;

            while (*ptr != '\0') ++ptr;
            ++ptr;

            if (i == index) {
                switch (static_cast<bsoncxx::type>(type_tag)) {
                    case bsoncxx::type::k_int32:
                        if constexpr (std::is_same_v<T, int32_t>) {
                            return ReadInt32(ptr);
                        }
                        break;
                    case bsoncxx::type::k_int64:
                        if constexpr (std::is_same_v<T, int64_t>) {
                            return ReadInt64(ptr);
                        }
                        break;
                    case bsoncxx::type::k_double:
                        if constexpr (std::is_same_v<T, double>) {
                            return ReadDouble(ptr);
                        }
                        break;
                    case bsoncxx::type::k_bool:
                        if constexpr (std::is_same_v<T, bool>) {
                            return ReadBool(ptr);
                        }
                        break;
                    case bsoncxx::type::k_string:
                        if constexpr (std::is_same_v<T, std::string>) {
                            return ReadUtf8(ptr);
                        }
                        if constexpr (std::is_same_v<T, std::string_view>) {
                            return ReadUtf8View(ptr);
                        }
                        break;
                    case bsoncxx::type::k_array:
                        if constexpr (std::is_same_v<T, bsoncxx::array::view>) {
                            return ParseAsArray(ptr);
                        }
                        break;
                    default:
                        return std::nullopt;
                }
            }

            switch (static_cast<bsoncxx::type>(type_tag)) {
                case bsoncxx::type::k_string: {
                    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
                    ptr += 4 + len;
                    break;
                }
                case bsoncxx::type::k_int32:
                    ptr += 4;
                    break;
                case bsoncxx::type::k_int64:
                    ptr += 8;
                    break;
                case bsoncxx::type::k_double:
                    ptr += 8;
                    break;
                case bsoncxx::type::k_bool:
                    ptr += 1;
                    break;
                case bsoncxx::type::k_array:
                case bsoncxx::type::k_document: {
                    int32_t len = *reinterpret_cast<const int32_t*>(ptr);
                    ptr += len;
                    break;
                }
                default:
                    return std::nullopt;
            }
        }

        return std::nullopt;
    }

    inline BsonRawField
    ParseBsonField(const uint8_t* bson_data, size_t offset) {
        const uint8_t* ptr = bson_data + offset;
        auto type_tag = static_cast<bsoncxx::type>(*ptr++);

        const char* key_cstr = reinterpret_cast<const char*>(ptr);
        size_t key_len = strlen(key_cstr);
        ptr += key_len + 1;

        return BsonRawField{
            .type = type_tag, .key = key_cstr, .value_ptr = ptr};
    }

    template <typename T>
    static std::optional<T>
    GetValueFromElement(const bsoncxx::document::element& element) {
        if constexpr (std::is_same_v<T, int32_t>) {
            if (element.type() == bsoncxx::type::k_int32) {
                return element.get_int32().value;
            }
        } else if constexpr (std::is_same_v<T, int64_t>) {
            if (element.type() == bsoncxx::type::k_int64) {
                return element.get_int64().value;
            }
        } else if constexpr (std::is_same_v<T, double>) {
            if (element.type() == bsoncxx::type::k_double) {
                return element.get_double().value;
            }
        } else if constexpr (std::is_same_v<T, bool>) {
            if (element.type() == bsoncxx::type::k_bool) {
                return element.get_bool().value;
            }
        } else if constexpr (std::is_same_v<T, std::string>) {
            if (element.type() == bsoncxx::type::k_string) {
                return std::string(element.get_string().value.data(),
                                   element.get_string().value.size());
            }
        } else if constexpr (std::is_same_v<T, std::string_view>) {
            if (element.type() == bsoncxx::type::k_string) {
                return std::string_view(element.get_string().value.data(),
                                        element.get_string().value.size());
            }
        }
        return std::nullopt;
    }

    template <typename T>
    static std::optional<T>
    GetValueFromBsonView(const bsoncxx::types::bson_value::view& value_view) {
        switch (value_view.type()) {
            case bsoncxx::type::k_int32:
                if constexpr (std::is_same_v<T, int32_t>)
                    return value_view.get_int32().value;
                break;
            case bsoncxx::type::k_int64:
                if constexpr (std::is_same_v<T, int64_t>)
                    return value_view.get_int64().value;
                if constexpr (std::is_same_v<T, double>)
                    return static_cast<double>(value_view.get_int64().value);
                break;
            case bsoncxx::type::k_double:
                if constexpr (std::is_same_v<T, double>)
                    return value_view.get_double().value;
                break;
            case bsoncxx::type::k_bool:
                if constexpr (std::is_same_v<T, bool>)
                    return value_view.get_bool().value;
                break;
            case bsoncxx::type::k_string:
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::string(value_view.get_string().value.data(),
                                       value_view.get_string().value.size());
                } else if constexpr (std::is_same_v<T, std::string_view>) {
                    return std::string_view(
                        value_view.get_string().value.data(),
                        value_view.get_string().value.size());
                }
                break;
            case bsoncxx::type::k_array:
                if constexpr (std::is_same_v<T, bsoncxx::array::view>) {
                    return value_view.get_array().value;
                }
                break;
            // other types...
            default:
                break;
        }
        return std::nullopt;
    }
    const uint8_t* data_;
    size_t size_;
};

}  // namespace milvus