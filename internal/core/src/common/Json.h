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

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "simdjson.h"
#include "fmt/core.h"
#include "simdjson/common_defs.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/document.h"
#include "simdjson/dom/element.h"
#include "simdjson/error.h"
#include "simdjson/padded_string.h"

#define SIMDJSON_CHECK_ERROR(result)               \
    do {                                           \
        if ((result).error() != simdjson::SUCCESS) \
            return (result).error();               \
    } while (0)
namespace milvus {

bool
isObjectEmpty(simdjson::ondemand::value value);
bool
isDocEmpty(simdjson::ondemand::document document);

// Extract specific top-level keys from a JSON string using simdjson ondemand.
// Uses raw_json() to copy value fragments directly from the source without
// re-parsing or re-serializing, which is faster than rapidjson or nlohmann.
// Note: output key order follows source JSON field order (not keys param order).
// This is safe because JSON objects are unordered per RFC 8259, and downstream
// consumers (Go protobuf → map) do not depend on key ordering.
inline std::string
ExtractSubJson(const std::string& json, const std::vector<std::string>& keys) {
    simdjson::padded_string padded(json);
    thread_local simdjson::ondemand::parser parser;
    auto doc = parser.iterate(padded);
    if (doc.error()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "json parse failed: {}",
                  simdjson::error_message(doc.error()));
    }

    auto obj = doc.get_object();
    if (obj.error()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "ExtractSubJson: input is not a JSON object: {}",
                  simdjson::error_message(obj.error()));
    }

    std::string result = "{";
    result.reserve(json.size());
    bool first = true;
    for (auto field : obj) {
        // escaped_key() returns the key as-is from source (safe for JSON output)
        std::string_view ek = field.escaped_key();
        // unescaped_key() resolves escape sequences for correct comparison
        auto uk = field.unescaped_key();
        if (uk.error()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "ExtractSubJson: failed to decode key: {}",
                      simdjson::error_message(uk.error()));
        }
        if (std::find(keys.begin(), keys.end(), uk.value()) != keys.end()) {
            // raw_json() extracts the original JSON text of the value,
            // avoiding any re-serialization overhead
            auto raw = field.value().raw_json();
            if (raw.error()) {
                ThrowInfo(ErrorCode::UnexpectedError,
                          "ExtractSubJson: failed to extract value for "
                          "key '{}': {}",
                          uk.value(),
                          simdjson::error_message(raw.error()));
            }
            if (!first) {
                result += ',';
            }
            result += '"';
            result += ek;
            result += "\":";
            result += raw.value();
            first = false;
        }
    }
    result += '}';
    return result;
}

using document = simdjson::ondemand::document;
template <typename T>
using value_result = simdjson::simdjson_result<T>;
class Json {
 public:
    Json() = default;

    explicit Json(simdjson::padded_string data) : own_data_(std::move(data)) {
        data_ = own_data_.value();
    }

    Json(const char* data, size_t len, size_t cap) : data_(data, len) {
        AssertInfo(
            len + simdjson::SIMDJSON_PADDING <= cap,
            "create json without enough memory size for SIMD, len={}, cap={}",
            len,
            cap);
    }

    // WARN: this is used for fast non-copy construction,
    // MUST make sure there at least SIMDJSON_PADDING bytes allocated
    // after the string_view
    explicit Json(const std::string_view& data)
        : Json(data.data(), data.size()) {
    }

    // WARN: this is used for fast non-copy construction,
    // MUST make sure that the data points to a memory that
    // with size at least len + SIMDJSON_PADDING
    Json(const char* data, size_t len)
        : data_(data, len, len + simdjson::SIMDJSON_PADDING) {
    }

    Json(const Json& json) {
        if (json.own_data_.has_value()) {
            own_data_ = simdjson::padded_string(
                json.own_data_.value().data(), json.own_data_.value().length());
            data_ = own_data_.value();
        } else {
            data_ = json.data_;
        }
    };
    Json(Json&& json) noexcept {
        if (json.own_data_.has_value()) {
            own_data_ = std::move(json.own_data_);
            data_ = own_data_.value();
        } else {
            data_ = json.data_;
        }
    }

    Json&
    operator=(const Json& json) {
        if (json.own_data_.has_value()) {
            own_data_ = simdjson::padded_string(
                json.own_data_.value().data(), json.own_data_.value().length());
            data_ = own_data_.value();
        } else {
            data_ = json.data_;
        }
        return *this;
    }

    operator std::string_view() const {
        return data_;
    }

    value_result<document>
    doc() const {
        if (data_.size() == 0) {
            return {};
        }
        thread_local simdjson::ondemand::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc =
            parser.iterate(data_, data_.size() + simdjson::SIMDJSON_PADDING);
        AssertInfo(doc.error() == simdjson::SUCCESS,
                   "failed to parse the json {}: {}",
                   data_,
                   simdjson::error_message(doc.error()));
        return doc;
    }

    value_result<document>
    doc(uint16_t offset, uint16_t length) const {
        thread_local simdjson::ondemand::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc = parser.iterate(
            data_.data() + offset, length, length + simdjson::SIMDJSON_PADDING);
        AssertInfo(doc.error() == simdjson::SUCCESS,
                   "failed to parse the json {} offset {}, length {}: {}, "
                   "total_json:{}",
                   std::string(data_.data() + offset, length),
                   offset,
                   length,
                   simdjson::error_message(doc.error()),
                   data_);
        return doc;
    }

    value_result<simdjson::dom::element>
    dom_doc() const {
        if (data_.size() == 0) {
            return {};
        }
        thread_local simdjson::dom::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc = parser.parse(data_);
        AssertInfo(doc.error() == simdjson::SUCCESS,
                   "failed to parse the json {}: {}",
                   data_,
                   simdjson::error_message(doc.error()));
        return doc;
    }

    value_result<simdjson::dom::element>
    dom_doc(uint16_t offset, uint16_t length) const {
        thread_local simdjson::dom::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc = parser.parse(data_.data() + offset, length);
        AssertInfo(doc.error() == simdjson::SUCCESS,
                   "failed to parse the json {}: {}",
                   std::string(data_.data() + offset, length),
                   simdjson::error_message(doc.error()));
        return doc;
    }

    bool
    exist(std::string_view pointer) const {
        auto doc = this->doc();
        if (pointer.empty()) {
            return doc.error() == simdjson::SUCCESS &&
                   !isDocEmpty(std::move(doc));
        } else {
            auto res = doc.at_pointer(pointer);
            return res.error() == simdjson::SUCCESS &&
                   !isObjectEmpty(res.value());
        }
    }

    // construct JSON pointer with provided path
    static std::string
    pointer(std::vector<std::string> nested_path) {
        if (nested_path.empty()) {
            return "";
        }
        std::for_each(
            nested_path.begin(), nested_path.end(), [](std::string& key) {
                boost::replace_all(key, "~", "~0");
                boost::replace_all(key, "/", "~1");
            });
        auto pointer = "/" + boost::algorithm::join(nested_path, "/");
        return pointer;
    }

    auto
    type(const std::string& pointer) const {
        return pointer.empty() ? doc().type()
                               : doc().at_pointer(pointer).type();
    }

    auto
    get_number_type(const std::string& pointer) const {
        return pointer.empty() ? doc().get_number_type()
                               : doc().at_pointer(pointer).get_number_type();
    }

    template <typename T>
    value_result<T>
    at(std::string_view pointer) const {
        if (pointer == "") {
            if constexpr (std::is_same_v<std::string_view, T> ||
                          std::is_same_v<std::string, T>) {
                return doc().get_string(false);
            } else if constexpr (std::is_same_v<bool, T>) {
                return doc().get_bool();
            } else if constexpr (std::is_same_v<int64_t, T>) {
                return doc().get_int64();
            } else if constexpr (std::is_same_v<double, T>) {
                return doc().get_double();
            }
        }

        return doc().at_pointer(pointer).get<T>();
    }

    value_result<std::string>
    at_string_any(std::string_view pointer) const {
        if (data_.empty()) {
            return std::string{};
        }

        auto doc_res = doc();
        SIMDJSON_CHECK_ERROR(doc_res);

        auto el_res = doc_res.value().at_pointer(pointer);
        SIMDJSON_CHECK_ERROR(el_res);

        auto json_str = simdjson::to_json_string(el_res.value());
        SIMDJSON_CHECK_ERROR(json_str);

        return std::string{json_str.value()};
    }

    template <typename T>
    value_result<T>
    at(uint16_t offset, uint16_t length) const {
        return doc(offset, length).get<T>();
    }

    std::string_view
    at_string(uint16_t offset, uint16_t length) const {
        return std::string_view(data_.data() + offset, length);
    }

    value_result<simdjson::dom::array>
    array_at(uint16_t offset, uint16_t length) const {
        return dom_doc(offset, length).get_array();
    }

    // get dom array by JSON pointer,
    // call `size()` to get array size,
    // call `at()` to get array element by index,
    // iterate through array elements by iterator.
    value_result<simdjson::dom::array>
    array_at(std::string_view pointer) const {
        return dom_doc().at_pointer(pointer).get_array();
    }

    size_t
    size() const {
        return data_.size();
    }

    std::string_view
    data() const {
        return data_;
    }

    const char*
    c_str() const {
        return data_.data();
    }

 private:
    std::optional<simdjson::padded_string>
        own_data_{};  // this could be empty, then the Json will be just s view on bytes
    simdjson::padded_string_view data_{};
};

inline bool
isObjectEmpty(simdjson::ondemand::value value) {
    if (value.is_null()) {
        return true;
    }

    if (value.type().value() == simdjson::ondemand::json_type::object) {
        auto object = value.get_object();
        for (auto field : object) {
            if (!isObjectEmpty(field.value())) {
                return false;
            }
        }
        return true;
    }

    if (value.type().value() == simdjson::ondemand::json_type::array) {
        auto array = value.get_array();
        for (auto element : array) {
            if (!isObjectEmpty(std::move(element))) {
                return false;
            }
        }
        return true;
    }

    return false;
}

inline bool
isDocEmpty(simdjson::ondemand::document document) {
    if (document.is_null()) {
        return true;
    }

    if (document.type().value() == simdjson::ondemand::json_type::object) {
        auto object = document.get_object();
        for (auto field : object) {
            if (!isObjectEmpty(field.value())) {
                return false;
            }
        }
        return true;
    }

    if (document.type().value() == simdjson::ondemand::json_type::array) {
        auto array = document.get_array();
        for (auto element : array) {
            if (!isObjectEmpty(std::move(element))) {
                return false;
            }
        }
        return true;
    }
    return false;
}
}  // namespace milvus
