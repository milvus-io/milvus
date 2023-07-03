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

#include "exceptions/EasyAssert.h"
#include "simdjson.h"
#include "fmt/core.h"
#include "simdjson/common_defs.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/document.h"
#include "simdjson/dom/element.h"
#include "simdjson/error.h"
#include "simdjson/padded_string.h"

namespace milvus {
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
        AssertInfo(len + simdjson::SIMDJSON_PADDING <= cap,
                   fmt::format("create json without enough memory size for "
                               "SIMD, len={}, cap={}",
                               len,
                               cap));
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
        thread_local simdjson::ondemand::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc =
            parser.iterate(data_, data_.size() + simdjson::SIMDJSON_PADDING);
        AssertInfo(doc.error() == simdjson::SUCCESS,
                   fmt::format("failed to parse the json {}: {}",
                               data_,
                               simdjson::error_message(doc.error())));
        return doc;
    }

    value_result<simdjson::dom::element>
    dom_doc() const {
        thread_local simdjson::dom::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc = parser.parse(data_);
        AssertInfo(doc.error() == simdjson::SUCCESS,
                   fmt::format("failed to parse the json {}: {}",
                               data_,
                               simdjson::error_message(doc.error())));
        return doc;
    }

    bool
    exist(std::string_view pointer) const {
        return doc().at_pointer(pointer).error() == simdjson::SUCCESS;
    }

    // construct JSON pointer with provided path
    static std::string
    pointer(std::vector<std::string> nested_path) {
        std::for_each(
            nested_path.begin(), nested_path.end(), [](std::string& key) {
                boost::replace_all(key, "~", "~0");
                boost::replace_all(key, "/", "~1");
            });
        auto pointer = "/" + boost::algorithm::join(nested_path, "/");
        return pointer;
    }

    template <typename T>
    value_result<T>
    at(std::string_view pointer) const {
        return doc().at_pointer(pointer).get<T>();
    }

    // get dom array by JSON pointer,
    // call `size()` to get array size,
    // call `at()` to get array element by index,
    // iterate through array elements by iterator.
    value_result<simdjson::dom::array>
    array_at(std::string_view pointer) const {
        return dom_doc().at_pointer(pointer).get_array();
    }

    std::string_view
    data() const {
        return data_;
    }

 private:
    std::optional<simdjson::padded_string>
        own_data_{};  // this could be empty, then the Json will be just s view on bytes
    simdjson::padded_string_view data_{};
};
}  // namespace milvus
