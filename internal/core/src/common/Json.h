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

#include <cstddef>
#include <optional>
#include <string_view>

#include "exceptions/EasyAssert.h"
#include "simdjson.h"
#include "fmt/core.h"

namespace milvus {

class Json {
 public:
    Json() = default;

    explicit Json(simdjson::padded_string data) : own_data_(std::move(data)) {
        data_ = own_data_.value();
    }

    explicit Json(simdjson::padded_string_view data) : data_(data) {
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
    Json(const char* data, size_t len) : data_(data, len) {
    }

    Json(Json&& json) = default;

    Json&
    operator=(const Json& json) {
        if (json.own_data_.has_value()) {
            own_data_ = simdjson::padded_string(
                json.own_data_.value().data(), json.own_data_.value().length());
        }

        data_ = json.data_;
        return *this;
    }

    operator std::string_view() const {
        return data_;
    }

    void
    parse(simdjson::padded_string_view data) {
        data_ = data;
    }

    auto
    doc() const {
        thread_local simdjson::ondemand::parser parser;

        // it's always safe to add the padding,
        // as we have allocated the memory with this padding
        auto doc =
            parser.iterate(data_, data_.size() + simdjson::SIMDJSON_PADDING);
        return doc.get_object();
    }

    auto
    operator[](const std::string_view field) const {
        return doc()[field];
    }

    auto
    at_pointer(const std::string_view pointer) const {
        return doc().at_pointer(pointer);
    }

    std::string_view
    data() const {
        return data_;
    }

 private:
    std::optional<simdjson::padded_string>
        own_data_;  // this could be empty, then the Json will be just s view on bytes
    simdjson::padded_string_view data_;
};
}  // namespace milvus
