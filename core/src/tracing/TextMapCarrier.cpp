// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "tracing/TextMapCarrier.h"

namespace milvus {
namespace tracing {

TextMapCarrier::TextMapCarrier(std::unordered_map<std::string, std::string>& text_map) : text_map_(text_map) {
}

opentracing::expected<void>
TextMapCarrier::Set(opentracing::string_view key, opentracing::string_view value) const {
    //    text_map_[key] = value;
    //    return {};
    opentracing::expected<void> result;

    auto was_successful = text_map_.emplace(key, value);
    if (was_successful.second) {
        // Use a default constructed opentracing::expected<void> to indicate
        // success.
        return result;
    } else {
        // `key` clashes with existing data, so the span context can't be encoded
        // successfully; set opentracing::expected<void> to an std::error_code.
        return opentracing::make_unexpected(std::make_error_code(std::errc::not_supported));
    }
}

opentracing::expected<void>
TextMapCarrier::ForeachKey(F f) const {
    // Iterate through all key-value pairs, the tracer will use the relevant keys
    // to extract a span context.
    for (auto& key_value : text_map_) {
        auto was_successful = f(key_value.first, key_value.second);
        if (!was_successful) {
            // If the callback returns and unexpected value, bail out of the loop.
            return was_successful;
        }
    }

    // Indicate successful iteration.
    return {};
}

// Optional, define TextMapReader::LookupKey to allow for faster extraction.
opentracing::expected<opentracing::string_view>
TextMapCarrier::LookupKey(opentracing::string_view key) const {
    auto iter = text_map_.find(key);
    if (iter != text_map_.end()) {
        return opentracing::make_unexpected(opentracing::key_not_found_error);
    }
    return opentracing::string_view{iter->second};
}

}  // namespace tracing
}  // namespace milvus
