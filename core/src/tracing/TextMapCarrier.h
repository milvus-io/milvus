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

#pragma once

#include <opentracing/propagation.h>

#include <string>
#include <unordered_map>

namespace milvus {
namespace tracing {

class TextMapCarrier : public opentracing::TextMapReader, public opentracing::TextMapWriter {
 public:
    explicit TextMapCarrier(std::unordered_map<std::string, std::string>& text_map);

    opentracing::expected<void>
    Set(opentracing::string_view key, opentracing::string_view value) const override;

    using F = std::function<opentracing::expected<void>(opentracing::string_view, opentracing::string_view)>;

    opentracing::expected<void>
    ForeachKey(F f) const override;

    // Optional, define TextMapReader::LookupKey to allow for faster extraction.
    opentracing::expected<opentracing::string_view>
    LookupKey(opentracing::string_view key) const override;

 private:
    std::unordered_map<std::string, std::string>& text_map_;
};

}  // namespace tracing
}  // namespace milvus
