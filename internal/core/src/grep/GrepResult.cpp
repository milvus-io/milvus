// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "grep/GrepResult.h"

#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "grep/Grep.h"

namespace milvus::grep {

std::string
GrepDetailsView::Serialize(const CheckGrepJsonSize& check_size) const {
    std::string document = "{\"items\":[";
    constexpr uint64_t suffix_size = 2;  // ]}
    check_size(document.size() + suffix_size);
    for (size_t j = 0; j < result.LineCount(text_index); ++j) {
        std::vector<int64_t> starts, ends;
        const auto count = result.SpanCount(text_index, j);
        starts.reserve(count);
        ends.reserve(count);
        for (size_t k = 0; k < count; ++k) {
            auto hit = result.GetSpan(text_index, j, k);
            AssertInfo(hit.start <= static_cast<uint64_t>(
                                        std::numeric_limits<int64_t>::max()) &&
                           hit.end <= static_cast<uint64_t>(
                                          std::numeric_limits<int64_t>::max()),
                       "grep span boundary exceeds Int64");
            starts.push_back(static_cast<int64_t>(hit.start));
            ends.push_back(static_cast<int64_t>(hit.end));
        }
        const auto item = nlohmann::json{
            {"type", result.IsMatch(text_index, j) ? "match" : "context"},
            {"text", std::string(result.Content(text_index, j))},
            {"line_number", result.LineNumber(text_index, j)},
            {"absolute_offset", result.LineStartOffset(text_index, j)},
            {"starts", std::move(starts)},
            {"ends",
             std::move(ends)}}.dump();
        check_size(document.size() + (j != 0) + item.size() + suffix_size);
        if (j != 0) {
            document += ",";
        }
        document += item;
    }
    document += "]}";
    return document;
}

nlohmann::json
GrepCount::ToJson() const {
    return {{"count", count}, {"complete", complete}};
}

nlohmann::json
GrepExists::ToJson() const {
    return nlohmann::json::object();
}

std::string
GrepResult::Serialize(const CheckGrepJsonSize& check_size) const {
    return std::visit(
        [&](const auto& value) -> std::string {
            if constexpr (std::is_same_v<std::decay_t<decltype(value)>,
                                         GrepDetailsView>) {
                return value.Serialize(check_size);
            } else {
                auto encoded = value.ToJson().dump();
                check_size(encoded.size());
                return encoded;
            }
        },
        payload);
}

}  // namespace milvus::grep
