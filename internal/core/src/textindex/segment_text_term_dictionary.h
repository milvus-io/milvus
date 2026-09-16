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
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace milvus::textindex {

class TextFst;

struct TextTermMatch {
    std::string term;
    std::uint32_t edit_distance = 0;
};

struct TextTermTrieStats {
    std::size_t term_count = 0;
    std::size_t memory_bytes = 0;
};

// SegmentTextTermDictionary owns the mutable term vocabulary of one segcore
// segment. A growing segment imports its recovery FSTs into this dictionary;
// immutable FST readers are supplied only for sealed-segment queries because
// their file/cache lifecycle remains coupled to QueryNode segment loading.
class SegmentTextTermDictionary {
 public:
    SegmentTextTermDictionary();
    ~SegmentTextTermDictionary();

    SegmentTextTermDictionary(const SegmentTextTermDictionary&) = delete;
    SegmentTextTermDictionary&
    operator=(const SegmentTextTermDictionary&) = delete;
    SegmentTextTermDictionary(SegmentTextTermDictionary&&) = delete;
    SegmentTextTermDictionary&
    operator=(SegmentTextTermDictionary&&) = delete;

    void
    AddTerms(std::int64_t field_id, const std::vector<std::string>& terms);

    // Adds every term from the immutable recovery artifacts to the mutable
    // dictionary without materializing an intermediate term vector.
    void
    AddFstTerms(std::int64_t field_id,
                std::span<const TextFst* const> immutable_fsts);

    [[nodiscard]] std::vector<TextTermMatch>
    FuzzySearch(std::int64_t field_id,
                std::span<const TextFst* const> immutable_fsts,
                std::string_view query,
                std::uint32_t max_edit_distance,
                std::size_t max_expansions) const;

    [[nodiscard]] TextTermTrieStats
    TrieStats() const;

 private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::textindex
