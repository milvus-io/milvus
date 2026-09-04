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

#include "textindex/segment_text_term_dictionary.h"

#include <gtest/gtest.h>

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "textindex/fst/text_fst.h"

namespace milvus::textindex {
namespace {

void
BuildFst(TextFst& fst, const std::vector<std::string>& terms) {
    std::size_t index = 0;
    fst.Build([&]() -> std::optional<std::string_view> {
        return index == terms.size()
                   ? std::nullopt
                   : std::optional<std::string_view>(terms[index++]);
    });
}

TEST(SegmentTextTermDictionaryTest, MutableTrieDeduplicatesAndTracksMemory) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {});
    EXPECT_EQ(dictionary.TrieStats().term_count, 0);
    EXPECT_EQ(dictionary.TrieStats().memory_bytes, 0);

    dictionary.AddTerms(101, {"fuzzy", "milvus", "fuzzy"});
    const auto first = dictionary.TrieStats();
    EXPECT_EQ(first.term_count, 2);
    EXPECT_GT(first.memory_bytes, 0);

    dictionary.AddTerms(101, {"milvus"});
    const auto duplicate = dictionary.TrieStats();
    EXPECT_EQ(duplicate.term_count, first.term_count);
    EXPECT_EQ(duplicate.memory_bytes, first.memory_bytes);

    dictionary.AddTerms(102, {"search"});
    const auto second_field = dictionary.TrieStats();
    EXPECT_EQ(second_field.term_count, 3);
    EXPECT_GT(second_field.memory_bytes, first.memory_bytes);
}

TEST(SegmentTextTermDictionaryTest, MutableTrieSupportsDamerauAndUtf8) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"book", "你好"});

    auto transposition = dictionary.FuzzySearch(101, {}, "boko", 1, 50);
    ASSERT_EQ(transposition.size(), 1);
    EXPECT_EQ(transposition[0].term, "book");
    EXPECT_EQ(transposition[0].edit_distance, 1);

    auto utf8 = dictionary.FuzzySearch(101, {}, "你号", 1, 50);
    ASSERT_EQ(utf8.size(), 1);
    EXPECT_EQ(utf8[0].term, "你好");
    EXPECT_EQ(utf8[0].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, MutableTrieKeepsBoundedBestMatches) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"boo", "coo", "doo", "zoo"});

    const auto matches = dictionary.FuzzySearch(101, {}, "zoo", 1, 2);
    ASSERT_EQ(matches.size(), 2);
    EXPECT_EQ(matches[0].term, "zoo");
    EXPECT_EQ(matches[0].edit_distance, 0);
    EXPECT_EQ(matches[1].term, "boo");
    EXPECT_EQ(matches[1].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, CombinesFstsAndMutableTrie) {
    TextFst first;
    BuildFst(first, {"boo", "coo"});
    TextFst second;
    BuildFst(second, {"doo"});
    const std::vector<const TextFst*> fsts{&first, &second};

    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"zoo", "boo"});
    const auto matches = dictionary.FuzzySearch(101, fsts, "zoo", 1, 1);

    // The current contract applies max_expansions to each FST/Trie before
    // merging, so the union may be larger than the configured value.
    ASSERT_EQ(matches.size(), 3);
    EXPECT_EQ(matches[0].term, "zoo");
    EXPECT_EQ(matches[0].edit_distance, 0);
    EXPECT_EQ(matches[1].term, "boo");
    EXPECT_EQ(matches[1].edit_distance, 1);
    EXPECT_EQ(matches[2].term, "doo");
    EXPECT_EQ(matches[2].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, ImportsFstsIntoOneMutableTrie) {
    TextFst first;
    BuildFst(first, {"book", "fuzzy"});
    TextFst second;
    BuildFst(second, {"books", "fuzzy", "milvus"});
    const std::vector<const TextFst*> fsts{&first, &second};

    SegmentTextTermDictionary dictionary;
    dictionary.AddFstTerms(101, fsts);
    const auto stats = dictionary.TrieStats();
    EXPECT_EQ(stats.term_count, 4);
    EXPECT_GT(stats.memory_bytes, 0);

    // The expansion bound is applied once to the complete imported
    // vocabulary, rather than once per recovery fragment.
    const auto matches = dictionary.FuzzySearch(101, {}, "book", 1, 1);
    ASSERT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0].term, "book");
    EXPECT_EQ(matches[0].edit_distance, 0);
}

TEST(SegmentTextTermDictionaryTest, RejectsInvalidTermsBeforeMutation) {
    SegmentTextTermDictionary dictionary;
    const std::string invalid_utf8(1, static_cast<char>(0xff));
    EXPECT_THROW(dictionary.AddTerms(101, {"valid", invalid_utf8}),
                 std::invalid_argument);
    EXPECT_THROW(dictionary.AddTerms(101, {""}), std::invalid_argument);
    EXPECT_EQ(dictionary.TrieStats().term_count, 0);
}

}  // namespace
}  // namespace milvus::textindex
