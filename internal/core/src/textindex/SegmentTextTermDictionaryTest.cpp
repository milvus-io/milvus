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

#include <atomic>
#include <initializer_list>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "textindex/fst/levenshtein_dfa.h"
#include "textindex/fst/text_fst.h"

namespace milvus::textindex {
namespace {

void
BuildFst(TextFst& fst, std::initializer_list<std::string_view> terms) {
    auto term = terms.begin();
    fst.Build([&]() -> std::optional<std::string_view> {
        return term == terms.end() ? std::nullopt
                                   : std::optional<std::string_view>(*term++);
    });
}

TextTermFuzzySearchResult
Search(const SegmentTextTermDictionary& dictionary,
       std::int64_t field_id,
       std::span<const TextFst* const> fsts,
       std::string_view query,
       std::uint32_t max_edit_distance,
       std::size_t max_expansions,
       std::uint32_t prefix_length = 0) {
    const auto prepared =
        PrepareLevenshteinQuery(query, max_edit_distance, prefix_length);
    return dictionary.FuzzySearchPrepared(
        field_id, fsts, prepared, max_expansions);
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

    auto transposition = Search(dictionary, 101, {}, "boko", 1, 50);
    ASSERT_EQ(transposition.matches.size(), 1);
    EXPECT_EQ(transposition.matches[0].term, "book");
    EXPECT_EQ(transposition.matches[0].edit_distance, 1);

    auto utf8 = Search(dictionary, 101, {}, "你号", 1, 50);
    ASSERT_EQ(utf8.matches.size(), 1);
    EXPECT_EQ(utf8.matches[0].term, "你好");
    EXPECT_EQ(utf8.matches[0].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, MutableTrieSupportsConcurrentAddAndSearch) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"term0"});

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<std::size_t> searches = 0;
    std::thread reader([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (!done.load(std::memory_order_acquire)) {
            static_cast<void>(Search(dictionary, 101, {}, "term0", 1, 10));
            searches.fetch_add(1, std::memory_order_release);
        }
    });
    std::thread writer([&] {
        start.store(true, std::memory_order_release);
        while (searches.load(std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }
        for (int i = 1; i <= 1'000; ++i) {
            dictionary.AddTerms(101, {"term" + std::to_string(i)});
        }
        done.store(true, std::memory_order_release);
    });

    writer.join();
    reader.join();
    EXPECT_GT(searches.load(std::memory_order_acquire), 0);
    EXPECT_EQ(dictionary.TrieStats().term_count, 1'001);
    const auto result = Search(dictionary, 101, {}, "term1000", 0, 10);
    ASSERT_EQ(result.matches.size(), 1);
    EXPECT_EQ(result.matches[0].term, "term1000");
}

TEST(SegmentTextTermDictionaryTest, PrefixLengthUsesUnicodeCharacters) {
    SegmentTextTermDictionary growing;
    growing.AddTerms(101, {"book", "你好"});

    ASSERT_EQ(Search(growing, 101, {}, "cook", 1, 50).matches.size(), 1);
    EXPECT_TRUE(Search(growing, 101, {}, "cook", 1, 50, 1).matches.empty());

    const auto unicode = Search(growing, 101, {}, "你号", 1, 50, 1);
    ASSERT_EQ(unicode.matches.size(), 1);
    EXPECT_EQ(unicode.matches[0].term, "你好");
    EXPECT_EQ(unicode.matches[0].edit_distance, 1);
    EXPECT_TRUE(Search(growing, 101, {}, "他好", 1, 50, 1).matches.empty());

    TextFst sealed;
    BuildFst(sealed, {"book", "你好"});
    const std::vector<const TextFst*> fsts{&sealed};
    ASSERT_EQ(Search(growing, 102, fsts, "cook", 1, 50).matches.size(), 1);
    EXPECT_TRUE(Search(growing, 102, fsts, "cook", 1, 50, 1).matches.empty());
    ASSERT_EQ(Search(growing, 102, fsts, "你号", 1, 50, 1).matches.size(), 1);
    EXPECT_TRUE(Search(growing, 102, fsts, "他好", 1, 50, 1).matches.empty());
}

TEST(SegmentTextTermDictionaryTest, MutableTrieKeepsBoundedBestMatches) {
    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"boo", "coo", "doo", "zoo"});

    const auto matches = Search(dictionary, 101, {}, "zoo", 1, 2);
    ASSERT_EQ(matches.matches.size(), 2);
    EXPECT_EQ(matches.matches[0].term, "zoo");
    EXPECT_EQ(matches.matches[0].edit_distance, 0);
    EXPECT_EQ(matches.matches[1].term, "boo");
    EXPECT_EQ(matches.matches[1].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, CombinesFstsAndMutableTrie) {
    TextFst first;
    BuildFst(first, {"boo", "coo"});
    TextFst second;
    BuildFst(second, {"doo"});
    const std::vector<const TextFst*> fsts{&first, &second};

    SegmentTextTermDictionary dictionary;
    dictionary.AddTerms(101, {"zoo", "boo"});
    const auto matches = Search(dictionary, 101, fsts, "zoo", 1, 1);

    // The current contract applies max_expansions to each FST/Trie before
    // merging, so the union may be larger than the configured value.
    ASSERT_EQ(matches.matches.size(), 3);
    EXPECT_EQ(matches.matches[0].term, "zoo");
    EXPECT_EQ(matches.matches[0].edit_distance, 0);
    EXPECT_EQ(matches.matches[1].term, "boo");
    EXPECT_EQ(matches.matches[1].edit_distance, 1);
    EXPECT_EQ(matches.matches[2].term, "doo");
    EXPECT_EQ(matches.matches[2].edit_distance, 1);
}

TEST(SegmentTextTermDictionaryTest, ReusesOnePreparedQueryAcrossSegments) {
    TextFst first_fst;
    BuildFst(first_fst, {"book"});
    TextFst second_fst;
    BuildFst(second_fst, {"books"});
    const std::vector<const TextFst*> fsts{&first_fst, &second_fst};

    SegmentTextTermDictionary sealed;
    SegmentTextTermDictionary growing;
    growing.AddTerms(101, {"boo"});
    const auto prepared = PrepareLevenshteinQuery("bok", 1, 0);

    const auto sealed_matches =
        sealed.FuzzySearchPrepared(101, fsts, prepared, 50);
    const auto growing_matches =
        growing.FuzzySearchPrepared(101, {}, prepared, 50);
    ASSERT_EQ(sealed_matches.matches.size(), 1);
    EXPECT_EQ(sealed_matches.matches[0].term, "book");
    ASSERT_EQ(growing_matches.matches.size(), 1);
    EXPECT_EQ(growing_matches.matches[0].term, "boo");
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
    const auto matches = Search(dictionary, 101, {}, "book", 1, 1);
    ASSERT_EQ(matches.matches.size(), 1);
    EXPECT_EQ(matches.matches[0].term, "book");
    EXPECT_EQ(matches.matches[0].edit_distance, 0);
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
