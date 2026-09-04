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

#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "textindex/fst/levenshtein_dfa.h"
#include "textindex/fst/text_fst.h"

namespace milvus::textindex {
namespace {

struct TextTermMatchBetter {
    bool
    operator()(const TextTermMatch& left, const TextTermMatch& right) const {
        if (left.edit_distance != right.edit_distance) {
            return left.edit_distance < right.edit_distance;
        }
        return left.term < right.term;
    }
};

class BoundedTextTermMatches {
 public:
    explicit BoundedTextTermMatches(std::size_t limit) : limit_(limit) {
    }

    void
    Add(TextTermMatch match) {
        if (limit_ == 0) {
            return;
        }
        if (matches_.size() < limit_) {
            matches_.push(std::move(match));
            return;
        }
        if (TextTermMatchBetter{}(match, matches_.top())) {
            matches_.pop();
            matches_.push(std::move(match));
        }
    }

    std::vector<TextTermMatch>
    Take() {
        std::vector<TextTermMatch> result;
        result.reserve(matches_.size());
        while (!matches_.empty()) {
            result.push_back(matches_.top());
            matches_.pop();
        }
        std::sort(result.begin(), result.end(), TextTermMatchBetter{});
        return result;
    }

 private:
    std::size_t limit_;
    std::priority_queue<TextTermMatch,
                        std::vector<TextTermMatch>,
                        TextTermMatchBetter>
        matches_;
};

class MutableTermTrie {
 private:
    struct Node;

    struct Edge {
        std::uint8_t label = 0;
        std::unique_ptr<Node> target;
    };

    struct Node {
        std::vector<Edge> edges;
        bool terminal = false;
    };

 public:
    MutableTermTrie() : memory_bytes_(sizeof(MutableTermTrie)) {
    }

    bool
    Insert(std::string_view term) {
        if (term.empty()) {
            throw std::invalid_argument("empty text terms are not supported");
        }
        Node* node = &root_;
        for (const unsigned char byte : term) {
            std::unique_lock lock(MutexFor(node));
            auto position =
                std::lower_bound(node->edges.begin(),
                                 node->edges.end(),
                                 byte,
                                 [](const Edge& edge, std::uint8_t label) {
                                     return edge.label < label;
                                 });
            if (position == node->edges.end() || position->label != byte) {
                const auto old_capacity = node->edges.capacity();
                auto child = std::make_unique<Node>();
                position = node->edges.insert(
                    position,
                    Edge{static_cast<std::uint8_t>(byte), std::move(child)});
                memory_bytes_.fetch_add(sizeof(Node),
                                        std::memory_order_relaxed);
                memory_bytes_.fetch_add(
                    (node->edges.capacity() - old_capacity) * sizeof(Edge),
                    std::memory_order_relaxed);
            }
            node = position->target.get();
        }
        std::unique_lock lock(MutexFor(node));
        if (node->terminal) {
            return false;
        }
        node->terminal = true;
        term_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    [[nodiscard]] TextTermFuzzySearchResult
    FuzzySearch(const PreparedLevenshteinQuery& query,
                std::size_t max_expansions) const {
        if (max_expansions == 0 ||
            term_count_.load(std::memory_order_relaxed) == 0) {
            return {};
        }

        TextTermFuzzySearchResult result;
        const Node* start = &root_;
        if (query.max_distance == 0) {
            for (const unsigned char byte : query.query) {
                start = FindChild(start, byte);
                if (start == nullptr) {
                    return result;
                }
            }
            if (IsTerminal(start)) {
                result.matches.push_back(TextTermMatch{
                    .term = query.query,
                    .edit_distance = 0,
                });
            }
            return result;
        }
        if (!query.dfa.has_value()) {
            throw std::invalid_argument(
                "mutable text term trie requires a prepared fuzzy query");
        }
        for (const unsigned char byte : query.exact_prefix) {
            start = FindChild(start, byte);
            if (start == nullptr) {
                return result;
            }
        }

        const auto& dfa = *query.dfa;
        BoundedTextTermMatches matches(max_expansions);
        std::string term(query.exact_prefix);
        struct Frame {
            const Node* node = nullptr;
            std::uint32_t dfa_state = 0;
            // Edges are append-only but remain sorted. A label cursor cannot
            // be shifted past an existing edge by a concurrent insertion.
            std::uint16_t next_label = 0;
            bool entered = false;
        };
        std::vector<Frame> stack;
        stack.push_back(Frame{
            .node = start,
            .dfa_state = dfa.InitialState(),
        });
        while (!stack.empty()) {
            auto& frame = stack.back();
            if (!frame.entered) {
                frame.entered = true;
                if (IsTerminal(frame.node) && dfa.IsMatch(frame.dfa_state)) {
                    matches.Add(TextTermMatch{
                        .term = term,
                        .edit_distance = dfa.Distance(frame.dfa_state),
                    });
                }
            }
            std::uint8_t label = 0;
            const Node* target = nullptr;
            if (!NextChild(frame.node, frame.next_label, label, target)) {
                stack.pop_back();
                if (!stack.empty()) {
                    term.pop_back();
                }
                continue;
            }

            const auto next_state = dfa.Transition(frame.dfa_state, label);
            if (!dfa.CanMatch(next_state)) {
                continue;
            }
            term.push_back(static_cast<char>(label));
            stack.push_back(Frame{
                .node = target,
                .dfa_state = next_state,
            });
        }
        result.matches = matches.Take();
        return result;
    }

    [[nodiscard]] TextTermTrieStats
    Stats() const {
        return TextTermTrieStats{
            .term_count = term_count_.load(std::memory_order_relaxed),
            .memory_bytes = memory_bytes_.load(std::memory_order_relaxed),
        };
    }

 private:
    const Node*
    FindChild(const Node* node, std::uint8_t label) const {
        std::shared_lock lock(MutexFor(node));
        const auto position =
            std::lower_bound(node->edges.begin(),
                             node->edges.end(),
                             label,
                             [](const Edge& edge, std::uint8_t value) {
                                 return edge.label < value;
                             });
        return position == node->edges.end() || position->label != label
                   ? nullptr
                   : position->target.get();
    }

    bool
    IsTerminal(const Node* node) const {
        std::shared_lock lock(MutexFor(node));
        return node->terminal;
    }

    bool
    NextChild(const Node* node,
              std::uint16_t& next_label,
              std::uint8_t& label,
              const Node*& target) const {
        if (next_label > std::numeric_limits<std::uint8_t>::max()) {
            return false;
        }
        std::shared_lock lock(MutexFor(node));
        const auto position =
            std::lower_bound(node->edges.begin(),
                             node->edges.end(),
                             static_cast<std::uint8_t>(next_label),
                             [](const Edge& edge, std::uint8_t value) {
                                 return edge.label < value;
                             });
        if (position == node->edges.end()) {
            return false;
        }
        label = position->label;
        target = position->target.get();
        next_label = static_cast<std::uint16_t>(label) + 1;
        return true;
    }

    std::shared_mutex&
    MutexFor(const Node* node) const {
        const auto address = reinterpret_cast<std::uintptr_t>(node);
        return mutexes_[(address >> 4) % mutexes_.size()];
    }

    Node root_;
    mutable std::array<std::shared_mutex, 64> mutexes_;
    std::atomic<std::size_t> term_count_ = 0;
    std::atomic<std::size_t> memory_bytes_ = 0;
};

}  // namespace

struct SegmentTextTermDictionary::Impl {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::int64_t, std::unique_ptr<MutableTermTrie>> tries;
};

SegmentTextTermDictionary::SegmentTextTermDictionary()
    : impl_(std::make_unique<Impl>()) {
}

SegmentTextTermDictionary::~SegmentTextTermDictionary() = default;

void
SegmentTextTermDictionary::AddTerms(std::int64_t field_id,
                                    const std::vector<std::string>& terms) {
    for (const auto& term : terms) {
        if (term.empty()) {
            throw std::invalid_argument("empty text terms are not supported");
        }
        ValidateUtf8(term);
    }
    if (terms.empty()) {
        return;
    }

    MutableTermTrie* trie;
    {
        std::unique_lock lock(impl_->mutex);
        auto& entry = impl_->tries[field_id];
        if (entry == nullptr) {
            entry = std::make_unique<MutableTermTrie>();
        }
        trie = entry.get();
    }
    for (const auto& term : terms) {
        trie->Insert(term);
    }
}

void
SegmentTextTermDictionary::AddFstTerms(
    std::int64_t field_id, std::span<const TextFst* const> immutable_fsts) {
    if (immutable_fsts.empty()) {
        return;
    }

    MutableTermTrie* trie;
    {
        std::unique_lock lock(impl_->mutex);
        auto& entry = impl_->tries[field_id];
        if (entry == nullptr) {
            entry = std::make_unique<MutableTermTrie>();
        }
        trie = entry.get();
    }
    for (const auto* fst : immutable_fsts) {
        if (fst == nullptr) {
            throw std::invalid_argument("text term FST handle is null");
        }
        fst->VisitTerms([&](std::string_view term) {
            if (term.empty()) {
                throw std::invalid_argument(
                    "empty text terms are not supported");
            }
            ValidateUtf8(term);
            trie->Insert(term);
        });
    }
}

TextTermFuzzySearchResult
SegmentTextTermDictionary::FuzzySearchPrepared(
    std::int64_t field_id,
    std::span<const TextFst* const> immutable_fsts,
    const PreparedLevenshteinQuery& query,
    std::size_t max_expansions) const {
    if (max_expansions == 0) {
        throw std::invalid_argument(
            "text term max expansions must be positive");
    }
    TextTermFuzzySearchResult result;

    std::unordered_map<std::string, std::uint32_t> merged;
    const auto merge = [&](std::string term, std::uint32_t distance) {
        const auto [position, inserted] =
            merged.try_emplace(std::move(term), distance);
        if (!inserted && distance < position->second) {
            position->second = distance;
        }
    };

    // Each immutable FST and the mutable Trie applies max_expansions
    // independently. They share the request-scoped query DFA but retain their
    // own competitive top-N collectors.
    for (const auto* fst : immutable_fsts) {
        if (fst == nullptr) {
            throw std::invalid_argument("text term FST handle is null");
        }
        auto fuzzy = fst->FuzzySearchPrepared(query, max_expansions);
        for (auto& match : fuzzy.matches) {
            merge(std::move(match.term), match.edit_distance);
        }
    }

    const MutableTermTrie* trie = nullptr;
    {
        std::shared_lock lock(impl_->mutex);
        const auto position = impl_->tries.find(field_id);
        if (position != impl_->tries.end()) {
            trie = position->second.get();
        }
    }
    if (trie != nullptr) {
        auto trie_result = trie->FuzzySearch(query, max_expansions);
        for (auto& match : trie_result.matches) {
            merge(std::move(match.term), match.edit_distance);
        }
    }

    result.matches.reserve(merged.size());
    for (auto& [term, distance] : merged) {
        result.matches.push_back(TextTermMatch{
            .term = std::move(term),
            .edit_distance = distance,
        });
    }
    std::sort(
        result.matches.begin(), result.matches.end(), TextTermMatchBetter{});
    return result;
}

TextTermTrieStats
SegmentTextTermDictionary::TrieStats() const {
    std::shared_lock lock(impl_->mutex);
    TextTermTrieStats result;
    for (const auto& [field_id, trie] : impl_->tries) {
        static_cast<void>(field_id);
        const auto stats = trie->Stats();
        result.term_count += stats.term_count;
        result.memory_bytes += stats.memory_bytes;
    }
    return result;
}

}  // namespace milvus::textindex
