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
                memory_bytes_ += sizeof(Node);
                memory_bytes_ +=
                    (node->edges.capacity() - old_capacity) * sizeof(Edge);
            }
            node = position->target.get();
        }
        if (node->terminal) {
            return false;
        }
        node->terminal = true;
        ++term_count_;
        return true;
    }

    [[nodiscard]] std::vector<TextTermMatch>
    FuzzySearch(std::string_view query,
                std::uint32_t max_edit_distance,
                std::size_t max_expansions) const {
        if (max_edit_distance > 2) {
            throw std::invalid_argument(
                "mutable text term trie edit distance must be in [0, 2]");
        }
        ValidateUtf8(query);
        if (max_expansions == 0 || term_count_ == 0) {
            return {};
        }

        const auto dfa = BuildLevenshteinDfa(query, max_edit_distance);
        BoundedTextTermMatches matches(max_expansions);
        std::string term;
        struct Frame {
            const Node* node = nullptr;
            std::uint32_t dfa_state = 0;
            std::size_t next_edge = 0;
            bool entered = false;
        };
        std::vector<Frame> stack;
        stack.push_back(Frame{
            .node = &root_,
            .dfa_state = dfa.InitialState(),
        });
        while (!stack.empty()) {
            auto& frame = stack.back();
            if (!frame.entered) {
                frame.entered = true;
                if (frame.node->terminal && dfa.IsMatch(frame.dfa_state)) {
                    matches.Add(TextTermMatch{
                        .term = term,
                        .edit_distance = dfa.Distance(frame.dfa_state),
                    });
                }
            }
            if (frame.next_edge >= frame.node->edges.size()) {
                stack.pop_back();
                if (!stack.empty()) {
                    term.pop_back();
                }
                continue;
            }

            const auto& edge = frame.node->edges[frame.next_edge++];
            const auto next_state = dfa.Transition(frame.dfa_state, edge.label);
            if (!dfa.CanMatch(next_state)) {
                continue;
            }
            term.push_back(static_cast<char>(edge.label));
            stack.push_back(Frame{
                .node = edge.target.get(),
                .dfa_state = next_state,
            });
        }
        return matches.Take();
    }

    [[nodiscard]] TextTermTrieStats
    Stats() const {
        return TextTermTrieStats{
            .term_count = term_count_,
            .memory_bytes = memory_bytes_,
        };
    }

 private:
    Node root_;
    std::size_t term_count_ = 0;
    std::size_t memory_bytes_ = 0;
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

    std::unique_lock lock(impl_->mutex);
    auto& trie = impl_->tries[field_id];
    if (trie == nullptr) {
        trie = std::make_unique<MutableTermTrie>();
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

    std::unique_lock lock(impl_->mutex);
    auto& trie = impl_->tries[field_id];
    if (trie == nullptr) {
        trie = std::make_unique<MutableTermTrie>();
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

std::vector<TextTermMatch>
SegmentTextTermDictionary::FuzzySearch(
    std::int64_t field_id,
    std::span<const TextFst* const> immutable_fsts,
    std::string_view query,
    std::uint32_t max_edit_distance,
    std::size_t max_expansions) const {
    if (max_edit_distance > 2) {
        throw std::invalid_argument(
            "text term edit distance must be in [0, 2]");
    }
    if (max_expansions == 0) {
        throw std::invalid_argument(
            "text term max expansions must be positive");
    }
    ValidateUtf8(query);

    std::unordered_map<std::string, std::uint32_t> merged;
    const auto merge = [&merged](std::string term, std::uint32_t distance) {
        const auto [position, inserted] =
            merged.try_emplace(std::move(term), distance);
        if (!inserted && distance < position->second) {
            position->second = distance;
        }
    };

    // Each immutable FST and the mutable Trie currently applies
    // max_expansions independently. Sharing one competitive top-N collector
    // and automaton state across all components is a follow-up optimization.
    for (const auto* fst : immutable_fsts) {
        if (fst == nullptr) {
            throw std::invalid_argument("text term FST handle is null");
        }
        auto result =
            fst->FuzzySearch(query, max_edit_distance, max_expansions);
        for (auto& match : result.matches) {
            merge(std::move(match.term), match.edit_distance);
        }
    }

    {
        std::shared_lock lock(impl_->mutex);
        if (const auto position = impl_->tries.find(field_id);
            position != impl_->tries.end()) {
            auto trie_matches = position->second->FuzzySearch(
                query, max_edit_distance, max_expansions);
            for (auto& match : trie_matches) {
                merge(std::move(match.term), match.edit_distance);
            }
        }
    }

    std::vector<TextTermMatch> result;
    result.reserve(merged.size());
    for (auto& [term, distance] : merged) {
        result.push_back(TextTermMatch{
            .term = std::move(term),
            .edit_distance = distance,
        });
    }
    std::sort(result.begin(), result.end(), TextTermMatchBetter{});
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
