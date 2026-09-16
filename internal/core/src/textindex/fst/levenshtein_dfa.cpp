// The MIT License (MIT)
//
// Copyright (c) 2018 Paul Masurel
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "levenshtein_dfa.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <compare>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus::textindex {
namespace {

constexpr std::uint32_t kSinkState = 0;

std::vector<std::uint32_t>
DecodeUtf8(std::string_view text, bool collect = true) {
    std::vector<std::uint32_t> codepoints;
    if (collect) {
        codepoints.reserve(text.size());
    }
    for (std::size_t i = 0; i < text.size();) {
        const auto first = static_cast<std::uint8_t>(text[i]);
        std::uint32_t value = 0;
        std::uint32_t minimum = 0;
        std::size_t length = 0;
        if (first < 0x80) {
            value = first;
            length = 1;
        } else if ((first & 0xE0U) == 0xC0U) {
            value = first & 0x1FU;
            minimum = 0x80;
            length = 2;
        } else if ((first & 0xF0U) == 0xE0U) {
            value = first & 0x0FU;
            minimum = 0x800;
            length = 3;
        } else if ((first & 0xF8U) == 0xF0U) {
            value = first & 0x07U;
            minimum = 0x10000;
            length = 4;
        } else {
            throw std::invalid_argument("invalid UTF-8 leading byte");
        }
        if (i + length > text.size()) {
            throw std::invalid_argument("truncated UTF-8 sequence");
        }
        for (std::size_t offset = 1; offset < length; ++offset) {
            const auto byte = static_cast<std::uint8_t>(text[i + offset]);
            if ((byte & 0xC0U) != 0x80U) {
                throw std::invalid_argument("invalid UTF-8 continuation byte");
            }
            value = (value << 6) | (byte & 0x3FU);
        }
        if ((length != 1 && value < minimum) || value > 0x10FFFF ||
            (value >= 0xD800 && value <= 0xDFFF)) {
            throw std::invalid_argument("invalid UTF-8 code point");
        }
        if (collect) {
            codepoints.push_back(value);
        }
        i += length;
    }
    return codepoints;
}

std::array<std::uint8_t, 4>
EncodeUtf8(std::uint32_t codepoint, std::size_t& length) {
    std::array<std::uint8_t, 4> bytes{};
    if (codepoint <= 0x7F) {
        bytes[0] = static_cast<std::uint8_t>(codepoint);
        length = 1;
    } else if (codepoint <= 0x7FF) {
        bytes[0] = static_cast<std::uint8_t>(0xC0U | (codepoint >> 6));
        bytes[1] = static_cast<std::uint8_t>(0x80U | (codepoint & 0x3FU));
        length = 2;
    } else if (codepoint <= 0xFFFF) {
        bytes[0] = static_cast<std::uint8_t>(0xE0U | (codepoint >> 12));
        bytes[1] =
            static_cast<std::uint8_t>(0x80U | ((codepoint >> 6) & 0x3FU));
        bytes[2] = static_cast<std::uint8_t>(0x80U | (codepoint & 0x3FU));
        length = 3;
    } else {
        bytes[0] = static_cast<std::uint8_t>(0xF0U | (codepoint >> 18));
        bytes[1] =
            static_cast<std::uint8_t>(0x80U | ((codepoint >> 12) & 0x3FU));
        bytes[2] =
            static_cast<std::uint8_t>(0x80U | ((codepoint >> 6) & 0x3FU));
        bytes[3] = static_cast<std::uint8_t>(0x80U | (codepoint & 0x3FU));
        length = 4;
    }
    return bytes;
}

struct NfaState {
    std::uint32_t offset = 0;
    std::uint8_t distance = 0;
    bool in_transpose = false;

    auto
    operator<=>(const NfaState&) const = default;

    bool
    Implies(const NfaState& other) const {
        const bool transpose_implies = in_transpose || !other.in_transpose;
        const auto delta = offset >= other.offset ? offset - other.offset
                                                  : other.offset - offset;
        if (transpose_implies) {
            return static_cast<std::uint32_t>(other.distance) >=
                   static_cast<std::uint32_t>(distance) + delta;
        }
        return static_cast<std::uint32_t>(other.distance) >
               static_cast<std::uint32_t>(distance) + delta;
    }
};

struct MultiState {
    std::vector<NfaState> states;

    bool
    operator==(const MultiState&) const = default;

    void
    Add(NfaState state) {
        if (std::any_of(
                states.begin(), states.end(), [&](const auto& existing) {
                    return existing.Implies(state);
                })) {
            return;
        }
        for (std::size_t index = 0; index < states.size();) {
            if (state.Implies(states[index])) {
                states[index] = states.back();
                states.pop_back();
            } else {
                ++index;
            }
        }
        states.push_back(state);
    }

    std::uint32_t
    Normalize() {
        std::uint32_t minimum = 0;
        if (!states.empty()) {
            minimum = std::min_element(states.begin(),
                                       states.end(),
                                       [](const auto& left, const auto& right) {
                                           return left.offset < right.offset;
                                       })
                          ->offset;
        }
        for (auto& state : states) {
            state.offset -= minimum;
        }
        std::sort(states.begin(), states.end());
        return minimum;
    }
};

struct MultiStateHash {
    std::size_t
    operator()(const MultiState& multistate) const noexcept {
        constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
        constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
        std::uint64_t hash = kFnvOffset;
        auto mix = [&](std::uint64_t value) {
            hash = (hash ^ value) * kFnvPrime;
        };
        for (const auto& state : multistate.states) {
            mix(state.offset);
            mix(state.distance);
            mix(state.in_transpose ? 1 : 0);
        }
        return static_cast<std::size_t>(hash);
    }
};

class LevenshteinNfa {
 public:
    explicit LevenshteinNfa(std::uint8_t max_distance)
        : max_distance_(max_distance) {
    }

    std::uint8_t
    Diameter() const {
        return 2 * max_distance_ + 1;
    }

    MultiState
    InitialStates() const {
        MultiState states;
        states.Add(NfaState{});
        return states;
    }

    std::uint8_t
    Distance(const MultiState& states, std::uint32_t query_length) const {
        auto result = static_cast<std::uint8_t>(max_distance_ + 1);
        for (const auto& state : states.states) {
            const auto offset_delta = query_length >= state.offset
                                          ? query_length - state.offset
                                          : state.offset - query_length;
            const auto distance =
                static_cast<std::uint32_t>(state.distance) + offset_delta;
            if (distance <= max_distance_) {
                result = std::min(result, static_cast<std::uint8_t>(distance));
            }
        }
        return result;
    }

    void
    Transition(const MultiState& current,
               MultiState& destination,
               std::uint64_t characteristic) const {
        destination.states.clear();
        const auto mask = (std::uint64_t{1} << Diameter()) - 1;
        for (const auto& state : current.states) {
            SimpleTransition(
                state, (characteristic >> state.offset) & mask, destination);
        }
        std::sort(destination.states.begin(), destination.states.end());
    }

 private:
    static bool
    ExtractBit(std::uint64_t value, std::uint8_t bit) {
        return ((value >> bit) & 1U) != 0;
    }

    void
    SimpleTransition(const NfaState& state,
                     std::uint64_t symbol,
                     MultiState& destination) const {
        if (state.distance < max_distance_) {
            destination.Add(NfaState{
                .offset = state.offset,
                .distance = static_cast<std::uint8_t>(state.distance + 1),
            });
            destination.Add(NfaState{
                .offset = state.offset + 1,
                .distance = static_cast<std::uint8_t>(state.distance + 1),
            });
            for (std::uint8_t deletion = 1;
                 deletion < max_distance_ + 1 - state.distance;
                 ++deletion) {
                if (ExtractBit(symbol, deletion)) {
                    destination.Add(NfaState{
                        .offset = state.offset + 1 + deletion,
                        .distance = static_cast<std::uint8_t>(state.distance +
                                                              deletion),
                    });
                }
            }
            if (ExtractBit(symbol, 1)) {
                destination.Add(NfaState{
                    .offset = state.offset,
                    .distance = static_cast<std::uint8_t>(state.distance + 1),
                    .in_transpose = true,
                });
            }
        }
        if (ExtractBit(symbol, 0)) {
            destination.Add(NfaState{
                .offset = state.offset + 1,
                .distance = state.distance,
            });
        }
        if (state.in_transpose && ExtractBit(symbol, 0)) {
            destination.Add(NfaState{
                .offset = state.offset + 2,
                .distance = state.distance,
            });
        }
    }

    std::uint8_t max_distance_;
};

struct ParametricTransition {
    std::uint32_t destination_shape = 0;
    std::uint32_t offset_delta = 0;
};

struct ParametricState {
    std::uint32_t shape = 0;
    std::uint32_t offset = 0;

    bool
    operator==(const ParametricState&) const = default;
};

class ParametricDfa {
 public:
    static ParametricDfa
    Build(std::uint8_t max_distance) {
        LevenshteinNfa nfa(max_distance);
        std::vector<MultiState> shapes;
        std::unordered_map<MultiState, std::uint32_t, MultiStateHash> shape_ids;
        auto allocate = [&](const MultiState& shape) {
            if (const auto found = shape_ids.find(shape);
                found != shape_ids.end()) {
                return found->second;
            }
            const auto id = static_cast<std::uint32_t>(shapes.size());
            shapes.push_back(shape);
            shape_ids.emplace(shape, id);
            return id;
        };
        allocate(MultiState{});
        allocate(nfa.InitialStates());

        ParametricDfa dfa;
        dfa.max_distance_ = max_distance;
        dfa.diameter_ = nfa.Diameter();
        dfa.transition_stride_ = std::size_t{1} << dfa.diameter_;
        MultiState destination;
        for (std::size_t shape_id = 0; shape_id < shapes.size(); ++shape_id) {
            const auto source = shapes[shape_id];
            for (std::size_t characteristic = 0;
                 characteristic < dfa.transition_stride_;
                 ++characteristic) {
                nfa.Transition(source, destination, characteristic);
                const auto offset_delta = destination.Normalize();
                const auto destination_id = allocate(destination);
                dfa.transitions_.push_back(ParametricTransition{
                    .destination_shape = destination_id,
                    .offset_delta = offset_delta,
                });
            }
        }
        dfa.distances_.reserve(shapes.size() * dfa.diameter_);
        for (const auto& shape : shapes) {
            for (std::uint32_t offset = 0; offset < dfa.diameter_; ++offset) {
                dfa.distances_.push_back(nfa.Distance(shape, offset));
            }
        }
        return dfa;
    }

    std::size_t
    ShapeCount() const {
        return transitions_.size() / transition_stride_;
    }

    ParametricState
    Apply(ParametricState state, std::uint32_t characteristic) const {
        const auto& transition =
            transitions_.at(transition_stride_ * state.shape + characteristic);
        return ParametricState{
            .shape = transition.destination_shape,
            .offset = transition.destination_shape == 0
                          ? 0
                          : state.offset + transition.offset_delta,
        };
    }

    std::uint8_t
    Distance(ParametricState state, std::size_t query_length) const {
        if (state.shape == 0 || state.offset > query_length) {
            return static_cast<std::uint8_t>(max_distance_ + 1);
        }
        const auto remaining = query_length - state.offset;
        if (remaining >= diameter_) {
            return static_cast<std::uint8_t>(max_distance_ + 1);
        }
        return distances_.at(diameter_ * state.shape + remaining);
    }

    std::size_t
    Diameter() const {
        return diameter_;
    }

 private:
    std::vector<std::uint8_t> distances_;
    std::vector<ParametricTransition> transitions_;
    std::size_t transition_stride_ = 0;
    std::size_t diameter_ = 0;
    std::uint8_t max_distance_ = 0;
};

const ParametricDfa&
ParametricForDistance(std::uint32_t max_distance) {
    switch (max_distance) {
        case 0: {
            static const auto dfa = ParametricDfa::Build(0);
            return dfa;
        }
        case 1: {
            static const auto dfa = ParametricDfa::Build(1);
            return dfa;
        }
        case 2: {
            static const auto dfa = ParametricDfa::Build(2);
            return dfa;
        }
        default:
            throw std::invalid_argument(
                "text FST fuzzy distance must be in [0, 2]");
    }
}

struct CharacteristicVector {
    std::vector<std::size_t> positions;

    std::uint32_t
    ShiftAndMask(std::size_t offset, std::uint32_t mask) const {
        std::uint32_t result = 0;
        auto position =
            std::lower_bound(positions.begin(), positions.end(), offset);
        while (position != positions.end() && *position - offset < 32) {
            result |= std::uint32_t{1} << (*position - offset);
            ++position;
        }
        return result & mask;
    }
};

struct AlphabetEntry {
    std::uint32_t codepoint = 0;
    CharacteristicVector characteristic;
};

std::vector<AlphabetEntry>
BuildAlphabet(const std::vector<std::uint32_t>& query) {
    std::vector<AlphabetEntry> entries;
    std::unordered_map<std::uint32_t, std::size_t> entry_ids;
    for (std::size_t index = 0; index < query.size(); ++index) {
        const auto [position, inserted] =
            entry_ids.try_emplace(query[index], entries.size());
        if (inserted) {
            entries.push_back(AlphabetEntry{.codepoint = query[index]});
        }
        entries[position->second].characteristic.positions.push_back(index);
    }
    return entries;
}

class ParametricStateIndex {
 public:
    ParametricStateIndex(std::size_t offsets, std::size_t index_size)
        : offsets_(offsets), index_(index_size) {
        states_.reserve(100);
    }

    std::uint32_t
    GetOrAllocate(ParametricState state) {
        const auto bucket =
            static_cast<std::size_t>(state.shape) * offsets_ + state.offset;
        if (bucket >= index_.size()) {
            throw std::runtime_error("parametric Levenshtein state overflow");
        }
        if (index_[bucket].has_value()) {
            return *index_[bucket];
        }
        const auto id = static_cast<std::uint32_t>(states_.size());
        index_[bucket] = id;
        states_.push_back(state);
        return id;
    }

    const ParametricState&
    Get(std::uint32_t id) const {
        return states_.at(id);
    }

    std::size_t
    Size() const {
        return states_.size();
    }

    std::size_t
    MaxSize() const {
        return index_.size();
    }

 private:
    std::size_t offsets_;
    std::vector<std::optional<std::uint32_t>> index_;
    std::vector<ParametricState> states_;
};

class Utf8DfaEncoder {
 public:
    Utf8DfaEncoder(std::size_t max_decoded_states,
                   std::size_t encoded_index_size,
                   std::vector<std::array<std::uint32_t, 256>>& transitions,
                   std::vector<std::uint8_t>& distances)
        : index_(encoded_index_size, kUnallocated),
          defaults_(max_decoded_states),
          encoded_originals_(max_decoded_states, kUnallocated),
          transitions_(transitions),
          distances_(distances) {
    }

    void
    AddState(std::uint32_t state,
             std::uint8_t distance,
             std::uint32_t default_successor) {
        const auto encoded_state = GetOrAllocate(Original(state));
        distances_[encoded_state] = distance;
        const auto default_encoded = GetOrAllocate(Original(default_successor));
        std::array<std::uint32_t, 4> predecessors{};
        predecessors.fill(default_encoded);
        for (std::uint32_t bytes = 1; bytes < 4; ++bytes) {
            const auto predecessor =
                GetOrAllocate(Predecessor(default_successor, bytes));
            predecessors[bytes] = predecessor;
            transitions_[predecessor].fill(predecessors[bytes - 1]);
        }
        auto& table = transitions_[encoded_state];
        std::fill(table.begin(), table.begin() + 192, predecessors[0]);
        std::fill(table.begin() + 192, table.begin() + 224, predecessors[1]);
        std::fill(table.begin() + 224, table.begin() + 240, predecessors[2]);
        std::fill(table.begin() + 240, table.end(), predecessors[3]);
        defaults_[state] = predecessors;
        encoded_originals_[state] = encoded_state;
    }

    void
    AddTransition(std::uint32_t state,
                  std::uint32_t codepoint,
                  std::uint32_t destination) {
        std::size_t length = 0;
        const auto bytes = EncodeUtf8(codepoint, length);
        auto from = encoded_originals_.at(state);
        const auto defaults = defaults_.at(state);
        for (std::size_t index = 0; index + 1 < length; ++index) {
            const auto remaining = length - index - 1;
            auto intermediary = transitions_[from][bytes[index]];
            if (intermediary == defaults[remaining]) {
                intermediary = Allocate();
                transitions_[intermediary].fill(defaults[remaining - 1]);
                transitions_[from][bytes[index]] = intermediary;
            }
            from = intermediary;
        }
        const auto encoded_destination = GetOrAllocate(Original(destination));
        transitions_[from][bytes[length - 1]] = encoded_destination;
    }

    [[nodiscard]] std::uint32_t
    EncodedOriginal(std::uint32_t state) {
        return GetOrAllocate(Original(state));
    }

 private:
    static constexpr std::uint32_t kUnallocated =
        std::numeric_limits<std::uint32_t>::max();

    static std::size_t
    Original(std::uint32_t state) {
        return static_cast<std::size_t>(state) * 4;
    }

    static std::size_t
    Predecessor(std::uint32_t state, std::uint32_t steps) {
        return static_cast<std::size_t>(state) * 4 + steps;
    }

    [[nodiscard]] std::uint32_t
    Allocate() {
        if (transitions_.size() >= std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("UTF-8 DFA has too many states");
        }
        const auto id = static_cast<std::uint32_t>(transitions_.size());
        transitions_.push_back({});
        distances_.push_back(255);
        return id;
    }

    [[nodiscard]] std::uint32_t
    GetOrAllocate(std::size_t bucket) {
        if (bucket >= index_.size()) {
            throw std::runtime_error("UTF-8 DFA state index overflow");
        }
        if (index_[bucket] == kUnallocated) {
            index_[bucket] = Allocate();
        }
        return index_[bucket];
    }

    std::vector<std::uint32_t> index_;
    std::vector<std::array<std::uint32_t, 4>> defaults_;
    std::vector<std::uint32_t> encoded_originals_;
    std::vector<std::array<std::uint32_t, 256>>& transitions_;
    std::vector<std::uint8_t>& distances_;
};

}  // namespace

std::uint32_t
LevenshteinDfa::InitialState() const {
    return initial_state_;
}

std::uint32_t
LevenshteinDfa::Transition(std::uint32_t state, std::uint8_t byte) const {
    return transitions_[state][byte];
}

bool
LevenshteinDfa::IsMatch(std::uint32_t state) const {
    return distances_[state] <= max_distance_;
}

bool
LevenshteinDfa::CanMatch(std::uint32_t state) const {
    return state != kSinkState;
}

std::uint32_t
LevenshteinDfa::Distance(std::uint32_t state) const {
    return distances_[state];
}

LevenshteinDfa
BuildLevenshteinDfa(std::string_view query, std::uint32_t max_distance) {
    const auto& parametric = ParametricForDistance(max_distance);
    const auto query_codepoints = DecodeUtf8(query);
    const auto alphabet = BuildAlphabet(query_codepoints);
    if (query_codepoints.size() == std::numeric_limits<std::size_t>::max()) {
        throw std::length_error("Levenshtein query is too long");
    }
    const auto offsets = query_codepoints.size() + 1;
    if (parametric.ShapeCount() != 0 &&
        offsets >
            std::numeric_limits<std::size_t>::max() / parametric.ShapeCount()) {
        throw std::length_error("Levenshtein state index is too large");
    }
    ParametricStateIndex state_index(offsets,
                                     offsets * parametric.ShapeCount());
    const auto dead = state_index.GetOrAllocate(ParametricState{});
    if (dead != kSinkState) {
        throw std::logic_error("Levenshtein sink state must be zero");
    }
    const auto initial = state_index.GetOrAllocate(ParametricState{
        .shape = 1,
        .offset = 0,
    });
    LevenshteinDfa dfa;
    dfa.max_distance_ = static_cast<std::uint8_t>(max_distance);
    const auto max_decoded_states = state_index.MaxSize();
    if (max_decoded_states >
        (std::numeric_limits<std::size_t>::max() - 3) / 4) {
        throw std::length_error("UTF-8 DFA state index is too large");
    }
    Utf8DfaEncoder encoder(max_decoded_states,
                           max_decoded_states * 4 + 3,
                           dfa.transitions_,
                           dfa.distances_);
    const auto mask = static_cast<std::uint32_t>(
        (std::uint64_t{1} << parametric.Diameter()) - 1);
    for (std::uint32_t state_id = 0; state_id < state_index.Size();
         ++state_id) {
        const auto state = state_index.Get(state_id);
        const auto distance =
            parametric.Distance(state, query_codepoints.size());
        const auto default_successor =
            state_index.GetOrAllocate(parametric.Apply(state, 0));
        encoder.AddState(state_id, distance, default_successor);
        for (const auto& entry : alphabet) {
            const auto characteristic =
                entry.characteristic.ShiftAndMask(state.offset, mask);
            const auto destination = state_index.GetOrAllocate(
                parametric.Apply(state, characteristic));
            encoder.AddTransition(state_id, entry.codepoint, destination);
        }
    }
    dfa.initial_state_ = encoder.EncodedOriginal(initial);
    return dfa;
}

PreparedLevenshteinQuery
PrepareLevenshteinQuery(std::string_view query,
                        std::uint32_t max_distance,
                        std::uint32_t prefix_length) {
    if (max_distance > 2) {
        throw std::invalid_argument(
            "text FST fuzzy distance must be in [0, 2]");
    }
    const auto prefix_bytes = Utf8PrefixByteLength(query, prefix_length);
    PreparedLevenshteinQuery prepared{
        .query = std::string(query),
        .exact_prefix = std::string(query.substr(0, prefix_bytes)),
        .max_distance = max_distance,
    };
    if (max_distance > 0) {
        prepared.dfa =
            BuildLevenshteinDfa(query.substr(prefix_bytes), max_distance);
    }
    return prepared;
}

void
ValidateUtf8(std::string_view text) {
    static_cast<void>(DecodeUtf8(text, false));
}

std::size_t
Utf8PrefixByteLength(std::string_view text, std::size_t char_count) {
    const auto codepoints = DecodeUtf8(text);
    if (char_count >= codepoints.size()) {
        return text.size();
    }

    std::size_t byte_offset = 0;
    for (std::size_t index = 0; index < char_count; ++index) {
        ++byte_offset;
        while (byte_offset < text.size() &&
               (static_cast<std::uint8_t>(text[byte_offset]) & 0xC0U) ==
                   0x80U) {
            ++byte_offset;
        }
    }
    return byte_offset;
}

}  // namespace milvus::textindex
