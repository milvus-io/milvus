// The serialized-format implementation derives from BurntSushi/fst 0.4.7.
//
// The MIT License (MIT)
//
// Copyright (c) 2015 Andrew Gallant
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

#include "textindex/fst/text_fst.h"

#include "crc32c/crc32c.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace milvus::textindex {
namespace {

using Address = std::size_t;
using Output = std::uint64_t;

constexpr std::uint64_t kVersion = 3;
constexpr std::uint64_t kFstType = 0;
constexpr Address kEmptyAddress = 0;
constexpr Address kNoneAddress = 1;
constexpr std::size_t kTransitionIndexThreshold = 32;
constexpr std::size_t kTrailerBytes = 20;
constexpr std::size_t kRootAddressOffset = kTrailerBytes + 1;
constexpr std::array<std::uint8_t, 63> kCommonInputs = {
    't', 'e', '/', 'o', 'a', 's', 'r', 'i', 'p', 'c', 'n', 'w', '.',
    'h', 'l', 'm', '-', 'd', 'u', '0', '1', '2', 'g', '=', ':', 'b',
    'f', '3', 'y', '5', '&', '_', '4', 'v', '9', '6', '7', '8', 'k',
    '%', '?', 'x', 'C', 'D', 'A', 'S', 'F', 'I', 'B', 'E', 'j', 'P',
    'T', 'z', 'R', 'N', 'M', '+', 'L', 'O', 'q', 'H', 'G',
};

constexpr auto kCommonInputIndexes = [] {
    std::array<std::uint8_t, 256> indexes{};
    for (std::size_t index = 0; index < kCommonInputs.size(); ++index) {
        indexes[kCommonInputs[index]] = static_cast<std::uint8_t>(index + 1);
    }
    return indexes;
}();

void
ValidateUtf8(std::string_view text) {
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
        i += length;
    }
}

void
WriteU32(std::vector<std::uint8_t>& data, std::uint32_t value) {
    for (std::size_t shift = 0; shift < 4; ++shift) {
        data.push_back(static_cast<std::uint8_t>(value >> (shift * 8)));
    }
}

void
WriteU64(std::vector<std::uint8_t>& data, std::uint64_t value) {
    for (std::size_t shift = 0; shift < 8; ++shift) {
        data.push_back(static_cast<std::uint8_t>(value >> (shift * 8)));
    }
}

std::uint32_t
ReadU32(std::span<const std::uint8_t> data, std::size_t offset) {
    if (offset > data.size() || data.size() - offset < 4) {
        throw std::runtime_error("truncated text FST u32");
    }
    std::uint32_t value = 0;
    for (std::size_t shift = 0; shift < 4; ++shift) {
        value |= static_cast<std::uint32_t>(data[offset + shift])
                 << (shift * 8);
    }
    return value;
}

std::uint64_t
ReadU64(std::span<const std::uint8_t> data, std::size_t offset) {
    if (offset > data.size() || data.size() - offset < 8) {
        throw std::runtime_error("truncated text FST u64");
    }
    std::uint64_t value = 0;
    for (std::size_t shift = 0; shift < 8; ++shift) {
        value |= static_cast<std::uint64_t>(data[offset + shift])
                 << (shift * 8);
    }
    return value;
}

std::uint8_t
PackSize(std::uint64_t value) {
    for (std::uint8_t bytes = 1; bytes < 8; ++bytes) {
        if (value < (std::uint64_t{1} << (bytes * 8))) {
            return bytes;
        }
    }
    return 8;
}

void
PackUIntIn(std::vector<std::uint8_t>& data,
           std::uint64_t value,
           std::uint8_t bytes) {
    if (bytes == 0 || bytes > 8 || bytes < PackSize(value)) {
        throw std::logic_error("invalid text FST packed integer width");
    }
    for (std::uint8_t index = 0; index < bytes; ++index) {
        data.push_back(static_cast<std::uint8_t>(value >> (index * 8)));
    }
}

std::uint8_t
PackUInt(std::vector<std::uint8_t>& data, std::uint64_t value) {
    const auto bytes = PackSize(value);
    PackUIntIn(data, value, bytes);
    return bytes;
}

std::uint64_t
UnpackUInt(std::span<const std::uint8_t> data,
           std::size_t offset,
           std::size_t bytes) {
    if (bytes == 0 || bytes > 8 || offset > data.size() ||
        data.size() - offset < bytes) {
        throw std::runtime_error("invalid text FST packed integer");
    }
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < bytes; ++index) {
        value |= static_cast<std::uint64_t>(data[offset + index])
                 << (index * 8);
    }
    return value;
}

std::uint32_t
MaskedCrc32c(const std::uint8_t* bytes, std::size_t length) {
    const auto crc = crc32c::Crc32c(bytes, length);
    return ((crc >> 15) | (crc << 17)) + 0xA282EAD8U;
}

std::uint8_t
CommonIndex(std::uint8_t input) {
    return kCommonInputIndexes[input];
}

std::optional<std::uint8_t>
CommonInput(std::uint8_t index) {
    if (index == 0) {
        return std::nullopt;
    }
    if (index > kCommonInputs.size()) {
        throw std::runtime_error("invalid text FST common-input index");
    }
    return kCommonInputs[index - 1];
}

struct Transition {
    std::uint8_t input = 0;
    Output output = 0;
    Address address = kNoneAddress;

    bool
    operator==(const Transition&) const = default;
};

struct BuilderNode {
    bool is_final = false;
    Output final_output = 0;
    std::vector<Transition> transitions;

    bool
    operator==(const BuilderNode&) const = default;
};

struct LastTransition {
    std::uint8_t input = 0;
    Output output = 0;
};

struct UnfinishedNode {
    BuilderNode node;
    std::optional<LastTransition> last;

    void
    LastCompiled(Address address) {
        if (last.has_value()) {
            node.transitions.push_back(
                Transition{last->input, last->output, address});
            last.reset();
        }
    }

    void
    AddOutputPrefix(Output prefix) {
        if (node.is_final) {
            node.final_output += prefix;
        }
        for (auto& transition : node.transitions) {
            transition.output += prefix;
        }
        if (last.has_value()) {
            last->output += prefix;
        }
    }
};

class UnfinishedNodes {
 public:
    UnfinishedNodes() {
        stack_.push_back(UnfinishedNode{});
    }

    std::size_t
    Size() const {
        return stack_.size();
    }

    BuilderNode
    PopRoot() {
        if (stack_.size() != 1 || stack_.front().last.has_value()) {
            throw std::logic_error("invalid unfinished text FST root");
        }
        auto root = std::move(stack_.front().node);
        stack_.clear();
        return root;
    }

    BuilderNode
    PopFreeze(Address address) {
        auto unfinished = std::move(stack_.back());
        stack_.pop_back();
        unfinished.LastCompiled(address);
        return std::move(unfinished.node);
    }

    BuilderNode
    PopEmpty() {
        auto unfinished = std::move(stack_.back());
        stack_.pop_back();
        if (unfinished.last.has_value()) {
            throw std::logic_error("expected empty unfinished FST node");
        }
        return std::move(unfinished.node);
    }

    void
    TopLastFreeze(Address address) {
        if (stack_.empty()) {
            throw std::logic_error("missing unfinished FST node");
        }
        stack_.back().LastCompiled(address);
    }

    void
    SetRootOutput(Output output) {
        stack_.front().node.is_final = true;
        stack_.front().node.final_output = output;
    }

    std::pair<std::size_t, Output>
    FindCommonPrefixAndSetOutput(std::string_view term, Output output) {
        std::size_t index = 0;
        while (index < term.size()) {
            auto& unfinished = stack_.at(index);
            if (!unfinished.last.has_value() ||
                unfinished.last->input !=
                    static_cast<std::uint8_t>(term[index])) {
                break;
            }
            ++index;
            auto& transition_output = unfinished.last->output;
            const auto common = std::min(transition_output, output);
            const auto add_prefix = transition_output - common;
            output -= common;
            transition_output = common;
            if (add_prefix != 0) {
                stack_.at(index).AddOutputPrefix(add_prefix);
            }
        }
        return {index, output};
    }

    void
    AddSuffix(std::string_view suffix, Output output) {
        if (suffix.empty()) {
            return;
        }
        if (stack_.back().last.has_value()) {
            throw std::logic_error(
                "unfinished FST node already has a last transition");
        }
        stack_.back().last =
            LastTransition{static_cast<std::uint8_t>(suffix.front()), output};
        for (std::size_t index = 1; index < suffix.size(); ++index) {
            UnfinishedNode node;
            node.last =
                LastTransition{static_cast<std::uint8_t>(suffix[index]), 0};
            stack_.push_back(std::move(node));
        }
        UnfinishedNode final;
        final.node.is_final = true;
        stack_.push_back(std::move(final));
    }

 private:
    std::vector<UnfinishedNode> stack_;
};

struct RegistryCell {
    Address address = kNoneAddress;
    BuilderNode node;
};

struct RegistryResult {
    std::optional<Address> found;
    RegistryCell* cell = nullptr;
};

class Registry {
 public:
    Registry() : cells_(10'000 * 2) {
    }

    RegistryResult
    Entry(const BuilderNode& node) {
        const auto bucket = Hash(node) % 10'000;
        auto& first = cells_[bucket * 2];
        auto& second = cells_[bucket * 2 + 1];
        if (first.address != kNoneAddress && first.node == node) {
            return RegistryResult{first.address, nullptr};
        }
        if (second.address != kNoneAddress && second.node == node) {
            const auto address = second.address;
            std::swap(first, second);
            return RegistryResult{address, nullptr};
        }
        second.node = node;
        std::swap(first, second);
        return RegistryResult{std::nullopt, &first};
    }

 private:
    static std::uint64_t
    Hash(const BuilderNode& node) {
        constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
        constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
        std::uint64_t hash = kFnvOffset;
        auto mix = [&](std::uint64_t value) {
            hash = (hash ^ value) * kFnvPrime;
        };
        mix(node.is_final ? 1 : 0);
        mix(node.final_output);
        for (const auto& transition : node.transitions) {
            mix(transition.input);
            mix(transition.output);
            mix(transition.address);
        }
        return hash;
    }

    std::vector<RegistryCell> cells_;
};

std::uint8_t
PackDeltaSize(Address node_address, Address transition_address) {
    const auto delta = transition_address == kEmptyAddress
                           ? kEmptyAddress
                           : node_address - transition_address;
    return PackSize(delta);
}

void
PackDeltaIn(std::vector<std::uint8_t>& data,
            Address node_address,
            Address transition_address,
            std::uint8_t bytes) {
    const auto delta = transition_address == kEmptyAddress
                           ? kEmptyAddress
                           : node_address - transition_address;
    PackUIntIn(data, delta, bytes);
}

void
CompileOneTransitionNext(std::vector<std::uint8_t>& data, std::uint8_t input) {
    const auto common = CommonIndex(input);
    if (common == 0) {
        data.push_back(input);
    }
    data.push_back(static_cast<std::uint8_t>(0xC0U | common));
}

void
CompileOneTransition(std::vector<std::uint8_t>& data,
                     Address node_address,
                     const Transition& transition) {
    const auto output_size =
        transition.output == 0 ? 0 : PackUInt(data, transition.output);
    const auto transition_size =
        PackDeltaSize(node_address, transition.address);
    PackDeltaIn(data, node_address, transition.address, transition_size);
    data.push_back(
        static_cast<std::uint8_t>((transition_size << 4) | output_size));
    const auto common = CommonIndex(transition.input);
    if (common == 0) {
        data.push_back(transition.input);
    }
    data.push_back(static_cast<std::uint8_t>(0x80U | common));
}

void
CompileAnyTransition(std::vector<std::uint8_t>& data,
                     Address node_address,
                     const BuilderNode& node) {
    if (node.transitions.size() > 256) {
        throw std::logic_error("text FST node has more than 256 transitions");
    }
    std::uint8_t transition_size = 0;
    std::uint8_t output_size = PackSize(node.final_output);
    bool any_outputs = node.final_output != 0;
    for (const auto& transition : node.transitions) {
        transition_size = std::max(
            transition_size, PackDeltaSize(node_address, transition.address));
        output_size = std::max(output_size, PackSize(transition.output));
        any_outputs = any_outputs || transition.output != 0;
    }
    if (!any_outputs) {
        output_size = 0;
    }
    if (any_outputs) {
        if (node.is_final) {
            PackUIntIn(data, node.final_output, output_size);
        }
        for (auto transition = node.transitions.rbegin();
             transition != node.transitions.rend();
             ++transition) {
            PackUIntIn(data, transition->output, output_size);
        }
    }
    for (auto transition = node.transitions.rbegin();
         transition != node.transitions.rend();
         ++transition) {
        PackDeltaIn(data, node_address, transition->address, transition_size);
    }
    for (auto transition = node.transitions.rbegin();
         transition != node.transitions.rend();
         ++transition) {
        data.push_back(transition->input);
    }
    if (node.transitions.size() > kTransitionIndexThreshold) {
        std::array<std::uint8_t, 256> index{};
        index.fill(255);
        for (std::size_t i = 0; i < node.transitions.size(); ++i) {
            index[node.transitions[i].input] = static_cast<std::uint8_t>(i);
        }
        data.insert(data.end(), index.begin(), index.end());
    }
    data.push_back(
        static_cast<std::uint8_t>((transition_size << 4) | output_size));
    const bool external_count =
        node.transitions.empty() || node.transitions.size() > 63;
    if (external_count) {
        data.push_back(
            node.transitions.size() == 256
                ? 1
                : static_cast<std::uint8_t>(node.transitions.size()));
    }
    auto state = static_cast<std::uint8_t>(node.is_final ? 0x40U : 0U);
    if (!external_count) {
        state |= static_cast<std::uint8_t>(node.transitions.size());
    }
    data.push_back(state);
}

class Builder {
 public:
    Builder() {
        WriteU64(data_, kVersion);
        WriteU64(data_, kFstType);
    }

    void
    Insert(std::string_view term, Output output) {
        if (last_.has_value() && term == *last_) {
            throw std::invalid_argument("duplicate term: " + std::string(term));
        }
        if (last_.has_value() && term < *last_) {
            throw std::invalid_argument("text FST input must be sorted");
        }
        last_ = std::string(term);
        if (term.empty()) {
            length_ = 1;
            unfinished_.SetRootOutput(output);
            return;
        }
        auto [prefix_length, remaining_output] =
            unfinished_.FindCommonPrefixAndSetOutput(term, output);
        if (prefix_length == term.size()) {
            throw std::logic_error(
                "duplicate output term passed FST validation");
        }
        ++length_;
        CompileFrom(prefix_length);
        unfinished_.AddSuffix(term.substr(prefix_length), remaining_output);
    }

    std::vector<std::uint8_t>
    Finish() {
        CompileFrom(0);
        auto root = unfinished_.PopRoot();
        const auto root_address = Compile(root);
        WriteU64(data_, length_);
        WriteU64(data_, root_address);
        const auto checksum = MaskedCrc32c(data_.data(), data_.size());
        WriteU32(data_, checksum);
        return std::move(data_);
    }

 private:
    void
    CompileFrom(std::size_t state) {
        Address address = kNoneAddress;
        while (state + 1 < unfinished_.Size()) {
            auto node = address == kNoneAddress
                            ? unfinished_.PopEmpty()
                            : unfinished_.PopFreeze(address);
            address = Compile(node);
        }
        unfinished_.TopLastFreeze(address);
    }

    Address
    Compile(const BuilderNode& node) {
        if (node.is_final && node.transitions.empty() &&
            node.final_output == 0) {
            return kEmptyAddress;
        }
        auto registry = registry_.Entry(node);
        if (registry.found.has_value()) {
            return *registry.found;
        }
        const auto start = data_.size();
        if (node.transitions.size() == 1 && !node.is_final) {
            const auto& transition = node.transitions.front();
            if (transition.address == last_address_ && transition.output == 0) {
                CompileOneTransitionNext(data_, transition.input);
            } else {
                CompileOneTransition(data_, start, transition);
            }
        } else {
            CompileAnyTransition(data_, start, node);
        }
        last_address_ = data_.size() - 1;
        registry.cell->address = last_address_;
        return last_address_;
    }

    std::vector<std::uint8_t> data_;
    UnfinishedNodes unfinished_;
    Registry registry_;
    std::optional<std::string> last_;
    Address last_address_ = kNoneAddress;
    std::size_t length_ = 0;
};

struct Metadata {
    std::size_t term_count = 0;
    Address root_address = kEmptyAddress;
    std::uint32_t checksum = 0;
};

Metadata
ReadMetadata(std::span<const std::uint8_t> data) {
    if (data.size() < 36 || ReadU64(data, 0) != kVersion ||
        ReadU64(data, 8) != kFstType) {
        throw std::runtime_error("invalid text FST version or header");
    }
    const auto checksum_offset = data.size() - 4;
    const auto root = ReadU64(data, checksum_offset - 8);
    const auto length = ReadU64(data, checksum_offset - 16);
    if (root > std::numeric_limits<Address>::max() ||
        length > std::numeric_limits<std::size_t>::max()) {
        throw std::runtime_error("text FST metadata overflows this platform");
    }
    const auto root_address = static_cast<Address>(root);
    if (root_address != kEmptyAddress &&
        root_address + kRootAddressOffset != data.size()) {
        throw std::runtime_error("invalid text FST root address");
    }
    return Metadata{
        .term_count = static_cast<std::size_t>(length),
        .root_address = root_address,
        .checksum = ReadU32(data, checksum_offset),
    };
}

enum class NodeKind { kEmptyFinal, kOneTransitionNext, kOneTransition, kAny };

struct NodeView {
    struct DecodedTransition {
        std::uint8_t input = 0;
        Output output = 0;
        Address address = kNoneAddress;
    };

    std::span<const std::uint8_t> data;
    Address address = kEmptyAddress;
    Address end = kEmptyAddress;
    NodeKind kind = NodeKind::kEmptyFinal;
    bool is_final = true;
    std::size_t transition_count = 0;
    std::size_t transition_size = 0;
    std::size_t output_size = 0;
    std::size_t input_size = 0;
    std::size_t count_size = 0;
    Output final_output = 0;

    static NodeView
    Read(std::span<const std::uint8_t> bytes, Address address) {
        if (address == kEmptyAddress) {
            return NodeView{.data = bytes};
        }
        if (address >= bytes.size() - kTrailerBytes) {
            throw std::runtime_error("text FST node address is out of range");
        }
        NodeView node;
        node.data = bytes;
        node.address = address;
        const auto state = bytes[address];
        const auto kind = (state & 0xC0U) >> 6;
        if (kind == 3) {
            node.kind = NodeKind::kOneTransitionNext;
            node.is_final = false;
            node.transition_count = 1;
            node.input_size = (state & 0x3FU) == 0 ? 1 : 0;
            if (address < node.input_size) {
                throw std::runtime_error("truncated text FST OTN node");
            }
            node.end = address - node.input_size;
            return node;
        }
        if (kind == 2) {
            node.kind = NodeKind::kOneTransition;
            node.is_final = false;
            node.transition_count = 1;
            node.input_size = (state & 0x3FU) == 0 ? 1 : 0;
            if (address < node.input_size + 1) {
                throw std::runtime_error("truncated text FST OT node");
            }
            const auto sizes = bytes[address - node.input_size - 1];
            node.transition_size = sizes >> 4;
            node.output_size = sizes & 0x0FU;
            const auto body =
                node.input_size + 1 + node.transition_size + node.output_size;
            if (node.transition_size == 0 || node.transition_size > 8 ||
                node.output_size > 8 || address < body) {
                throw std::runtime_error("invalid text FST OT packed sizes");
            }
            node.end = address - body;
            return node;
        }

        node.kind = NodeKind::kAny;
        node.is_final = (state & 0x40U) != 0;
        const auto inline_count = state & 0x3FU;
        node.count_size = inline_count == 0 ? 1 : 0;
        if (address < node.count_size + 1) {
            throw std::runtime_error("truncated text FST AnyTrans node");
        }
        if (inline_count != 0) {
            node.transition_count = inline_count;
        } else {
            const auto encoded = bytes[address - 1];
            node.transition_count = encoded == 1 ? 256 : encoded;
        }
        const auto sizes = bytes[address - node.count_size - 1];
        node.transition_size = sizes >> 4;
        node.output_size = sizes & 0x0FU;
        const auto index_size =
            node.transition_count > kTransitionIndexThreshold ? 256 : 0;
        const auto transition_body =
            index_size + node.transition_count +
            node.transition_count * node.transition_size;
        const auto output_body = node.transition_count * node.output_size +
                                 (node.is_final ? node.output_size : 0);
        const auto body = node.count_size + 1 + transition_body + output_body;
        if (node.transition_size > 8 || node.output_size > 8 ||
            address < body) {
            throw std::runtime_error("invalid text FST AnyTrans packed sizes");
        }
        node.end = address - body;
        if (node.is_final && node.output_size != 0) {
            node.final_output = UnpackUInt(bytes, node.end, node.output_size);
        }
        return node;
    }

    std::uint8_t
    Input(std::size_t index) const {
        if (index >= transition_count || kind == NodeKind::kEmptyFinal) {
            throw std::out_of_range("text FST transition index");
        }
        const auto bytes = data;
        if (kind == NodeKind::kOneTransitionNext ||
            kind == NodeKind::kOneTransition) {
            const auto common = CommonInput(bytes[address] & 0x3FU);
            return common.has_value() ? *common : bytes[address - 1];
        }
        const auto index_size =
            transition_count > kTransitionIndexThreshold ? 256 : 0;
        return bytes[address - count_size - 1 - index_size - index - 1];
    }

    std::optional<std::size_t>
    FindInput(std::uint8_t input) const {
        if (kind == NodeKind::kEmptyFinal) {
            return std::nullopt;
        }
        if (kind != NodeKind::kAny) {
            return Input(0) == input ? std::optional<std::size_t>(0)
                                     : std::nullopt;
        }
        const auto bytes = data;
        if (transition_count > kTransitionIndexThreshold) {
            const auto start = address - count_size - 1 - 256;
            const auto index = static_cast<std::size_t>(bytes[start + input]);
            return index < transition_count ? std::optional<std::size_t>(index)
                                            : std::nullopt;
        }
        const auto start = address - count_size - 1 - transition_count;
        for (std::size_t offset = 0; offset < transition_count; ++offset) {
            if (bytes[start + offset] == input) {
                return transition_count - offset - 1;
            }
        }
        return std::nullopt;
    }

    Address
    TransitionAddress(std::size_t index) const {
        if (index >= transition_count || kind == NodeKind::kEmptyFinal) {
            throw std::out_of_range("text FST transition index");
        }
        if (kind == NodeKind::kOneTransitionNext) {
            if (end == 0) {
                throw std::runtime_error("invalid text FST OTN target");
            }
            return end - 1;
        }
        const auto bytes = data;
        std::size_t offset = 0;
        if (kind == NodeKind::kOneTransition) {
            offset = address - input_size - 1 - transition_size;
        } else {
            const auto index_size =
                transition_count > kTransitionIndexThreshold ? 256 : 0;
            offset = address - count_size - 1 - index_size - transition_count -
                     index * transition_size - transition_size;
        }
        const auto delta = UnpackUInt(bytes, offset, transition_size);
        if (delta == kEmptyAddress) {
            return kEmptyAddress;
        }
        if (delta > end) {
            throw std::runtime_error("invalid text FST transition delta");
        }
        return end - static_cast<Address>(delta);
    }

    Output
    TransitionOutput(std::size_t index) const {
        if (index >= transition_count || output_size == 0 ||
            kind == NodeKind::kOneTransitionNext) {
            return 0;
        }
        const auto bytes = data;
        std::size_t offset = 0;
        if (kind == NodeKind::kOneTransition) {
            offset = address - input_size - 1 - transition_size - output_size;
        } else {
            const auto index_size =
                transition_count > kTransitionIndexThreshold ? 256 : 0;
            const auto total_transition_size =
                index_size + transition_count +
                transition_count * transition_size;
            offset = address - count_size - 1 - total_transition_size -
                     index * output_size - output_size;
        }
        return UnpackUInt(bytes, offset, output_size);
    }

    // This is the direct counterpart of upstream Node::transition. In
    // particular, dispatch on the compiled node kind exactly once and decode
    // input/output/address together. Upstream marks this operation
    // #[inline(always)] because it is on the hottest stream traversal path.
    [[gnu::always_inline]] inline DecodedTransition
    FullTransition(std::size_t index) const {
        if (index >= transition_count || kind == NodeKind::kEmptyFinal) {
            throw std::out_of_range("text FST transition index");
        }
        const auto bytes = data;
        switch (kind) {
            case NodeKind::kOneTransitionNext: {
                if (end == 0) {
                    throw std::runtime_error("invalid text FST OTN target");
                }
                const auto common = CommonInput(bytes[address] & 0x3FU);
                return DecodedTransition{
                    .input = common.has_value() ? *common : bytes[address - 1],
                    .output = 0,
                    .address = end - 1,
                };
            }
            case NodeKind::kOneTransition: {
                const auto common = CommonInput(bytes[address] & 0x3FU);
                const auto input =
                    common.has_value() ? *common : bytes[address - 1];
                const auto address_offset =
                    address - input_size - 1 - transition_size;
                const auto delta =
                    UnpackUInt(bytes, address_offset, transition_size);
                if (delta != kEmptyAddress && delta > end) {
                    throw std::runtime_error(
                        "invalid text FST transition delta");
                }
                const auto output =
                    output_size == 0
                        ? 0
                        : UnpackUInt(
                              bytes, address_offset - output_size, output_size);
                return DecodedTransition{
                    .input = input,
                    .output = output,
                    .address = delta == kEmptyAddress
                                   ? kEmptyAddress
                                   : end - static_cast<Address>(delta),
                };
            }
            case NodeKind::kAny: {
                const auto index_size =
                    transition_count > kTransitionIndexThreshold ? 256 : 0;
                const auto input_offset =
                    address - count_size - 1 - index_size - index - 1;
                const auto address_offset =
                    address - count_size - 1 - index_size - transition_count -
                    index * transition_size - transition_size;
                const auto delta =
                    UnpackUInt(bytes, address_offset, transition_size);
                if (delta != kEmptyAddress && delta > end) {
                    throw std::runtime_error(
                        "invalid text FST transition delta");
                }
                Output output = 0;
                if (output_size != 0) {
                    const auto total_transition_size =
                        index_size + transition_count +
                        transition_count * transition_size;
                    const auto output_offset =
                        address - count_size - 1 - total_transition_size -
                        index * output_size - output_size;
                    output = UnpackUInt(bytes, output_offset, output_size);
                }
                return DecodedTransition{
                    .input = bytes[input_offset],
                    .output = output,
                    .address = delta == kEmptyAddress
                                   ? kEmptyAddress
                                   : end - static_cast<Address>(delta),
                };
            }
            case NodeKind::kEmptyFinal:
                break;
        }
        throw std::logic_error("unreachable text FST node kind");
    }
};

}  // namespace

struct TextFst::Impl {
    std::vector<std::uint8_t> owned_data;
    std::span<const std::uint8_t> data;
    Metadata metadata;
};

TextFst::TextFst() : impl_(std::make_unique<Impl>()) {
}

TextFst::~TextFst() = default;

TextFst::TextFst(TextFst&&) noexcept = default;

TextFst&
TextFst::operator=(TextFst&&) noexcept = default;

void
TextFst::Build(const TextFstTermReader& reader) {
    Builder builder;
    while (const auto term = reader()) {
        if (term->empty()) {
            throw std::invalid_argument("empty terms are not supported");
        }
        ValidateUtf8(*term);
        builder.Insert(*term, 1);
    }

    auto owned_data = builder.Finish();
    const auto metadata = ReadMetadata(owned_data);
    impl_->owned_data = std::move(owned_data);
    impl_->data = impl_->owned_data;
    impl_->metadata = metadata;
}

std::size_t
TextFst::TermCount() const {
    return impl_->metadata.term_count;
}

std::size_t
TextFst::DataSize() const {
    return impl_->data.size();
}

std::span<const std::uint8_t>
TextFst::SerializedBytes() const {
    return impl_->data;
}

bool
TextFst::VerifyChecksum() const {
    if (impl_->data.size() < 4) {
        return false;
    }
    return ReadU32(impl_->data, impl_->data.size() - 4) ==
           MaskedCrc32c(impl_->data.data(), impl_->data.size() - 4);
}

}  // namespace milvus::textindex
