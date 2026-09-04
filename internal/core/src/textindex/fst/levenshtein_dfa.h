#pragma once

#include <array>
#include <cstdint>
#include <string_view>
#include <vector>

namespace milvus::textindex {

class LevenshteinDfa {
 public:
    [[nodiscard]] std::uint32_t
    InitialState() const;
    [[nodiscard]] std::uint32_t
    Transition(std::uint32_t state, std::uint8_t byte) const;
    [[nodiscard]] bool
    IsMatch(std::uint32_t state) const;
    [[nodiscard]] bool
    CanMatch(std::uint32_t state) const;
    [[nodiscard]] std::uint32_t
    Distance(std::uint32_t state) const;

 private:
    friend LevenshteinDfa BuildLevenshteinDfa(std::string_view, std::uint32_t);

    std::vector<std::array<std::uint32_t, 256>> transitions_;
    std::vector<std::uint8_t> distances_;
    std::uint32_t initial_state_ = 0;
    std::uint8_t max_distance_ = 0;
};

[[nodiscard]] LevenshteinDfa
BuildLevenshteinDfa(std::string_view query, std::uint32_t max_distance);

void
ValidateUtf8(std::string_view text);

}  // namespace milvus::textindex
