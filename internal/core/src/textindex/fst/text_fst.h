#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace milvus::textindex {

struct TextFstMatch {
    std::string term;
    std::uint32_t edit_distance;

    bool
    operator==(const TextFstMatch&) const = default;
};

struct TextFstSearchResult {
    std::vector<TextFstMatch> matches;
    std::size_t work_used = 0;
};

// Returns the next strictly byte-sorted term, or nullopt at end of stream.
// The returned view only needs to remain valid until the next reader call.
using TextFstTermReader = std::function<std::optional<std::string_view>()>;
using TextFstTermVisitor = std::function<void(std::string_view)>;

class TextFst {
 public:
    TextFst();
    ~TextFst();

    TextFst(const TextFst&) = delete;
    TextFst&
    operator=(const TextFst&) = delete;
    TextFst(TextFst&&) noexcept;
    TextFst&
    operator=(TextFst&&) noexcept;

    void
    Build(const TextFstTermReader& reader);

    [[nodiscard]] TextFstSearchResult
    FuzzySearch(std::string_view query,
                std::uint32_t max_edit_distance,
                std::size_t max_expansions) const;

    void
    LoadFile(const std::string& path, bool memory_mapped);
    void
    LoadBytes(std::span<const std::uint8_t> bytes);
    void
    VisitTerms(const TextFstTermVisitor& visitor) const;

    [[nodiscard]] std::size_t
    TermCount() const;
    [[nodiscard]] std::size_t
    DataSize() const;
    [[nodiscard]] bool
    IsMemoryMapped() const;
    [[nodiscard]] std::span<const std::uint8_t>
    SerializedBytes() const;
    [[nodiscard]] bool
    VerifyChecksum() const;

 private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::textindex
