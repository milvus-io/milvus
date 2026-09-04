#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace milvus::textindex {

// Returns the next strictly byte-sorted term, or nullopt at end of stream.
// The returned view only needs to remain valid until the next reader call.
using TextFstTermReader = std::function<std::optional<std::string_view>()>;

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

    [[nodiscard]] std::size_t
    TermCount() const;
    [[nodiscard]] std::size_t
    DataSize() const;
    [[nodiscard]] std::span<const std::uint8_t>
    SerializedBytes() const;
    [[nodiscard]] bool
    VerifyChecksum() const;

 private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::textindex
