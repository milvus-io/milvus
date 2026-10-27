// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include "mmap/detail/PinnedStringViews.h"

#include <limits>

#include "common/Chunk.h"

namespace milvus::detail::storage {
namespace {

template <bool WriteState>
void
GatherViews(const StringChunk* const* chunks,
            const std::size_t* row_offsets,
            std::int64_t count,
            std::string_view* output,
            bool* validity,
            bool* skipped) {
    for (std::int64_t lane = 0; lane < count; ++lane) {
        AssertInfo(chunks[lane] != nullptr,
                   "borrowed string batch chunk is null at lane {}",
                   lane);
        AssertInfo(row_offsets[lane] <= static_cast<std::size_t>(
                                            std::numeric_limits<int>::max()),
                   "borrowed string row {} exceeds the getter domain",
                   row_offsets[lane]);
        output[lane] = (*chunks[lane])[static_cast<int>(row_offsets[lane])];
        if constexpr (WriteState) {
            validity[lane] = true;
            skipped[lane] = false;
        }
        if (!output[lane].empty()) {
            __builtin_prefetch(output[lane].data(), 0, 1);
        }
    }
}
}  // namespace

void
GatherPinnedStringViews(const StringChunk* const* chunks,
                        const std::size_t* row_offsets,
                        std::int64_t count,
                        std::string_view* output,
                        bool* validity,
                        bool* skipped) {
    AssertInfo(count >= 0, "borrowed string batch count must be non-negative");
    if (count == 0) {
        return;
    }
    AssertInfo(chunks != nullptr && row_offsets != nullptr && output != nullptr,
               "non-empty borrowed string batch requires input and output");
    AssertInfo((validity == nullptr) == (skipped == nullptr),
               "borrowed string state outputs must be supplied together");
    // Select storage output shape once, never a predicate operation.
    if (validity != nullptr) {
        GatherViews<true>(
            chunks, row_offsets, count, output, validity, skipped);
    } else {
        GatherViews<false>(
            chunks, row_offsets, count, output, nullptr, nullptr);
    }
}

}  // namespace milvus::detail::storage
