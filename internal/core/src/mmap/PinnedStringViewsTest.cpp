// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <vector>

#include "common/Chunk.h"
#include "mmap/detail/PinnedStringViews.h"
#include "test_utils/OriginalNqTextFixture.h"

namespace milvus {
namespace {

struct PinnedOriginalTextChunk {
    explicit PinnedOriginalTextChunk(const std::vector<OriginalNqTextRow>& rows,
                                     bool title) {
        size_t size = (rows.size() + 1) * sizeof(uint32_t);
        for (const auto& row : rows) {
            size += (title ? row.title : row.text).size();
        }
        bytes.resize(size);
        uint32_t position = (rows.size() + 1) * sizeof(uint32_t);
        std::memcpy(bytes.data(), &position, sizeof(position));
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& value = title ? rows[i].title : rows[i].text;
            std::memcpy(bytes.data() + position, value.data(), value.size());
            position += value.size();
            std::memcpy(bytes.data() + (i + 1) * sizeof(position),
                        &position,
                        sizeof(position));
        }
        chunk = std::make_unique<StringChunk>(
            rows.size(),
            bytes.data(),
            bytes.size(),
            false,
            std::make_shared<ChunkMmapGuard>(nullptr, 0, ""));
    }

    std::vector<char> bytes;
    std::unique_ptr<StringChunk> chunk;
};

TEST(TakeContextFoundation,
     PinnedStringViewsPreserveOriginalOrderAndLargeTails) {
    const auto rows = LoadOriginalNqTextRows();
    PinnedOriginalTextChunk title(rows, true), text(rows, false);
    // Only request order changes. The two columns contain the same unchanged
    // original NQ rows; larger batches do not inflate the dataset.
    for (size_t count : {size_t{1},
                         size_t{31},
                         size_t{32},
                         size_t{64},
                         size_t{65},
                         size_t{257},
                         rows.size()}) {
        std::vector<const StringChunk*> chunks(count);
        std::vector<size_t> offsets(count);
        std::vector<std::string_view> output(count);
        for (size_t lane = 0; lane < count; ++lane) {
            offsets[lane] = (lane * 73 + 11) % rows.size();
            chunks[lane] = lane % 2 == 0 ? title.chunk.get() : text.chunk.get();
        }
        if (count > 1) {
            offsets.back() = offsets.front();  // Duplicate requested row.
            chunks.back() = chunks.front();
        }
        detail::storage::GatherPinnedStringViews(
            chunks.data(), offsets.data(), count, output.data());
        for (size_t lane = 0; lane < count; ++lane) {
            const auto& expected = chunks[lane] == title.chunk.get()
                                       ? rows[offsets[lane]].title
                                       : rows[offsets[lane]].text;
            EXPECT_EQ(output[lane], expected);
            // Borrow the original backing, not a copied/temporary value.
            EXPECT_EQ(output[lane].data(),
                      (*chunks[lane])[offsets[lane]].data());
        }
        FixedVector<bool> validity(count), skipped(count);
        std::fill_n(validity.data(), count, false);
        std::fill_n(skipped.data(), count, true);
        detail::storage::GatherPinnedStringViews(chunks.data(),
                                                 offsets.data(),
                                                 count,
                                                 output.data(),
                                                 validity.data(),
                                                 skipped.data());
        for (size_t lane = 0; lane < count; ++lane) {
            EXPECT_TRUE(validity[lane]);
            EXPECT_FALSE(skipped[lane]);
            EXPECT_EQ(output[lane],
                      chunks[lane] == title.chunk.get()
                          ? rows[offsets[lane]].title
                          : rows[offsets[lane]].text);
            EXPECT_EQ(output[lane].data(),
                      (*chunks[lane])[offsets[lane]].data());
        }
    }
}

TEST(TakeContextFoundation, PinnedStringViewsRejectInvalidMetadataAndRecover) {
    using detail::storage::GatherPinnedStringViews;
    const auto rows = LoadOriginalNqTextRows();
    PinnedOriginalTextChunk source(rows, false);
    const StringChunk* chunks[] = {source.chunk.get(), source.chunk.get()};
    size_t offsets[] = {0, rows.size() - 1};
    std::string_view output[2];
    EXPECT_NO_THROW(GatherPinnedStringViews(nullptr, nullptr, 0, nullptr));
    EXPECT_ANY_THROW(GatherPinnedStringViews(nullptr, nullptr, -1, nullptr));
    EXPECT_ANY_THROW(GatherPinnedStringViews(nullptr, offsets, 1, output));
    EXPECT_ANY_THROW(GatherPinnedStringViews(chunks, nullptr, 1, output));
    EXPECT_ANY_THROW(GatherPinnedStringViews(chunks, offsets, 1, nullptr));
    bool validity[2], skipped[2];
    EXPECT_ANY_THROW(
        GatherPinnedStringViews(chunks, offsets, 1, output, validity, nullptr));
    EXPECT_ANY_THROW(
        GatherPinnedStringViews(chunks, offsets, 1, output, nullptr, skipped));
    chunks[1] = nullptr;
    EXPECT_ANY_THROW(GatherPinnedStringViews(chunks, offsets, 2, output));
    chunks[1] = source.chunk.get();
    for (size_t invalid :
         {rows.size(),
          static_cast<size_t>(std::numeric_limits<int>::max()) + 1,
          std::numeric_limits<size_t>::max()}) {
        offsets[1] = invalid;
        EXPECT_ANY_THROW(GatherPinnedStringViews(chunks, offsets, 2, output));
    }
    offsets[1] = rows.size() - 1;
    EXPECT_NO_THROW(GatherPinnedStringViews(chunks, offsets, 2, output));
    EXPECT_EQ(output[0], rows.front().text);
    EXPECT_EQ(output[1], rows.back().text);
    EXPECT_NO_THROW(
        GatherPinnedStringViews(chunks, offsets, 2, output, validity, skipped));
    EXPECT_TRUE(validity[0] && validity[1]);
    EXPECT_FALSE(skipped[0] || skipped[1]);
}

}  // namespace
}  // namespace milvus
