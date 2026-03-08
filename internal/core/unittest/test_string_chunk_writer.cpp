// Copyright (C) 2019-2025 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0

#include <gtest/gtest.h>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"

#include "common/Chunk.h"
#include "common/ChunkWriter.h"

using milvus::StringChunk;
using milvus::StringChunkWriter;

namespace {

std::shared_ptr<arrow::BinaryArray>
BuildBinaryArray(const std::vector<std::optional<std::string>>& values) {
    arrow::BinaryBuilder builder;
    for (const auto& v : values) {
        if (v.has_value()) {
            auto st =
                builder.Append(v->data(), static_cast<int32_t>(v->size()));
            ASSERT_TRUE(st.ok());
        } else {
            auto st = builder.AppendNull();
            ASSERT_TRUE(st.ok());
        }
    }
    std::shared_ptr<arrow::Array> arr;
    auto st = builder.Finish(&arr);
    EXPECT_TRUE(st.ok());
    return std::static_pointer_cast<arrow::BinaryArray>(arr);
}

}  // namespace

TEST(StringChunkWriterTest, NoNullsMultiBatches) {
    // Prepare two batches without nulls
    std::vector<std::optional<std::string>> b1 = {std::string("a"),
                                                  std::string(""),
                                                  std::string("hello"),
                                                  std::string("world"),
                                                  std::string("foobar")};
    std::vector<std::optional<std::string>> b2;
    for (int i = 0; i < 100; ++i) {
        b2.emplace_back(std::string(1 + (i % 7), 'x' + (i % 3)));
    }

    auto a1 = BuildBinaryArray(b1);
    auto a2 = BuildBinaryArray(b2);

    arrow::ArrayVector vec{a1, a2};

    StringChunkWriter writer(/*nullable=*/false);
    writer.write(vec);
    auto chunk_up = writer.finish();

    auto* chunk = dynamic_cast<StringChunk*>(chunk_up.get());
    ASSERT_NE(chunk, nullptr);

    // Verify rows and content
    int64_t total_rows = static_cast<int64_t>(b1.size() + b2.size());
    EXPECT_EQ(chunk->RowNums(), total_rows);

    for (int64_t i = 0; i < static_cast<int64_t>(b1.size()); ++i) {
        auto sv = (*chunk)[static_cast<int>(i)];
        ASSERT_TRUE(chunk->isValid(static_cast<int>(i)));
        EXPECT_EQ(sv, b1[static_cast<size_t>(i)].value());
    }
    for (int64_t i = 0; i < static_cast<int64_t>(b2.size()); ++i) {
        auto idx = static_cast<int>(b1.size() + i);
        auto sv = (*chunk)[idx];
        ASSERT_TRUE(chunk->isValid(idx));
        EXPECT_EQ(sv, b2[static_cast<size_t>(i)].value());
    }
}

TEST(StringChunkWriterTest, WithNullsMergedBitmap) {
    // Prepare batches with nulls; only verify validity for nulls
    std::vector<std::optional<std::string>> b1 = {
        std::nullopt,
        std::string("alpha"),
        std::nullopt,
        std::string("beta"),
        std::string("gamma"),
    };
    std::vector<std::optional<std::string>> b2 = {
        std::string("one"),
        std::nullopt,
        std::string("two"),
        std::string(""),
    };

    auto a1 = BuildBinaryArray(b1);
    auto a2 = BuildBinaryArray(b2);

    arrow::ArrayVector vec{a1, a2};

    StringChunkWriter writer(/*nullable=*/true);
    writer.write(vec);
    auto chunk_up = writer.finish();

    auto* chunk = dynamic_cast<StringChunk*>(chunk_up.get());
    ASSERT_NE(chunk, nullptr);

    // Verify validity and content for non-null entries only
    std::vector<std::optional<std::string>> all;
    all.reserve(b1.size() + b2.size());
    all.insert(all.end(), b1.begin(), b1.end());
    all.insert(all.end(), b2.begin(), b2.end());

    for (int i = 0; i < static_cast<int>(all.size()); ++i) {
        bool expect_valid = all[static_cast<size_t>(i)].has_value();
        EXPECT_EQ(chunk->isValid(i), expect_valid);
        if (expect_valid) {
            EXPECT_EQ((*chunk)[i], all[static_cast<size_t>(i)].value());
        }
    }
}
