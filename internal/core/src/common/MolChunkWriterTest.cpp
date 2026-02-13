// Copyright (C) 2019-2025 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/builder_binary.h"

#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/FieldMeta.h"
#include "common/Types.h"

using milvus::create_chunk;
using milvus::DataType;
using milvus::FieldId;
using milvus::FieldMeta;
using milvus::FieldName;
using milvus::MolChunk;

namespace {

std::shared_ptr<arrow::BinaryArray>
BuildMolBinaryArray(const std::vector<std::optional<std::string>>& values) {
    arrow::BinaryBuilder builder;
    for (const auto& v : values) {
        if (v.has_value()) {
            auto st =
                builder.Append(v->data(), static_cast<int32_t>(v->size()));
            EXPECT_TRUE(st.ok());
        } else {
            auto st = builder.AppendNull();
            EXPECT_TRUE(st.ok());
        }
    }
    std::shared_ptr<arrow::Array> arr;
    auto st = builder.Finish(&arr);
    EXPECT_TRUE(st.ok());
    return std::static_pointer_cast<arrow::BinaryArray>(arr);
}

FieldMeta
MakeMolFieldMeta(bool nullable) {
    return FieldMeta(
        FieldName("mol"), FieldId(1), DataType::MOL, nullable, std::nullopt);
}

}  // namespace

TEST(MolChunkWriterTest, NoNullsMultiBatches) {
    // Prepare two batches of MOL data (SMILES strings stored as binary)
    std::vector<std::optional<std::string>> b1 = {
        std::string("C"),
        std::string("CC"),
        std::string("CCO"),
        std::string("c1ccccc1"),
        std::string("CC(=O)O"),
    };
    std::vector<std::optional<std::string>> b2 = {
        std::string("CCN"),
        std::string("CCOCC"),
        std::string("CC(C)C"),
    };

    auto a1 = BuildMolBinaryArray(b1);
    auto a2 = BuildMolBinaryArray(b2);

    arrow::ArrayVector vec{a1, a2};

    auto field_meta = MakeMolFieldMeta(/*nullable=*/false);
    auto chunk_up = create_chunk(field_meta, vec);

    auto* chunk = dynamic_cast<MolChunk*>(chunk_up.get());
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

TEST(MolChunkWriterTest, WithNullsMergedBitmap) {
    // Prepare batches with nulls
    std::vector<std::optional<std::string>> b1 = {
        std::nullopt,
        std::string("C"),
        std::nullopt,
        std::string("CCO"),
        std::string("c1ccccc1"),
    };
    std::vector<std::optional<std::string>> b2 = {
        std::string("CC(=O)O"),
        std::nullopt,
        std::string("CCN"),
        std::string(""),
    };

    auto a1 = BuildMolBinaryArray(b1);
    auto a2 = BuildMolBinaryArray(b2);

    arrow::ArrayVector vec{a1, a2};

    auto field_meta = MakeMolFieldMeta(/*nullable=*/true);
    auto chunk_up = create_chunk(field_meta, vec);

    auto* chunk = dynamic_cast<MolChunk*>(chunk_up.get());
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

TEST(MolChunkWriterTest, EmptyBatch) {
    // Test with an empty array
    std::vector<std::optional<std::string>> empty_batch;
    auto a1 = BuildMolBinaryArray(empty_batch);

    arrow::ArrayVector vec{a1};

    auto field_meta = MakeMolFieldMeta(/*nullable=*/false);
    auto chunk_up = create_chunk(field_meta, vec);

    auto* chunk = dynamic_cast<MolChunk*>(chunk_up.get());
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), 0);
}

TEST(MolChunkWriterTest, LargeBatch) {
    // Test with a large batch of MOL data
    static const std::vector<std::string> smiles_list = {
        "C",
        "CC",
        "CCC",
        "CCO",
        "CCCO",
        "c1ccccc1",
        "CC(=O)O",
        "CCN",
        "CCOCC",
        "CC(C)C",
    };

    std::vector<std::optional<std::string>> batch;
    for (int i = 0; i < 1000; ++i) {
        batch.emplace_back(smiles_list[i % smiles_list.size()]);
    }

    auto arr = BuildMolBinaryArray(batch);
    arrow::ArrayVector vec{arr};

    auto field_meta = MakeMolFieldMeta(/*nullable=*/false);
    auto chunk_up = create_chunk(field_meta, vec);

    auto* chunk = dynamic_cast<MolChunk*>(chunk_up.get());
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), 1000);

    for (int i = 0; i < 1000; ++i) {
        auto sv = (*chunk)[i];
        EXPECT_EQ(sv, smiles_list[i % smiles_list.size()]);
    }
}

TEST(MolChunkWriterTest, SingleRow) {
    // Test with a single row
    std::vector<std::optional<std::string>> batch = {
        std::string("c1ccc(O)cc1"),  // Phenol
    };

    auto arr = BuildMolBinaryArray(batch);
    arrow::ArrayVector vec{arr};

    auto field_meta = MakeMolFieldMeta(/*nullable=*/false);
    auto chunk_up = create_chunk(field_meta, vec);

    auto* chunk = dynamic_cast<MolChunk*>(chunk_up.get());
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), 1);
    EXPECT_EQ((*chunk)[0], "c1ccc(O)cc1");
}

TEST(MolChunkWriterTest, NullableSingleNull) {
    // Test with all nulls
    std::vector<std::optional<std::string>> batch = {
        std::nullopt,
        std::nullopt,
        std::nullopt,
    };

    auto arr = BuildMolBinaryArray(batch);
    arrow::ArrayVector vec{arr};

    auto field_meta = MakeMolFieldMeta(/*nullable=*/true);
    auto chunk_up = create_chunk(field_meta, vec);

    auto* chunk = dynamic_cast<MolChunk*>(chunk_up.get());
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), 3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(chunk->isValid(i));
    }
}
