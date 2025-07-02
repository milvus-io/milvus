// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cachinglayer/Translator.h>
#include "common/Chunk.h"
#include "gtest/gtest.h"
#include "mmap/ChunkedColumn.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"

namespace milvus {

TEST(test_chunked_column, test_get_chunkid) {
    std::vector<int64_t> num_rows_per_chunk = {0, 10, 20, 30};
    auto num_chunks = num_rows_per_chunk.size();
    std::vector<std::unique_ptr<Chunk>> chunks;
    for (auto i = 0; i < num_chunks; ++i) {
        auto row_num = num_rows_per_chunk[i];
        auto chunk =
            std::make_unique<FixedWidthChunk>(row_num, 1, nullptr, 0, 4, false);
        chunks.push_back(std::move(chunk));
    }
    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "test", std::move(chunks));
    FieldMeta field_meta(
        FieldName("test"), FieldId(1), DataType::INT64, false, std::nullopt);
    ChunkedColumn column(std::move(translator), field_meta);

    int offset = 0;
    for (int i = 0; i < num_chunks; ++i) {
        for (int j = 0; j < num_rows_per_chunk[i]; ++j) {
            auto [chunk_id, offset_in_chunk] =
                column.GetChunkIDByOffset(offset);
            ASSERT_EQ(chunk_id, i);
            ASSERT_EQ(offset_in_chunk, j);
            offset++;
        }
    }
}
}  // namespace milvus
