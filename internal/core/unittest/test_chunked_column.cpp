#include "common/Chunk.h"
#include "gtest/gtest.h"
#include "mmap/ChunkedColumn.h"
namespace milvus {
TEST(test_chunked_column, test_get_chunkid) {
    ChunkedColumn column;
    std::vector<size_t> chunk_row_nums = {10, 20, 30};
    for (auto row_num : chunk_row_nums) {
        auto chunk =
            std::make_shared<FixedWidthChunk>(row_num, 1, nullptr, 0, 4, false);
        column.AddChunk(chunk);
    }

    int offset = 0;
    for (int i = 0; i < chunk_row_nums.size(); ++i) {
        for (int j = 0; j < chunk_row_nums[i]; ++j) {
            auto [chunk_id, offset_in_chunk] =
                column.GetChunkIDByOffset(offset);
            ASSERT_EQ(chunk_id, i);
            ASSERT_EQ(offset_in_chunk, j);
            offset++;
        }
    }
}
}  // namespace milvus