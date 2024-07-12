#pragma once
#include <memory>
#include "arrow/record_batch.h"
#include "arrow/table_builder.h"
#include "common/Chunk.h"
#include "common/ChunkTarget.h"
#include "common/FieldMeta.h"
namespace milvus {
class StatisticsChunkWriter;
class StatisticsChunk;

class ChunkFileWriter {
 public:
    ChunkFileWriter() = default;
    ChunkFileWriter(std::string& file_path);
    struct FileRep {
        std::vector<Chunk> chunks;
    };
    void
    write_chunk(std::shared_ptr<arrow::RecordBatchReader> r);

    FileRep
    finish();

 private:
    FieldMeta& field_meta_;
    int dim_;
    StatisticsChunkWriter stat_writer_;
    File file_;
    FileRep rep_;
};

}  // namespace milvus