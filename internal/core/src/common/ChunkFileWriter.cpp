
#include "common/ChunkFileWriter.h"
#include "common/ChunkWriter.h"
namespace milvus {
ChunkFileWriter::ChunkFileWriter(std::string& file_path)
    : file_(File::Open(file_path, O_RDWR | O_CREAT | O_TRUNC)) {
}

void
ChunkFileWriter::write_chunk(std::shared_ptr<arrow::RecordBatchReader> r) {
    // FIXME
    size_t file_offset = file_.Seek(0, SEEK_END);
    auto chunk = create_chunk(field_meta_, dim_, file_, file_offset, r);
    // TODO: stat_writer_.write(chunk);
    rep_.chunks.push_back(*chunk);
}

FileRep
ChunkFileWriter::finish() {
    // TODO: stat_writer_.finish();
    // rep_.stat_chunk = stat_writer_.get();
    return rep_;
}

}  // namespace milvus