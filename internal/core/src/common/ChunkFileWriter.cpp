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