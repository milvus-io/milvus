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