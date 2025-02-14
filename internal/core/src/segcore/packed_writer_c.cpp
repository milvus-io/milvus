// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/packed_writer_c.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/common/log.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"

#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>

int
NewPackedWriter(const char* path,
                struct ArrowSchema* schema,
                const int64_t buffer_size,
                CPackedWriter* c_packed_writer) {
    try {
        auto truePath = std::string(path);
        auto factory = std::make_shared<milvus_storage::FileSystemFactory>();
        auto conf = milvus_storage::StorageConfig();
        conf.uri = "file:///tmp/";
        auto trueFs = factory->BuildFileSystem(conf, &truePath).value();
        auto trueSchema = arrow::ImportSchema(schema).ValueOrDie();
        auto writer = std::make_unique<milvus_storage::PackedRecordBatchWriter>(
            buffer_size, trueSchema, trueFs, truePath, conf);

        *c_packed_writer = writer.release();
        return 0;
    } catch (std::exception& e) {
        return -1;
    }
}

int
WriteRecordBatch(CPackedWriter c_packed_writer,
                 struct ArrowArray* array,
                 struct ArrowSchema* schema) {
    try {
        auto packed_writer =
            static_cast<milvus_storage::PackedRecordBatchWriter*>(
                c_packed_writer);
        auto record_batch =
            arrow::ImportRecordBatch(array, schema).ValueOrDie();
        auto status = packed_writer->Write(record_batch);
        if (!status.ok()) {
            return -1;
        }
        return 0;
    } catch (std::exception& e) {
        return -1;
    }
}

int
CloseWriter(CPackedWriter c_packed_writer) {
    try {
        auto packed_writer =
            static_cast<milvus_storage::PackedRecordBatchWriter*>(
                c_packed_writer);
        auto status = packed_writer->Close();
        delete packed_writer;
        if (!status.ok()) {
            return -1;
        }
        return 0;
    } catch (std::exception& e) {
        return -1;
    }
}