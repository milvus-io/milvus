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

#include "segcore/column_groups_c.h"
#include "segcore/packed_writer_c.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/common/log.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"

#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include "common/EasyAssert.h"
#include "common/type_c.h"

CStatus
NewPackedWriter(struct ArrowSchema* schema,
                const int64_t buffer_size,
                char** paths,
                int64_t num_paths,
                int64_t part_upload_size,
                CColumnGroups column_groups,
                CPackedWriter* c_packed_writer) {
    try {
        auto truePaths = std::vector<std::string>(paths, paths + num_paths);

        auto conf = milvus_storage::StorageConfig();
        conf.part_size = part_upload_size;

        auto trueFs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                .GetArrowFileSystem();
        if (!trueFs) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileWriteFailed, "Failed to get filesystem");
        }

        auto trueSchema = arrow::ImportSchema(schema).ValueOrDie();

        auto columnGroups = *static_cast<std::vector<std::vector<int>>*>(column_groups);

        auto writer = std::make_unique<milvus_storage::PackedRecordBatchWriter>(
            trueFs, truePaths, trueSchema, conf, columnGroups, buffer_size);

        *c_packed_writer = writer.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
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
            return milvus::FailureCStatus(milvus::ErrorCode::FileWriteFailed, status.ToString());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CloseWriter(CPackedWriter c_packed_writer) {
    try {
        auto packed_writer =
            static_cast<milvus_storage::PackedRecordBatchWriter*>(
                c_packed_writer);
        auto status = packed_writer->Close();
        delete packed_writer;
        if (!status.ok()) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileWriteFailed, status.ToString());
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}