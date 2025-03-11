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

#include "segcore/packed_reader_c.h"
#include "milvus-storage/packed/reader.h"
#include "milvus-storage/common/log.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/config.h"

#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/status.h>
#include <memory>
#include "common/EasyAssert.h"
#include "common/type_c.h"

CStatus
NewPackedReader(char** paths,
                int64_t num_paths,
                struct ArrowSchema* schema,
                const int64_t buffer_size,
                CPackedReader* c_packed_reader) {
    try {
        auto truePaths = std::vector<std::string>(paths, paths + num_paths);
        auto trueFs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                          .GetArrowFileSystem();
        if (!trueFs) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileReadFailed,
                                          "Failed to get filesystem");
        }
        auto trueSchema = arrow::ImportSchema(schema).ValueOrDie();
        std::set<int> needed_columns;
        for (int i = 0; i < trueSchema->num_fields(); i++) {
            needed_columns.emplace(i);
        }
        auto reader = std::make_unique<milvus_storage::PackedRecordBatchReader>(
            trueFs, truePaths, trueSchema, needed_columns, buffer_size);
        *c_packed_reader = reader.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
ReadNext(CPackedReader c_packed_reader,
         CArrowArray* out_array,
         CArrowSchema* out_schema) {
    try {
        auto packed_reader =
            static_cast<milvus_storage::PackedRecordBatchReader*>(
                c_packed_reader);
        std::shared_ptr<arrow::RecordBatch> record_batch;
        auto status = packed_reader->ReadNext(&record_batch);
        if (!status.ok()) {
            return milvus::FailureCStatus(milvus::ErrorCode::FileReadFailed,
                                          status.ToString());
        }
        if (record_batch == nullptr) {
            // end of file
            return milvus::SuccessCStatus();
        } else {
            std::unique_ptr<ArrowArray> arr = std::make_unique<ArrowArray>();
            std::unique_ptr<ArrowSchema> schema =
                std::make_unique<ArrowSchema>();
            auto status = arrow::ExportRecordBatch(
                *record_batch, arr.get(), schema.get());
            if (!status.ok()) {
                return milvus::FailureCStatus(milvus::ErrorCode::FileReadFailed,
                                              status.ToString());
            }
            *out_array = arr.release();
            *out_schema = schema.release();
            return milvus::SuccessCStatus();
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CloseReader(CPackedReader c_packed_reader) {
    auto packed_reader =
        static_cast<milvus_storage::PackedRecordBatchReader*>(c_packed_reader);
    auto status = packed_reader->Close();
    if (!status.ok()) {
        return milvus::FailureCStatus(milvus::ErrorCode::FileReadFailed,
                                      status.ToString());
    }
    delete packed_reader;
    return milvus::SuccessCStatus();
}