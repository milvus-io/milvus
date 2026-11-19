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
#include "milvus-storage/filesystem/fs.h"
#include "storage/PluginLoader.h"
#include "storage/KeyRetriever.h"
#include "storage/StorageV2FSCache.h"

#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/status.h>
#include <memory>
#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "monitor/scope_metric.h"

namespace {
CStatus
OpenPackedReader(char** paths,
                 int64_t num_paths,
                 ArrowSchema* schema,
                 int64_t buffer_size,
                 const CStorageConfig* config,
                 CPackedReader* reader,
                 const CPluginContext* context,
                 bool for_import) {
    try {
        auto fs = milvus::storage::GetPackedFileSystem(config);
        if (!fs) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::FileReadFailed,
                "[StorageV2] Failed to get filesystem");
        }
        auto file_paths = std::vector<std::string>(paths, paths + num_paths);
        auto arrow_schema = arrow::ImportSchema(schema).ValueOrDie();
        auto& loader = milvus::storage::PluginLoader::GetInstance();
        if (!for_import && context != nullptr) {
            if (auto cipher = loader.getCipherPlugin()) {
                cipher->Update(
                    context->ez_id, context->collection_id, context->key);
            }
        }
        auto properties =
            for_import ? milvus::storage::GetImportReaderProperties(context)
                       : milvus::storage::GetReaderProperties();
        *reader = new milvus_storage::PackedRecordBatchReader(
            fs,
            file_paths,
            arrow_schema,
            buffer_size,
            properties,
            milvus::storage::GetArrowReaderProperties());
        return milvus::SuccessCStatus();
    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
}  // namespace

CStatus
NewPackedReaderWithStorageConfig(char** paths,
                                 int64_t num_paths,
                                 ArrowSchema* schema,
                                 int64_t buffer_size,
                                 CStorageConfig config,
                                 CPackedReader* reader,
                                 CPluginContext* context) {
    SCOPE_CGO_CALL_METRIC();
    return OpenPackedReader(
        paths, num_paths, schema, buffer_size, &config, reader, context, false);
}

CStatus
NewPackedReader(char** paths,
                int64_t num_paths,
                ArrowSchema* schema,
                int64_t buffer_size,
                CPackedReader* reader,
                CPluginContext* context) {
    SCOPE_CGO_CALL_METRIC();
    return OpenPackedReader(
        paths, num_paths, schema, buffer_size, nullptr, reader, context, false);
}

CStatus
NewImportPackedReader(char** paths,
                      int64_t num_paths,
                      ArrowSchema* schema,
                      int64_t buffer_size,
                      const CStorageConfig* config,
                      CPackedReader* reader,
                      const CPluginContext* source) {
    SCOPE_CGO_CALL_METRIC();
    return OpenPackedReader(
        paths, num_paths, schema, buffer_size, config, reader, source, true);
}

CStatus
ReadNext(CPackedReader c_packed_reader,
         CArrowArray* out_array,
         CArrowSchema* out_schema) {
    SCOPE_CGO_CALL_METRIC();

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
    SCOPE_CGO_CALL_METRIC();

    try {
        auto packed_reader =
            static_cast<milvus_storage::PackedRecordBatchReader*>(
                c_packed_reader);
        packed_reader->Close();
        delete packed_reader;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
