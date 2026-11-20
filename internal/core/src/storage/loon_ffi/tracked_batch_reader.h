// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <arrow/record_batch.h>
#include <atomic>
#include <memory>
#include "storage/StatusToErrorCode.h"

namespace milvus::storage {
// Arrow's C stream retains only a text error. Keep the producer's typed outcome
// next to the owning packed reader so Go can recover it after stream failure.
class TrackedBatchReader final : public arrow::RecordBatchReader {
 public:
    TrackedBatchReader(std::shared_ptr<arrow::RecordBatchReader> reader,
                       std::shared_ptr<std::atomic<int>> error)
        : reader_(std::move(reader)), error_(std::move(error)) {
    }
    std::shared_ptr<arrow::Schema>
    schema() const override {
        return reader_->schema();
    }
    arrow::Status
    ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
        try {
            auto status = reader_->ReadNext(batch);
            error_->store(
                status.ok() ? 0
                            : static_cast<int>(ArrowStatusToErrorCode(status)));
            return status;
        } catch (const milvus::SegcoreError& error) {
            error_->store(static_cast<int>(error.get_error_code()));
        } catch (...) {
            error_->store(static_cast<int>(milvus::UnexpectedError));
        }
        // No C++ exception or diagnostic containing provider data may cross
        // Arrow's C callback. The structured category remains in error_.
        batch->reset();
        return arrow::Status::IOError("native record batch read failed");
    }
    arrow::Status
    Close() override {
        return reader_->Close();
    }

 private:
    std::shared_ptr<arrow::RecordBatchReader> reader_;
    std::shared_ptr<std::atomic<int>> error_;
};
}  // namespace milvus::storage
