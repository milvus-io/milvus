// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
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

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/future.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "storage/FileManager.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexEntryReader.h"
#include "storage/RemoteInputStream.h"

namespace milvus::test {

class ScopedLoadTransientBudget {
 public:
    explicit ScopedLoadTransientBudget(size_t capacity_bytes)
        : budget_(storage::TransientMemoryBudget::GetLoadTransientBudget()),
          previous_capacity_bytes_(budget_.CapacityBytes()) {
        budget_.SetCapacityBytes(capacity_bytes);
    }

    ScopedLoadTransientBudget(const ScopedLoadTransientBudget&) = delete;
    ScopedLoadTransientBudget&
    operator=(const ScopedLoadTransientBudget&) = delete;

    ~ScopedLoadTransientBudget() {
        budget_.SetCapacityBytes(previous_capacity_bytes_);
    }

 private:
    storage::TransientMemoryBudget& budget_;
    size_t previous_capacity_bytes_;
};

class AsyncTrackingRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
    explicit AsyncTrackingRandomAccessFile(std::vector<uint8_t> content)
        : content_(std::move(content)) {
    }

    arrow::Status
    Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t>
    Tell() const override {
        return position_;
    }

    bool
    closed() const override {
        return closed_;
    }

    arrow::Status
    Seek(int64_t position) override {
        position_ = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override {
        auto result = CopyRange(position_, nbytes, out);
        if (!result.ok()) {
            return result;
        }
        position_ += result.ValueOrDie();
        return result;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, MakeBuffer(position_, nbytes));
        position_ += nbytes;
        return buffer;
    }

    arrow::Result<int64_t>
    ReadAt(int64_t position, int64_t nbytes, void* out) override {
        read_at_calls_.fetch_add(1);
        return CopyRange(position, nbytes, out);
    }

    arrow::Future<std::shared_ptr<arrow::Buffer>>
    ReadAsync(const arrow::io::IOContext&,
              int64_t position,
              int64_t nbytes) override {
        async_read_calls_.fetch_add(1);
        return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(
            MakeBuffer(position, nbytes));
    }

    arrow::Result<int64_t>
    GetSize() override {
        return static_cast<int64_t>(content_.size());
    }

    size_t
    ReadAtCalls() const {
        return read_at_calls_.load();
    }

    size_t
    AsyncReadCalls() const {
        return async_read_calls_.load();
    }

 private:
    arrow::Status
    ValidateRange(int64_t position, int64_t nbytes) const {
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("read range out of bounds");
        }
        auto begin = static_cast<size_t>(position);
        auto len = static_cast<size_t>(nbytes);
        if (begin > content_.size() || len > content_.size() - begin) {
            return arrow::Status::Invalid("read range out of bounds");
        }
        return arrow::Status::OK();
    }

    arrow::Result<int64_t>
    CopyRange(int64_t position, int64_t nbytes, void* out) const {
        auto status = ValidateRange(position, nbytes);
        if (!status.ok()) {
            return status;
        }
        std::memcpy(out,
                    content_.data() + static_cast<size_t>(position),
                    static_cast<size_t>(nbytes));
        return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    MakeBuffer(int64_t position, int64_t nbytes) const {
        auto status = ValidateRange(position, nbytes);
        if (!status.ok()) {
            return status;
        }
        return std::make_shared<arrow::Buffer>(
            content_.data() + static_cast<size_t>(position), nbytes);
    }

    std::vector<uint8_t> content_;
    int64_t position_{0};
    std::atomic<size_t> read_at_calls_{0};
    std::atomic<size_t> async_read_calls_{0};
    bool closed_{false};
};

inline std::vector<uint8_t>
ReadPackedIndexBytes(const storage::FileManagerContext& ctx,
                     const std::vector<std::string>& index_files) {
    auto file_manager = std::make_shared<storage::MemFileManagerImpl>(ctx);
    auto input = file_manager->OpenInputStream(index_files.front());
    std::vector<uint8_t> bytes(input->Size());
    input->ReadAt(bytes.data(), 0, bytes.size());
    return bytes;
}

inline std::unique_ptr<storage::IndexEntryReader>
OpenAsyncIndexEntryReader(std::vector<uint8_t> bytes,
                          AsyncTrackingRandomAccessFile** remote_file) {
    auto tracking_file =
        std::make_shared<AsyncTrackingRandomAccessFile>(std::move(bytes));
    *remote_file = tracking_file.get();
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file = tracking_file;
    auto input =
        std::make_shared<storage::RemoteInputStream>(std::move(arrow_file));
    return storage::IndexEntryReader::Open(input, input->Size());
}

inline std::unique_ptr<bool[]>
MakeBoolArray(const std::vector<bool>& values) {
    auto result = std::make_unique<bool[]>(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i];
    }
    return result;
}

}  // namespace milvus::test
