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

#include <stddef.h>
#include <stdint.h>
#include <memory>

#include "arrow/io/interfaces.h"
#include "arrow/filesystem/filesystem.h"
#include "filemanager/InputStream.h"
#include "folly/coro/Task.h"

namespace milvus::storage {

class RemoteInputStream : public milvus::InputStream {
 public:
    explicit RemoteInputStream(
        std::shared_ptr<arrow::io::RandomAccessFile>&& remote_file);

    ~RemoteInputStream() override = default;

    // Open the backing file and cache its size without blocking the caller on IO.
    static folly::coro::Task<std::shared_ptr<InputStream>>
    OpenAsync(std::shared_ptr<arrow::fs::FileSystem> fs, std::string path);

    size_t
    Size() const override;

    size_t
    Read(void* data, size_t size) override;

    size_t
    ReadAt(void* data, size_t offset, size_t size) override;

    folly::SemiFuture<size_t>
    ReadAtAsync(void* data, size_t offset, size_t size) override;

    size_t
    Read(int fd, size_t size) override;

    size_t
    Tell() const override;

    bool
    Eof() const override;

    bool
    Seek(int64_t offset) override;

 private:
    RemoteInputStream(std::shared_ptr<arrow::io::RandomAccessFile> remote_file,
                      size_t file_size)
        : file_size_(file_size), remote_file_(std::move(remote_file)) {
    }

    // Retries belong to this stream operation. Its future completes only after
    // the backing file has stopped accessing the caller-owned destination.
    folly::coro::Task<size_t>
    ReadAtAsyncImpl(void* data, size_t offset, size_t size);

    size_t file_size_;
    std::shared_ptr<arrow::io::RandomAccessFile> remote_file_;
};

}  // namespace milvus::storage
