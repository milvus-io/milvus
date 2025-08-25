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

#include "filemanager/InputStream.h"
#include "milvus-storage/filesystem/fs.h"

namespace milvus::storage {

class RemoteInputStream : public milvus::InputStream {
 public:
    explicit RemoteInputStream(
        std::shared_ptr<arrow::io::RandomAccessFile>&& remote_file);

    ~RemoteInputStream() override = default;

    size_t
    Size() const override;

    size_t
    Read(void* data, size_t size) override;

    size_t
    ReadAt(void* data, size_t offset, size_t size) override;

    size_t
    Read(int fd, size_t size) override;

    size_t
    Tell() const override;

    bool
    Eof() const override;

    bool
    Seek(int64_t offset) override;

 private:
    size_t file_size_;
    std::shared_ptr<arrow::io::RandomAccessFile> remote_file_;
};

}  // namespace milvus::storage