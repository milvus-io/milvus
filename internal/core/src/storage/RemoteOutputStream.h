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

#include "filemanager/OutputStream.h"
#include "milvus-storage/filesystem/fs.h"

namespace milvus::storage {

class RemoteOutputStream : public milvus::OutputStream {
 public:
    explicit RemoteOutputStream(
        std::shared_ptr<arrow::io::OutputStream>&& output_stream);

    ~RemoteOutputStream() override = default;

    size_t
    Tell() const override;

    size_t
    Write(const void* data, size_t size) override;

    size_t
    Write(int fd, size_t size) override;

 private:
    std::shared_ptr<arrow::io::OutputStream> output_stream_;
};

}  // namespace milvus::storage