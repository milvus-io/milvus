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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"
#include "opendal.h"

namespace milvus::storage {
class OpenDALChunkManager : public ChunkManager {
 public:
    OpenDALChunkManager() = default;
    explicit OpenDALChunkManager(const StorageConfig& storage_config);

    OpenDALChunkManager(const OpenDALChunkManager&);
    OpenDALChunkManager&
    operator=(const OpenDALChunkManager&);

 public:
    virtual ~OpenDALChunkManager();

    bool
    Exist(const std::string& filepath) override;

    uint64_t
    Size(const std::string& filepath) override;

    uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) override {
        PanicInfo(NotImplemented, GetName() + "Read with offset not implement");
    }

    void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) override {
        PanicInfo(NotImplemented,
                  GetName() + "Write with offset not implement");
    }

    uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len) override;

    void
    Write(const std::string& filepath, void* buf, uint64_t len) override;

    std::vector<std::string>
    ListWithPrefix(const std::string& filepath) override;

    void
    Remove(const std::string& filepath) override;

    std::string
    GetName() const override {
        return "OpenDALChunkManager";
    }

    std::string
    GetRootPath() const override {
        return remote_root_path_;
    }

 private:
    std::string default_bucket_name_;
    std::string remote_root_path_;

    const opendal_operator* op_ptr_;
};
struct OpendalReader {
    explicit OpendalReader(opendal_reader* reader) : reader_(reader) {
    }
    ~OpendalReader() {
        opendal_reader_free(reader_);
    }
    opendal_reader*
    Get() {
        return reader_;
    }

 private:
    opendal_reader* reader_;
};
struct OpendalLister {
    explicit OpendalLister(opendal_lister* lister) : lister_(lister) {
    }
    ~OpendalLister() {
        opendal_lister_free(lister_);
    }
    opendal_lister*
    Get() {
        return lister_;
    }

 private:
    opendal_lister* lister_;
};
}  // namespace milvus::storage
