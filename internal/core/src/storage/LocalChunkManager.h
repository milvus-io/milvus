// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/ChunkManager.h"

namespace milvus::storage {

/**
 * @brief LocalChunkManager is responsible for read and write local file
 * that inherited from ChunkManager
 */
class LocalChunkManager : public ChunkManager {
 public:
    explicit LocalChunkManager(const std::string& path) : path_prefix_(path) {
    }

    LocalChunkManager(const LocalChunkManager&);
    LocalChunkManager&
    operator=(const LocalChunkManager&);

 public:
    virtual ~LocalChunkManager() {
    }

    virtual bool
    Exist(const std::string& filepath);

    /**
     * @brief Get file's size
     * if file not exist, throw exception
     * @param filepath
     * @return uint64_t
     */
    virtual uint64_t
    Size(const std::string& filepath);

    virtual uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len);

    /**
     * @brief Write buf to file
     * if file not exists, wAill create it automatically
     * not append mode, truncate mode
     * @param filepath
     * @param buf
     * @param len
     */
    virtual void
    Write(const std::string& filepath, void* buf, uint64_t len);

    /**
     * @brief Write buf to file with specified location.
     *  if file not exist, will throw exception instead of create it
     * @param filepath
     * @param offset
     * @param buf
     * @param len
     * @return uint64_t
     */
    virtual uint64_t
    Read(const std::string& filepath, uint64_t offset, void* buf, uint64_t len);

    virtual void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len);

    virtual std::vector<std::string>
    ListWithPrefix(const std::string& filepath);

    /**
     * @brief Remove file no matter whether file exists
     *  or not
     * @param filepath
     */
    virtual void
    Remove(const std::string& filepath);

    virtual std::string
    GetName() const {
        return "LocalChunkManager";
    }

    virtual std::string
    GetRootPath() const {
        return path_prefix_;
    }

    bool
    CreateFile(const std::string& filepath);

 public:
    bool
    DirExist(const std::string& dir);
    /**
     * @brief Delete directory totally
     * different from Remove, this interface drop local dir
     * instead of file, but for remote system, may has no
     * concept of directory, so just used in local chunk manager
     * @param dir
     */
    void
    RemoveDir(const std::string& dir);

    /**
     * @brief Create a Dir object
     * if dir already exists, throw exception
     * @param dir
     */
    void
    CreateDir(const std::string& dir);

    int64_t
    GetSizeOfDir(const std::string& dir);

 private:
    std::string path_prefix_;
};

using LocalChunkManagerSPtr =
    std::shared_ptr<milvus::storage::LocalChunkManager>;

}  // namespace milvus::storage
