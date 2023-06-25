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

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <map>

namespace milvus::storage {

/**
 * @brief This ChunkManager is abstract interface for milvus that
 * used to manager operation and interaction with storage
 */
class ChunkManager {
 public:
    /**
     * @brief Whether file exists or not
     * @param filepath
     * @return true
     * @return false
     */
    virtual bool
    Exist(const std::string& filepath) = 0;

    /**
     * @brief Get file size
     * @param filepath
     * @return uint64_t
     */
    virtual uint64_t
    Size(const std::string& filepath) = 0;

    /**
     * @brief Read file to buffer
     * @param filepath
     * @param buf
     * @param len
     * @return uint64_t
     */
    virtual uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len) = 0;

    /**
     * @brief Write buffer to file with offset
     * @param filepath
     * @param buf
     * @param len
     */
    virtual void
    Write(const std::string& filepath, void* buf, uint64_t len) = 0;

    /**
     * @brief Read file to buffer with offset
     * @param filepath
     * @param buf
     * @param len
     * @return uint64_t
     */
    virtual uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) = 0;

    /**
     * @brief Write buffer to file with offset
     * @param filepath
     * @param buf
     * @param len
     */
    virtual void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) = 0;

    /**
     * @brief List files with same prefix
     * @param filepath
     * @return std::vector<std::string>
     */
    virtual std::vector<std::string>
    ListWithPrefix(const std::string& filepath) = 0;

    /**
     * @brief Remove specified file
     * @param filepath
     */
    virtual void
    Remove(const std::string& filepath) = 0;

    /**
     * @brief Get the Name object
     * Used for forming diagnosis messages
     * @return std::string
     */
    virtual std::string
    GetName() const = 0;

    /**
     * @brief Get the Root Path
     * @return std::string
     */
    virtual std::string
    GetRootPath() const = 0;
};

using ChunkManagerPtr = std::shared_ptr<ChunkManager>;

enum ChunkManagerType : int8_t {
    None_CM = 0,
    Local = 1,
    Minio = 2,
};

extern std::map<std::string, ChunkManagerType> ChunkManagerType_Map;

}  // namespace milvus::storage
