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
#include <stdexcept>
#include <string>

namespace milvus::storage {

class NotImplementedException : public std::exception {
 public:
    explicit NotImplementedException(const std::string& msg) : std::exception(), exception_message_(msg) {
    }
    [[nodiscard]] const char*
    what() const noexcept override {
        return exception_message_.c_str();
    }
    ~NotImplementedException() override {
    }

 private:
    std::string exception_message_;
};

class LocalChunkManagerException : public std::runtime_error {
 public:
    explicit LocalChunkManagerException(const std::string& msg) : std::runtime_error(msg) {
    }
    ~LocalChunkManagerException() override {
    }
};

class InvalidPathException : public LocalChunkManagerException {
 public:
    explicit InvalidPathException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~InvalidPathException() override {
    }
};

class OpenFileException : public LocalChunkManagerException {
 public:
    explicit OpenFileException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~OpenFileException() override {
    }
};

class CreateFileException : public LocalChunkManagerException {
 public:
    explicit CreateFileException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~CreateFileException() override {
    }
};

class ReadFileException : public LocalChunkManagerException {
 public:
    explicit ReadFileException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~ReadFileException() override {
    }
};

class WriteFileException : public LocalChunkManagerException {
 public:
    explicit WriteFileException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~WriteFileException() override {
    }
};

class PathAlreadyExistException : public LocalChunkManagerException {
 public:
    explicit PathAlreadyExistException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~PathAlreadyExistException() override {
    }
};

class DirNotExistException : public LocalChunkManagerException {
 public:
    explicit DirNotExistException(const std::string& msg) : LocalChunkManagerException(msg) {
    }
    ~DirNotExistException() override {
    }
};

class MinioException : public std::runtime_error {
 public:
    explicit MinioException(const std::string& msg) : std::runtime_error(msg) {
    }
    ~MinioException() override {
    }
};

class InvalidBucketNameException : public MinioException {
 public:
    explicit InvalidBucketNameException(const std::string& msg) : MinioException(msg) {
    }
    ~InvalidBucketNameException() override {
    }
};

class ObjectNotExistException : public MinioException {
 public:
    explicit ObjectNotExistException(const std::string& msg) : MinioException(msg) {
    }
    ~ObjectNotExistException() override {
    }
};
class S3ErrorException : public MinioException {
 public:
    explicit S3ErrorException(const std::string& msg) : MinioException(msg) {
    }
    ~S3ErrorException() override {
    }
};

class DiskANNFileManagerException : public std::runtime_error {
 public:
    explicit DiskANNFileManagerException(const std::string& msg) : std::runtime_error(msg) {
    }
    ~DiskANNFileManagerException() override {
    }
};

class ArrowException : public std::runtime_error {
 public:
    explicit ArrowException(const std::string& msg) : std::runtime_error(msg) {
    }
    ~ArrowException() override {
    }
};

}  // namespace milvus::storage
