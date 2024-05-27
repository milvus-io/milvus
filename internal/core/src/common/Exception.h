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

namespace milvus {

class MilvusException : public std::exception {
 public:
    explicit MilvusException(const std::string& msg)
        : std::exception(), exception_message_(msg) {
    }
    const char*
    what() const noexcept {
        return exception_message_.c_str();
    }
    virtual ~MilvusException() {
    }

 private:
    std::string exception_message_;
};

class NotImplementedException : public std::exception {
 public:
    explicit NotImplementedException(const std::string& msg)
        : std::exception(), exception_message_(msg) {
    }
    const char*
    what() const noexcept {
        return exception_message_.c_str();
    }
    virtual ~NotImplementedException() {
    }

 private:
    std::string exception_message_;
};

class NotSupportedDataTypeException : public std::exception {
 public:
    explicit NotSupportedDataTypeException(const std::string& msg)
        : std::exception(), exception_message_(msg) {
    }
    const char*
    what() const noexcept {
        return exception_message_.c_str();
    }
    virtual ~NotSupportedDataTypeException() {
    }

 private:
    std::string exception_message_;
};

class UnistdException : public std::runtime_error {
 public:
    explicit UnistdException(const std::string& msg) : std::runtime_error(msg) {
    }

    virtual ~UnistdException() {
    }
};

// Exceptions for storage module
class LocalChunkManagerException : public std::runtime_error {
 public:
    explicit LocalChunkManagerException(const std::string& msg)
        : std::runtime_error(msg) {
    }
    virtual ~LocalChunkManagerException() {
    }
};

class InvalidPathException : public LocalChunkManagerException {
 public:
    explicit InvalidPathException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~InvalidPathException() {
    }
};

class OpenFileException : public LocalChunkManagerException {
 public:
    explicit OpenFileException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~OpenFileException() {
    }
};

class CreateFileException : public LocalChunkManagerException {
 public:
    explicit CreateFileException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~CreateFileException() {
    }
};

class ReadFileException : public LocalChunkManagerException {
 public:
    explicit ReadFileException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~ReadFileException() {
    }
};

class WriteFileException : public LocalChunkManagerException {
 public:
    explicit WriteFileException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~WriteFileException() {
    }
};

class PathAlreadyExistException : public LocalChunkManagerException {
 public:
    explicit PathAlreadyExistException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~PathAlreadyExistException() {
    }
};

class DirNotExistException : public LocalChunkManagerException {
 public:
    explicit DirNotExistException(const std::string& msg)
        : LocalChunkManagerException(msg) {
    }
    virtual ~DirNotExistException() {
    }
};

class MinioException : public std::runtime_error {
 public:
    explicit MinioException(const std::string& msg) : std::runtime_error(msg) {
    }
    virtual ~MinioException() {
    }
};

class InvalidBucketNameException : public MinioException {
 public:
    explicit InvalidBucketNameException(const std::string& msg)
        : MinioException(msg) {
    }
    virtual ~InvalidBucketNameException() {
    }
};

class ObjectNotExistException : public MinioException {
 public:
    explicit ObjectNotExistException(const std::string& msg)
        : MinioException(msg) {
    }
    virtual ~ObjectNotExistException() {
    }
};
class S3ErrorException : public MinioException {
 public:
    explicit S3ErrorException(const std::string& msg) : MinioException(msg) {
    }
    virtual ~S3ErrorException() {
    }
};

class DiskANNFileManagerException : public std::runtime_error {
 public:
    explicit DiskANNFileManagerException(const std::string& msg)
        : std::runtime_error(msg) {
    }
    virtual ~DiskANNFileManagerException() {
    }
};

class ArrowException : public std::runtime_error {
 public:
    explicit ArrowException(const std::string& msg) : std::runtime_error(msg) {
    }
    virtual ~ArrowException() {
    }
};

// Exceptions for executor module
class ExecDriverException : public std::exception {
 public:
    explicit ExecDriverException(const std::string& msg)
        : std::exception(), exception_message_(msg) {
    }
    const char*
    what() const noexcept {
        return exception_message_.c_str();
    }
    virtual ~ExecDriverException() {
    }

 private:
    std::string exception_message_;
};
class ExecOperatorException : public std::exception {
 public:
    explicit ExecOperatorException(const std::string& msg)
        : std::exception(), exception_message_(msg) {
    }
    const char*
    what() const noexcept {
        return exception_message_.c_str();
    }
    virtual ~ExecOperatorException() {
    }

 private:
    std::string exception_message_;
};
}  // namespace milvus
