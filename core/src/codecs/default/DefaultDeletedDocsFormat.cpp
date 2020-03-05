// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "codecs/default/DefaultDeletedDocsFormat.h"

#include <fcntl.h>
#include <unistd.h>

#define BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/filesystem.hpp>
#undef BOOST_NO_CXX11_SCOPED_ENUMS
#include <memory>
#include <string>
#include <vector>

#include "segment/Types.h"
#include "server/Config.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
DefaultDeletedDocsFormat::read(const storage::OperationPtr& operation_ptr, segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = operation_ptr->GetDirectory();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;

    try {
        TimeRecorder recorder("read deleted docs " + del_file_path);
        std::shared_ptr<storage::IOReader> reader_ptr;
        if (s3_enable) {
            reader_ptr = std::make_shared<storage::S3IOReader>(del_file_path);
        } else {
            reader_ptr = std::make_shared<storage::DiskIOReader>(del_file_path);
        }

        size_t file_size = reader_ptr->length();
        if (file_size <= 0) {
            std::string err_msg = "File size " + std::to_string(file_size) + " invalid: " + del_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }

        size_t rp = 0;
        reader_ptr->seekg(0);

        size_t num_bytes;
        reader_ptr->read(&num_bytes, sizeof(size_t));
        rp += sizeof(size_t);
        reader_ptr->seekg(rp);

        std::vector<segment::offset_t> deleted_docs_list;
        deleted_docs_list.resize(num_bytes / sizeof(segment::offset_t));
        reader_ptr->read(deleted_docs_list.data(), num_bytes);
        rp += num_bytes;
        reader_ptr->seekg(rp);

        if (rp != file_size) {
            std::string err_msg = "File size mis-match: " + del_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }

        deleted_docs = std::make_shared<segment::DeletedDocs>(deleted_docs_list);

        double span = recorder.RecordSection("done");
        double rate = file_size * 1000000.0 / span / 1024 / 1024;
        ENGINE_LOG_DEBUG << "read(" << del_file_path << ") rate " << rate << "MB/s";
    } catch (std::exception& e) {
        std::string err_msg = "Failed to read from file: " + del_file_path + ", error: " + e.what();
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

void
DefaultDeletedDocsFormat::write(const storage::OperationPtr& operation_ptr,
                                const segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = operation_ptr->GetDirectory();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;

    // try {
    //     TimeRecorder recorder("write " + del_file_path);
    //     std::shared_ptr<storage::IOWriter> writer_ptr;
    //     if (s3_enable) {
    //         writer_ptr = std::make_shared<storage::S3IOWriter>(del_file_path);
    //     } else {
    //         writer_ptr = std::make_shared<storage::FileIOWriter>(del_file_path);
    //     }
    //     auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    //     size_t num_bytes = sizeof(segment::offset_t) * deleted_docs->GetSize();
    //     writer_ptr->write((void*)(deleted_docs_list.data()), num_bytes);

    //     double span = recorder.RecordSection("done");
    //     double rate = num_bytes * 1000000.0 / span / 1024 / 1024;
    //     ENGINE_LOG_DEBUG << "write(" << del_file_path << ") rate " << rate << "MB/s";
    // } catch (std::exception& e) {
    //     std::string err_msg = "Failed to write rv file: " + del_file_path + ", error: " + e.what();
    //     ENGINE_LOG_ERROR << err_msg;
    //     throw Exception(SERVER_WRITE_ERROR, err_msg);
    // }

    // Create a temporary file from the existing file
    const std::string temp_path = dir_path + "/" + "temp_del";
    bool exists = boost::filesystem::exists(del_file_path);
    if (exists) {
        boost::filesystem::copy_file(del_file_path, temp_path, boost::filesystem::copy_option::fail_if_exists);
    }

    // Write to the temp file, in order to avoid possible race condition with search (concurrent read and write)
    int del_fd = open(temp_path.c_str(), O_RDWR | O_CREAT, 00664);
    if (del_fd == -1) {
        std::string err_msg = "Failed to open file: " + temp_path;
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    size_t old_num_bytes;
    if (exists) {
        if (::read(del_fd, &old_num_bytes, sizeof(size_t)) == -1) {
            std::string err_msg = "Failed to read from file: " + temp_path + ", error: " + std::strerror(errno);
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
    } else {
        old_num_bytes = 0;
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    size_t new_num_bytes = old_num_bytes + sizeof(segment::offset_t) * deleted_docs->GetSize();

    // rewind and overwrite with the new_num_bytes
    int off = lseek(del_fd, 0, SEEK_SET);
    if (off == -1) {
        std::string err_msg = "Failed to seek file: " + temp_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::write(del_fd, &new_num_bytes, sizeof(size_t)) == -1) {
        std::string err_msg = "Failed to write to file" + temp_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    // Move to the end of file and append
    off = lseek(del_fd, 0, SEEK_END);
    if (off == -1) {
        std::string err_msg = "Failed to seek file: " + temp_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::write(del_fd, deleted_docs_list.data(), new_num_bytes) == -1) {
        std::string err_msg = "Failed to write to file" + temp_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    if (::close(del_fd) == -1) {
        std::string err_msg = "Failed to close file: " + temp_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    // Move temp file to delete file
    boost::filesystem::rename(temp_path, del_file_path);
}

}  // namespace codec
}  // namespace milvus
