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

#include <boost/filesystem.hpp>
#include <memory>
#include <string>
#include <vector>

#include "segment/Types.h"
#include "server/Config.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "storage/file/FileIOReader.h"
#include "storage/file/FileIOWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

void
DefaultDeletedDocsFormat::read(const store::DirectoryPtr& directory_ptr, segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = directory_ptr->GetDirPath();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;

    try {
        std::shared_ptr<storage::IOReader> reader_ptr;
        if (s3_enable) {
            reader_ptr = std::make_shared<storage::S3IOReader>(del_file_path);
        } else {
            reader_ptr = std::make_shared<storage::FileIOReader>(del_file_path);
        }

        size_t file_size = reader_ptr->length();
        std::vector<segment::offset_t> deleted_docs_list;
        deleted_docs_list.resize(file_size / sizeof(segment::offset_t));
        reader_ptr->read(deleted_docs_list.data(), file_size);

        deleted_docs = std::make_shared<segment::DeletedDocs>(deleted_docs_list);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to read from file: " + del_file_path + ", error: " + e.what();
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

void
DefaultDeletedDocsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = directory_ptr->GetDirPath();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;

    try {
        std::shared_ptr<storage::IOWriter> writer_ptr;
        if (s3_enable) {
            writer_ptr = std::make_shared<storage::S3IOWriter>(del_file_path);
        } else {
            writer_ptr = std::make_shared<storage::FileIOWriter>(del_file_path);
        }
        auto deleted_docs_list = deleted_docs->GetDeletedDocs();
        writer_ptr->write((void*)(deleted_docs_list.data()), sizeof(segment::offset_t) * deleted_docs->GetSize());
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write rv file: " + del_file_path + ", error: " + e.what();
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

}  // namespace codec
}  // namespace milvus
