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

#include "codecs/default/DefaultVectorsFormat.h"

#include <fcntl.h>
#include <unistd.h>
#include <boost/filesystem.hpp>

#include "server/Config.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "storage/file/FileIOReader.h"
#include "storage/file/FileIOWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
DefaultVectorsFormat::read(const store::DirectoryPtr& directory_ptr, segment::VectorsPtr& vectors_read) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = directory_ptr->GetDirPath();
    if (!s3_enable && !boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    //    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    for (; it != it_end; ++it) {
        const auto& path = it->path();
        if (path.extension().string() == raw_vector_extension_) {
            try {
                TimeRecorder recorder("read " + path.string());
                std::shared_ptr<storage::IOReader> rv_reader_ptr;
                if (s3_enable) {
                    rv_reader_ptr = std::make_shared<storage::S3IOReader>(path.string());
                } else {
                    rv_reader_ptr = std::make_shared<storage::FileIOReader>(path.string());
                }

                size_t file_size = rv_reader_ptr->length();
                std::vector<uint8_t> vector_list;
                vector_list.resize(file_size);
                rv_reader_ptr->read(vector_list.data(), file_size);

                vectors_read->AddData(vector_list);
                vectors_read->SetName(path.stem().string());

                double span = recorder.RecordSection("done");
                double rate = file_size * 1000000.0 / span / 1024 / 1024;
                ENGINE_LOG_DEBUG << "read(" << path.string() << ") rate " << rate << "MB/s";
            } catch (std::exception& e) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + e.what();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }

        if (path.extension().string() == user_id_extension_) {
            try {
                TimeRecorder recorder("read " + path.string());
                std::shared_ptr<storage::IOReader> uid_reader_ptr;
                if (s3_enable) {
                    uid_reader_ptr = std::make_shared<storage::S3IOReader>(path.string());
                } else {
                    uid_reader_ptr = std::make_shared<storage::FileIOReader>(path.string());
                }

                size_t file_size = uid_reader_ptr->length();
                std::vector<segment::doc_id_t> uids;
                uids.resize(file_size / sizeof(segment::doc_id_t));
                uid_reader_ptr->read(uids.data(), file_size);

                vectors_read->AddUids(uids);

                double span = recorder.RecordSection("done");
                double rate = file_size * 1000000.0 / span / 1024 / 1024;
                ENGINE_LOG_DEBUG << "read(" << path.string() << ") rate " << rate << "MB/s";
            } catch (std::exception& e) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + e.what();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
    }
}

void
DefaultVectorsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::VectorsPtr& vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = directory_ptr->GetDirPath();

    const std::string rv_file_path = dir_path + "/" + vectors->GetName() + raw_vector_extension_;
    const std::string uid_file_path = dir_path + "/" + vectors->GetName() + user_id_extension_;

    try {
        TimeRecorder recorder("write " + rv_file_path);
        std::shared_ptr<storage::IOWriter> rv_writer_ptr;
        if (s3_enable) {
            rv_writer_ptr = std::make_shared<storage::S3IOWriter>(rv_file_path);
        } else {
            rv_writer_ptr = std::make_shared<storage::FileIOWriter>(rv_file_path);
        }
        size_t  num_bytes = vectors->GetData().size();
        rv_writer_ptr->write((void*)(vectors->GetData().data()), num_bytes);

        double span = recorder.RecordSection("done");
        double rate = num_bytes * 1000000.0 / span / 1024 / 1024;
        ENGINE_LOG_DEBUG << "write(" << rv_file_path << ") rate " << rate << "MB/s";
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write rv file: " + rv_file_path + ", error: " + e.what();
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    try {
        TimeRecorder recorder("write " + uid_file_path);
        std::shared_ptr<storage::IOWriter> uid_writer_ptr;
        if (s3_enable) {
            uid_writer_ptr = std::make_shared<storage::S3IOWriter>(uid_file_path);
        } else {
            uid_writer_ptr = std::make_shared<storage::FileIOWriter>(uid_file_path);
        }
        size_t num_bytes = sizeof(segment::doc_id_t) * vectors->GetCount();
        uid_writer_ptr->write((void*)(vectors->GetUids().data()), num_bytes);

        double span = recorder.RecordSection("done");
        double rate = num_bytes * 1000000.0 / span / 1024 / 1024;
        ENGINE_LOG_DEBUG << "write(" << uid_file_path << ") rate " << rate << "MB/s";
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write uid file: " + uid_file_path + ", error: " + e.what();
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

void
DefaultVectorsFormat::read_uids(const store::DirectoryPtr& directory_ptr, std::vector<segment::doc_id_t>& uids) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = directory_ptr->GetDirPath();
    if (!s3_enable && !boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    //    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    for (; it != it_end; ++it) {
        const auto& path = it->path();
        if (path.extension().string() == user_id_extension_) {
            try {
                TimeRecorder recorder("read " + path.string());
                std::shared_ptr<storage::IOReader> uid_reader_ptr;
                if (s3_enable) {
                    uid_reader_ptr = std::make_shared<storage::S3IOReader>(path.string());
                } else {
                    uid_reader_ptr = std::make_shared<storage::FileIOReader>(path.string());
                }

                size_t file_size = uid_reader_ptr->length();
                uids.resize(file_size / sizeof(segment::doc_id_t));
                uid_reader_ptr->read(uids.data(), file_size);

                double span = recorder.RecordSection("done");
                double rate = file_size * 1000000.0 / span / 1024 / 1024;
                ENGINE_LOG_DEBUG << "read(" << path.string() << ") rate " << rate << "MB/s";
            } catch (std::exception& e) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + e.what();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
    }
}

void
DefaultVectorsFormat::read_vectors(const store::DirectoryPtr& directory_ptr, off_t offset, size_t num_bytes,
                                   std::vector<uint8_t>& raw_vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

    bool s3_enable = false;
    server::Config& config = server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::string dir_path = directory_ptr->GetDirPath();
    if (!s3_enable && !boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    //    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    for (; it != it_end; ++it) {
        const auto &path = it->path();
        if (path.extension().string() == raw_vector_extension_) {
            try {
                TimeRecorder recorder("read " + path.string());
                std::shared_ptr<storage::IOReader> rv_reader_ptr;
                if (s3_enable) {
                    rv_reader_ptr = std::make_shared<storage::S3IOReader>(path.string());
                } else {
                    rv_reader_ptr = std::make_shared<storage::FileIOReader>(path.string());
                }

                rv_reader_ptr->seekg(offset);
                size_t file_size = rv_reader_ptr->length();
                num_bytes = std::min(num_bytes, file_size);
                raw_vectors.resize(num_bytes);
                rv_reader_ptr->read(raw_vectors.data(), num_bytes);

                double span = recorder.RecordSection("done");
                double rate = file_size * 1000000.0 / span / 1024 / 1024;
                ENGINE_LOG_DEBUG << "read(" << path.string() << ") rate " << rate << "MB/s";
            } catch (std::exception& e) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + e.what();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
    }
}

}  // namespace codec
}  // namespace milvus
