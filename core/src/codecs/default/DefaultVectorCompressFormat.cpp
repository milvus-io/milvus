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

#include <boost/filesystem.hpp>
#include <memory>

#include "codecs/default/DefaultVectorCompressFormat.h"
#include "knowhere/common/BinarySet.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
DefaultVectorCompressFormat::read(const storage::FSHandlerPtr& fs_ptr, const std::string& location,
                                  knowhere::BinaryPtr& compress) {
    const std::string compress_file_path = location + sq8_vector_extension_;

    milvus::TimeRecorder recorder("read_index");

    recorder.RecordSection("Start");
    if (!fs_ptr->reader_ptr_->open(compress_file_path)) {
        LOG_ENGINE_ERROR_ << "Fail to open vector index: " << compress_file_path;
        return;
    }

    int64_t length = fs_ptr->reader_ptr_->length();
    if (length <= 0) {
        LOG_ENGINE_ERROR_ << "Invalid vector index length: " << compress_file_path;
        return;
    }

    compress = std::make_shared<knowhere::Binary>();
    compress->data = std::shared_ptr<uint8_t[]>(new uint8_t[length]);
    compress->size = length;

    fs_ptr->reader_ptr_->seekg(0);
    fs_ptr->reader_ptr_->read(compress->data.get(), length);
    fs_ptr->reader_ptr_->close();

    double span = recorder.RecordSection("End");
    double rate = length * 1000000.0 / span / 1024 / 1024;
    LOG_ENGINE_DEBUG_ << "read_compress(" << compress_file_path << ") rate " << rate << "MB/s";
}

void
DefaultVectorCompressFormat::write(const storage::FSHandlerPtr& fs_ptr, const std::string& location,
                                   const knowhere::BinaryPtr& compress) {
    const std::string compress_file_path = location + sq8_vector_extension_;

    milvus::TimeRecorder recorder("write_index");

    recorder.RecordSection("Start");
    if (!fs_ptr->writer_ptr_->open(compress_file_path)) {
        LOG_ENGINE_ERROR_ << "Fail to open vector compress: " << compress_file_path;
        return;
    }

    fs_ptr->writer_ptr_->write(compress->data.get(), compress->size);
    fs_ptr->writer_ptr_->close();

    double span = recorder.RecordSection("End");
    double rate = compress->size * 1000000.0 / span / 1024 / 1024;
    LOG_ENGINE_DEBUG_ << "write_compress(" << compress_file_path << ") rate " << rate << "MB/s";
}

}  // namespace codec
}  // namespace milvus
