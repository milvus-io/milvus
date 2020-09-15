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
#include <unordered_map>

#include "codecs/ExtraFileInfo.h"
#include "codecs/VectorCompressFormat.h"
#include "db/Utils.h"
#include "knowhere/common/BinarySet.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

const char* VECTOR_COMPRESS_POSTFIX = ".cmp";

std::string
VectorCompressFormat::FilePostfix() {
    std::string str = VECTOR_COMPRESS_POSTFIX;
    return str;
}

Status
VectorCompressFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                           knowhere::BinaryPtr& compress) {
    milvus::TimeRecorder recorder("VectorCompressFormat::Read");

    const std::string full_file_path = file_path + VECTOR_COMPRESS_POSTFIX;
    if (!fs_ptr->reader_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open vector compress file: " + full_file_path);
    }
    CHECK_MAGIC_VALID(fs_ptr);
    CHECK_SUM_VALID(fs_ptr);

    int64_t length = fs_ptr->reader_ptr_->Length() - MAGIC_SIZE - HEADER_SIZE - SUM_SIZE;
    if (length <= 0) {
        return Status(SERVER_UNEXPECTED_ERROR, "Invalid vector compress length: " + full_file_path);
    }

    compress = std::make_shared<knowhere::Binary>();
    compress->data = std::shared_ptr<uint8_t[]>(new uint8_t[length]);
    compress->size = length;

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    fs_ptr->reader_ptr_->Read(compress->data.get(), length);
    fs_ptr->reader_ptr_->Close();

    double span = recorder.RecordSection("End");
    double rate = length * 1000000.0 / span / 1024 / 1024;
    LOG_ENGINE_DEBUG_ << "VectorCompressFormat::Read(" << full_file_path << ") rate " << rate << "MB/s";

    return Status::OK();
}

Status
VectorCompressFormat::Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                            const knowhere::BinaryPtr& compress) {
    milvus::TimeRecorder recorder("VectorCompressFormat::Write");

    const std::string full_file_path = file_path + VECTOR_COMPRESS_POSTFIX;

    if (!fs_ptr->writer_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open vector compress: " + full_file_path);
    }

    try {
        // TODO: add extra info
        WRITE_MAGIC(fs_ptr);
        HeaderMap maps;
        std::string header = HeaderWrapper(maps);
        WRITE_HEADER(fs_ptr, header);

        fs_ptr->writer_ptr_->Write(compress->data.get(), compress->size);

        WRITE_SUM(fs_ptr, header, reinterpret_cast<char*>(compress->data.get()), compress->size);

        fs_ptr->writer_ptr_->Close();

        double span = recorder.RecordSection("End");
        double rate = compress->size * 1000000.0 / span / 1024 / 1024;
        LOG_ENGINE_DEBUG_ << "SVectorCompressFormat::Write(" << full_file_path << ") rate " << rate << "MB/s";
    } catch (std::exception& ex) {
        std::string err_msg = "Failed to write compress data: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return Status::OK();
}

}  // namespace codec
}  // namespace milvus
