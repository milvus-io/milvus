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

#pragma once

#include <memory>
#include <string>

#include "storage/IOReader.h"
#include "storage/IOWriter.h"
#include "storage/Operation.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#ifdef MILVUS_WITH_AWS
#include "storage/s3/S3ClientWrapper.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "storage/s3/S3Operation.h"
#endif
#ifdef MILVUS_WITH_OSS
#include "storage/oss/OSSClientWrapper.h"
#include "storage/oss/OSSIOReader.h"
#include "storage/oss/OSSIOWriter.h"
#include "storage/oss/OSSOperation.h"
#endif
#include "config/Config.h"

namespace milvus {
namespace storage {

struct FSHandler {
    IOReaderPtr reader_ptr_ = nullptr;
    IOWriterPtr writer_ptr_ = nullptr;
    OperationPtr operation_ptr_ = nullptr;

    FSHandler(IOReaderPtr& reader_ptr, IOWriterPtr& writer_ptr, OperationPtr& operation_ptr)
        : reader_ptr_(reader_ptr), writer_ptr_(writer_ptr), operation_ptr_(operation_ptr) {
    }
};

using FSHandlerPtr = std::shared_ptr<FSHandler>;

inline FSHandlerPtr
createFsHandler(const std::string& directory) {
    storage::IOReaderPtr reader_ptr{nullptr};
    storage::IOWriterPtr writer_ptr{nullptr};
    storage::OperationPtr operation_ptr{nullptr};

#if defined(MILVUS_WITH_AWS) || defined(MILVUS_WITH_OSS)
    server::Config& config = server::Config::GetInstance();
#endif

#ifdef MILVUS_WITH_AWS
    bool enableS3 = false;
    config.GetStorageConfigS3Enable(enableS3);
    if (enableS3) {
        S3ClientWrapper::GetInstance();
        reader_ptr = std::make_shared<storage::S3IOReader>();
        writer_ptr = std::make_shared<storage::S3IOWriter>();
        operation_ptr = std::make_shared<storage::S3Operation>(directory);
        return std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    }
#endif

#ifdef MILVUS_WITH_OSS
    bool enableOSS = false;
    config.GetStorageConfigOSSEnable(enableOSS);
    if (enableOSS) {
        OSSClientWrapper::GetInstance();
        reader_ptr = std::make_shared<storage::OSSIOReader>();
        writer_ptr = std::make_shared<storage::OSSIOWriter>();
        operation_ptr = std::make_shared<storage::OSSOperation>(directory);
        return std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    }
#endif

    reader_ptr = std::make_shared<storage::DiskIOReader>();
    writer_ptr = std::make_shared<storage::DiskIOWriter>();
    operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    return std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
}

}  // namespace storage
}  // namespace milvus
