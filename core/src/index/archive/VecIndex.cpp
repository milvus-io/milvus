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

#include <memory>

#include "config/Config.h"
#include "index/archive/VecIndex.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexType.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

knowhere::VecIndexPtr
LoadVecIndex(const knowhere::IndexType& type, const knowhere::BinarySet& index_binary, int64_t size) {
    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
    auto index = vec_index_factory.CreateVecIndex(type, knowhere::IndexMode::MODE_CPU);
    if (index == nullptr)
        return nullptr;
    // else
    index->Load(index_binary);
    index->SetIndexSize(size);
    return index;
}

knowhere::VecIndexPtr
read_index(const std::string& location) {
    milvus::TimeRecorder recorder("read_index");
    knowhere::BinarySet load_data_list;

    bool s3_enable = false;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    config.GetStorageConfigS3Enable(s3_enable);

    std::shared_ptr<milvus::storage::IOReader> reader_ptr;
    if (s3_enable) {
        reader_ptr = std::make_shared<milvus::storage::S3IOReader>();
    } else {
        reader_ptr = std::make_shared<milvus::storage::DiskIOReader>();
    }

    recorder.RecordSection("Start");
    reader_ptr->open(location);

    size_t length = reader_ptr->length();
    if (length <= 0) {
        STORAGE_LOG_DEBUG << "read_index(" << location << ") failed!";
        return nullptr;
    }

    size_t rp = 0;
    reader_ptr->seekg(0);

    int32_t current_type = 0;
    reader_ptr->read(&current_type, sizeof(current_type));
    rp += sizeof(current_type);
    reader_ptr->seekg(rp);

    STORAGE_LOG_DEBUG << "Start to read_index(" << location << ") length: " << length << " bytes";
    while (rp < length) {
        size_t meta_length;
        reader_ptr->read(&meta_length, sizeof(meta_length));
        rp += sizeof(meta_length);
        reader_ptr->seekg(rp);

        auto meta = new char[meta_length];
        reader_ptr->read(meta, meta_length);
        rp += meta_length;
        reader_ptr->seekg(rp);

        size_t bin_length;
        reader_ptr->read(&bin_length, sizeof(bin_length));
        rp += sizeof(bin_length);
        reader_ptr->seekg(rp);

        auto bin = new uint8_t[bin_length];
        reader_ptr->read(bin, bin_length);
        rp += bin_length;
        reader_ptr->seekg(rp);

        auto binptr = std::make_shared<uint8_t>();
        binptr.reset(bin);
        load_data_list.Append(std::string(meta, meta_length), binptr, bin_length);
        delete[] meta;
    }
    reader_ptr->close();

    double span = recorder.RecordSection("End");
    double rate = length * 1000000.0 / span / 1024 / 1024;
    STORAGE_LOG_DEBUG << "read_index(" << location << ") rate " << rate << "MB/s";

    return LoadVecIndex(knowhere::OldIndexTypeToStr(current_type), load_data_list, length);
}

milvus::Status
write_index(knowhere::VecIndexPtr index, const std::string& location) {
    try {
        milvus::TimeRecorder recorder("write_index");

        auto binaryset = index->Serialize(knowhere::Config());
        int32_t index_type = knowhere::StrToOldIndexType(index->index_type());

        bool s3_enable = false;
        milvus::server::Config& config = milvus::server::Config::GetInstance();
        config.GetStorageConfigS3Enable(s3_enable);

        std::shared_ptr<milvus::storage::IOWriter> writer_ptr;
        if (s3_enable) {
            writer_ptr = std::make_shared<milvus::storage::S3IOWriter>();
        } else {
            writer_ptr = std::make_shared<milvus::storage::DiskIOWriter>();
        }

        recorder.RecordSection("Start");
        writer_ptr->open(location);

        writer_ptr->write(&index_type, sizeof(index_type));

        for (auto& iter : binaryset.binary_map_) {
            auto meta = iter.first.c_str();
            size_t meta_length = iter.first.length();
            writer_ptr->write(&meta_length, sizeof(meta_length));
            writer_ptr->write((void*)meta, meta_length);

            auto binary = iter.second;
            int64_t binary_length = binary->size;
            writer_ptr->write(&binary_length, sizeof(binary_length));
            writer_ptr->write((void*)binary->data.get(), binary_length);
        }
        writer_ptr->close();

        double span = recorder.RecordSection("End");
        double rate = writer_ptr->length() * 1000000.0 / span / 1024 / 1024;
        STORAGE_LOG_DEBUG << "write_index(" << location << ") rate " << rate << "MB/s";
    } catch (knowhere::KnowhereException& e) {
        WRAPPER_LOG_ERROR << e.what();
        return milvus::Status(milvus::KNOWHERE_UNEXPECTED_ERROR, e.what());
    } catch (std::exception& e) {
        WRAPPER_LOG_ERROR << e.what();
        std::string estring(e.what());
        if (estring.find("No space left on device") != estring.npos) {
            WRAPPER_LOG_ERROR << "No space left on the device";
            return milvus::Status(milvus::KNOWHERE_NO_SPACE, "No space left on the device");
        } else {
            return milvus::Status(milvus::KNOWHERE_ERROR, e.what());
        }
    }
    return milvus::Status::OK();
}

}  // namespace engine
}  // namespace milvus
