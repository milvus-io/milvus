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

#include "codecs/default/DefaultVectorIndexFormat.h"
#include "knowhere/common/BinarySet.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "segment/VectorIndex.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

knowhere::VecIndexPtr
DefaultVectorIndexFormat::read_internal(const std::string& path) {
    milvus::TimeRecorder recorder("read_index");
    knowhere::BinarySet load_data_list;

    storage::DiskIOReaderPtr reader_ptr = std::make_shared<milvus::storage::DiskIOReader>();

    recorder.RecordSection("Start");
    reader_ptr->open(path);

    size_t length = reader_ptr->length();
    if (length <= 0) {
        ENGINE_LOG_ERROR << "Invalid vector index length: " << path;
        return nullptr;
    }

    size_t rp = 0;
    reader_ptr->seekg(0);

    int32_t current_type = 0;
    reader_ptr->read(&current_type, sizeof(current_type));
    rp += sizeof(current_type);
    reader_ptr->seekg(rp);

    ENGINE_LOG_DEBUG << "Start to read_index(" << path << ") length: " << length << " bytes";
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

        std::shared_ptr<uint8_t[]> binptr(bin);
        load_data_list.Append(std::string(meta, meta_length), binptr, bin_length);
        delete[] meta;
    }
    reader_ptr->close();

    double span = recorder.RecordSection("End");
    double rate = length * 1000000.0 / span / 1024 / 1024;
    ENGINE_LOG_DEBUG << "read_index(" << path << ") rate " << rate << "MB/s";

    knowhere::VecIndexFactory& vec_index_factory = knowhere::VecIndexFactory::GetInstance();
    auto index =
        vec_index_factory.CreateVecIndex(knowhere::OldIndexTypeToStr(current_type), knowhere::IndexMode::MODE_CPU);
    if (index != nullptr) {
        index->Load(load_data_list);
        index->SetIndexSize(length);
    } else {
        ENGINE_LOG_ERROR << "Fail to create vector index: " << path;
    }

    return index;
}

void
DefaultVectorIndexFormat::read(const storage::FSHandlerPtr& fs_ptr, segment::VectorIndexPtr& vector_index) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);

    for (; it != it_end; ++it) {
        const auto& path = it->path();

        // if (path.extension().string() == vector_index_extension_) {
        /* tmp solution, should be replaced when use .idx as index extension name */
        const std::string& location = path.string();
        if (location.substr(location.length() - 3) == "000") {
            knowhere::VecIndexPtr index = read_internal(location);
            vector_index->SetVectorIndex(index);
            vector_index->SetName(path.stem().string());
            return;
        }
    }
}

void
DefaultVectorIndexFormat::write(const storage::FSHandlerPtr& fs_ptr, const std::string& location,
                                const segment::VectorIndexPtr& vector_index) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();

    const std::string index_file_path = location;
    // const std::string index_file_path = dir_path + "/" + vector_index->GetName() + vector_index_extension_;

    milvus::TimeRecorder recorder("write_index");

    knowhere::VecIndexPtr index = vector_index->GetVectorIndex();

    auto binaryset = index->Serialize(knowhere::Config());
    int32_t index_type = knowhere::StrToOldIndexType(index->index_type());

    storage::DiskIOWriterPtr writer_ptr = std::make_shared<milvus::storage::DiskIOWriter>();

    recorder.RecordSection("Start");
    writer_ptr->open(index_file_path);

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
    ENGINE_LOG_DEBUG << "write_index(" << index_file_path << ") rate " << rate << "MB/s";
}

}  // namespace codec
}  // namespace milvus
