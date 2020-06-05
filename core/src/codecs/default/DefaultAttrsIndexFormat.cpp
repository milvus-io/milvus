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

#include "codecs/default/DefaultAttrsIndexFormat.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <memory>

#include <boost/filesystem.hpp>
#include <src/index/knowhere/knowhere/index/structured_index/StructuredIndexSort.h>

#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

knowhere::IndexPtr
DefaultAttrsIndexFormat::read_internal(const milvus::storage::FSHandlerPtr& fs_ptr,
                                       const std::string& path) {
    milvus::TimeRecorder recorder("read_index");
    knowhere::BinarySet load_data_list;

    recorder.RecordSection("Start");
    if (!fs_ptr->reader_ptr_->open(path)) {
        LOG_ENGINE_ERROR_ << "Fail to open attribute index: " << path;
        return nullptr;
    }
    int64_t length = fs_ptr->reader_ptr_->length();
    if (length <= 0) {
        LOG_ENGINE_ERROR_ << "Invalid attr index length: " << path;
        return nullptr;
    }

    size_t rp = 0;
    fs_ptr->reader_ptr_->seekg(0);

    LOG_ENGINE_DEBUG_ << "Start to read_index(" << path << ") length: " << length << " bytes";
    while (rp < length) {
        size_t meta_length;
        fs_ptr->reader_ptr_->read(&meta_length, sizeof(meta_length));
        rp += sizeof(meta_length);
        fs_ptr->reader_ptr_->seekg(rp);

        auto meta = new char[meta_length];
        fs_ptr->reader_ptr_->read(meta, meta_length);
        rp += meta_length;
        fs_ptr->reader_ptr_->seekg(rp);

        size_t bin_length;
        fs_ptr->reader_ptr_->read(&bin_length, sizeof(bin_length));
        rp += sizeof(bin_length);
        fs_ptr->reader_ptr_->seekg(rp);

        auto bin = new uint8_t[bin_length];
        fs_ptr->reader_ptr_->read(bin, bin_length);
        rp += bin_length;
        fs_ptr->reader_ptr_->seekg(rp);

        std::shared_ptr<uint8_t[]> binptr(bin);
        load_data_list.Append(std::string(meta, meta_length), binptr, bin_length);
        delete[] meta;
    }
    fs_ptr->reader_ptr_->close();

    double span = recorder.RecordSection("End");
    double rate = length * 1000000.0 / span / 1024 / 1024;
    LOG_ENGINE_DEBUG_ << "read_index(" << path << ") rate " << rate << "MB/s";

    knowhere::IndexPtr index = std::make_shared<knowhere::StructuredIndexSort<int64_t>>();
    index->Load(load_data_list);

    return index;
}

void
DefaultAttrsIndexFormat::read(const milvus::storage::FSHandlerPtr& fs_ptr,
                              const std::string& location,
                              milvus::segment::AttrIndexPtr& attr_index) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    knowhere::IndexPtr index = read_internal(fs_ptr, location);
    attr_index->SetAttrIndex(index);
}


void
DefaultAttrsIndexFormat::write(const milvus::storage::FSHandlerPtr& fs_ptr,
                               const std::string& location,
                               const milvus::segment::AttrIndexPtr& attr_index) {
    const std::lock_guard<std::mutex> lock(mutex_);

    milvus::TimeRecorder recorder("write_index");

    knowhere::IndexPtr index = attr_index->GetAttrIndex();

    auto binaryset = index->Serialize(knowhere::Config());

    recorder.RecordSection("Start");
    if (!fs_ptr->writer_ptr_->open(location)) {
        LOG_ENGINE_ERROR_ << "Fail to open attribute index: " << location;
        return;
    }

//    fs_ptr->writer_ptr_->write()

    for (auto& iter : binaryset.binary_map_) {
        auto meta = iter.first.length();
        size_t meta_length = iter.first.length();
        fs_ptr->writer_ptr_->write(&meta_length, sizeof(meta_length));
        fs_ptr->writer_ptr_->write((void*)meta, meta_length);

        auto binary = iter.second;
        int64_t binary_length = binary->size;
        fs_ptr->writer_ptr_->write(&binary_length, sizeof(binary_length));
        fs_ptr->writer_ptr_->write((void*)binary->data.get(), binary_length);
    }
    fs_ptr->writer_ptr_->close();

    double span = recorder.RecordSection("End");
    double rate = fs_ptr->writer_ptr_->length() * 1000000.0 / span / 1024 / 1024;
    LOG_ENGINE_DEBUG_ << "write_index(" << location << ") rate " << rate << "MB/s";
}

}  // namespace codec
}  // namespace milvus
