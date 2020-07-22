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

#include "codecs/snapshot/SSStructuredIndexFormat.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <boost/filesystem.hpp>
#include <memory>
#include <utility>

#include "db/meta/MetaTypes.h"
#include "knowhere/index/structured_index/StructuredIndexSort.h"

#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

knowhere::IndexPtr
SSStructuredIndexFormat::create_structured_index(const milvus::engine::meta::hybrid::DataType data_type) {
    knowhere::IndexPtr index = nullptr;
    switch (data_type) {
        case engine::meta::hybrid::DataType::INT8: {
            index = std::make_shared<knowhere::StructuredIndexSort<int8_t>>();
            break;
        }
        case engine::meta::hybrid::DataType::INT16: {
            index = std::make_shared<knowhere::StructuredIndexSort<int16_t>>();
            break;
        }
        case engine::meta::hybrid::DataType::INT32: {
            index = std::make_shared<knowhere::StructuredIndexSort<int32_t>>();
            break;
        }
        case engine::meta::hybrid::DataType::INT64: {
            index = std::make_shared<knowhere::StructuredIndexSort<int64_t>>();
            break;
        }
        case engine::meta::hybrid::DataType::FLOAT: {
            index = std::make_shared<knowhere::StructuredIndexSort<float>>();
            break;
        }
        case engine::meta::hybrid::DataType::DOUBLE: {
            index = std::make_shared<knowhere::StructuredIndexSort<double>>();
            break;
        }
        default: {
            LOG_ENGINE_ERROR_ << "Invalid field type";
            return nullptr;
        }
    }
    return index;
}

void
SSStructuredIndexFormat::read(const milvus::storage::FSHandlerPtr& fs_ptr, const std::string& location,
                              knowhere::IndexPtr& index) {
    milvus::TimeRecorder recorder("read_index");
    knowhere::BinarySet load_data_list;

    recorder.RecordSection("Start");
    if (!fs_ptr->reader_ptr_->open(location)) {
        LOG_ENGINE_ERROR_ << "Fail to open structured index: " << location;
        return;
    }
    int64_t length = fs_ptr->reader_ptr_->length();
    if (length <= 0) {
        LOG_ENGINE_ERROR_ << "Invalid structured index length: " << location;
        return;
    }

    size_t rp = 0;
    fs_ptr->reader_ptr_->seekg(0);

    int32_t data_type = 0;
    fs_ptr->reader_ptr_->read(&data_type, sizeof(data_type));
    rp += sizeof(data_type);
    fs_ptr->reader_ptr_->seekg(rp);

    auto attr_type = (engine::meta::hybrid::DataType)data_type;

    LOG_ENGINE_DEBUG_ << "Start to read_index(" << location << ") length: " << length << " bytes";
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
    LOG_ENGINE_DEBUG_ << "SSStructuredIndexFormat::read(" << location << ") rate " << rate << "MB/s";

    index = create_structured_index((engine::meta::hybrid::DataType)data_type);

    index->Load(load_data_list);

    return;
}

void
SSStructuredIndexFormat::write(const milvus::storage::FSHandlerPtr& fs_ptr, const std::string& location,
                               engine::meta::hybrid::DataType data_type, const knowhere::IndexPtr& index) {
    milvus::TimeRecorder recorder("write_index");
    recorder.RecordSection("Start");

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();

    auto binaryset = index->Serialize(knowhere::Config());

    if (!fs_ptr->writer_ptr_->open(location)) {
        LOG_ENGINE_ERROR_ << "Fail to open structured index: " << location;
        return;
    }
    fs_ptr->writer_ptr_->write(&data_type, sizeof(data_type));

    for (auto& iter : binaryset.binary_map_) {
        auto meta = iter.first.c_str();
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
    LOG_ENGINE_DEBUG_ << "SSStructuredIndexFormat::write(" << dir_path << ") rate " << rate << "MB/s";
}

}  // namespace codec
}  // namespace milvus
