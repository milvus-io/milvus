// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <arrow/record_batch.h>
#include <cstdint>
#include <utility>
#include <vector>
#include <memory>
#include <utility>

#include "common/FieldData.h"
#include "storage/PayloadReader.h"
#include "storage/Types.h"
#include "storage/PayloadStream.h"
#include "storage/BinlogReader.h"

namespace milvus::storage {

class DataCodec {
 public:
    explicit DataCodec(FieldDataPtr data, CodecType type)
        : codec_type_(type), field_data_(std::move(data)) {
    }

    explicit DataCodec(std::shared_ptr<PayloadReader> reader, CodecType type)
        : codec_type_(type), payload_reader_(std::move(reader)) {
    }

    virtual ~DataCodec() = default;

    // Serialized data can be written directly to remote or local disk
    virtual std::vector<uint8_t>
    Serialize(StorageType medium) = 0;

    virtual void
    SetFieldDataMeta(const FieldDataMeta& meta) = 0;

    void
    SetTimestamps(Timestamp start_timestamp, Timestamp end_timestamp) {
        // if milvus version <= 2.2.5
        // assert(start_timestamp <= end_timestamp) condition may not be satisfied
        time_range_ = std::make_pair(start_timestamp, end_timestamp);
    }

    std::pair<Timestamp, Timestamp>
    GetTimeRage() const {
        return time_range_;
    }

    CodecType
    GetCodecType() const {
        return codec_type_;
    }

    DataType
    GetDataType() {
        return field_data_->get_data_type();
    }

    FieldDataPtr
    GetFieldData() const {
        return field_data_;
    }

    virtual std::shared_ptr<ArrowDataWrapper>
    GetReader() {
        auto ret = std::make_shared<ArrowDataWrapper>();
        ret->reader = payload_reader_->get_reader();
        ret->arrow_reader = payload_reader_->get_file_reader();
        ret->file_data = data_;
        return ret;
    }

    void
    SetData(std::shared_ptr<uint8_t[]> data) {
        data_ = std::move(data);
    }

 protected:
    CodecType codec_type_;
    std::pair<Timestamp, Timestamp> time_range_;
    FieldDataPtr field_data_;
    std::shared_ptr<PayloadReader> payload_reader_;
    std::shared_ptr<uint8_t[]> data_;
};

// Deserialize the data stream of the file obtained from remote or local
std::unique_ptr<DataCodec>
DeserializeFileData(const std::shared_ptr<uint8_t[]> input,
                    int64_t length,
                    bool is_field_data = true);

std::unique_ptr<DataCodec>
DeserializeRemoteFileData(BinlogReaderPtr reader, bool is_field_data);

std::unique_ptr<DataCodec>
DeserializeLocalFileData(BinlogReaderPtr reader);

}  // namespace milvus::storage
