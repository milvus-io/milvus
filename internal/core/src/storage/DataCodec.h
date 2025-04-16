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
    explicit DataCodec(std::shared_ptr<PayloadReader>& reader, CodecType type)
        : codec_type_(type), payload_reader_(std::move(reader)) {
    }

    explicit DataCodec(const uint8_t* payload_data,
                       int64_t length,
                       CodecType type)
        : codec_type_(type) {
        payload_reader_ = std::make_shared<PayloadReader>(
            payload_data, length, DataType::NONE, false, false);
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
        AssertInfo(payload_reader_ != nullptr,
                   "payload_reader in the data_codec is invalid, wrong state");
        return payload_reader_->get_payload_datatype();
    }

    FieldDataPtr
    GetFieldData() const {
        AssertInfo(payload_reader_ != nullptr,
                   "payload_reader in the data_codec is invalid, wrong state");
        AssertInfo(payload_reader_->has_field_data(),
                   "payload_reader in the data_codec has no field data, "
                   "wrongly calling the method");
        return payload_reader_->get_field_data();
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

    bool
    HasBinaryPayload() const {
        AssertInfo(payload_reader_ != nullptr,
                   "payload_reader in the data_codec is invalid, wrong state");
        return payload_reader_->has_binary_payload();
    }

    const uint8_t*
    PayloadData() const {
        if (HasBinaryPayload()) {
            return payload_reader_->get_payload_data();
        }
        if (payload_reader_->has_field_data()) {
            return reinterpret_cast<const uint8_t*>(
                const_cast<void*>(payload_reader_->get_field_data()->Data()));
        }
        return nullptr;
    }

    int64_t
    PayloadSize() const {
        if (HasBinaryPayload()) {
            return payload_reader_->get_payload_size();
        }
        if (payload_reader_->has_field_data()) {
            return payload_reader_->get_field_data_size();
        }
        return 0;
    }

 protected:
    CodecType codec_type_;
    std::pair<Timestamp, Timestamp> time_range_;

    std::shared_ptr<PayloadReader> payload_reader_;
    std::shared_ptr<uint8_t[]> data_;
    //the shared ptr to keep the original input data alive for zero-copy target
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
