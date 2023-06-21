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

#include <string>
#include <memory>
#include <vector>
#include <unordered_map>

#include "common/Types.h"
#include "storage/Types.h"
#include "storage/FieldData.h"
#include "storage/BinlogReader.h"

namespace milvus::storage {

struct EventHeader {
    milvus::Timestamp timestamp_;
    EventType event_type_;
    int32_t event_length_;
    int32_t next_position_;

    EventHeader() = default;
    explicit EventHeader(BinlogReaderPtr reader);

    std::vector<uint8_t>
    Serialize();
};

struct DescriptorEventDataFixPart {
    int64_t collection_id;
    int64_t partition_id;
    int64_t segment_id;
    int64_t field_id;
    Timestamp start_timestamp;
    Timestamp end_timestamp;
    milvus::proto::schema::DataType data_type;

    DescriptorEventDataFixPart() = default;
    explicit DescriptorEventDataFixPart(BinlogReaderPtr reader);

    std::vector<uint8_t>
    Serialize();
};

struct DescriptorEventData {
    DescriptorEventDataFixPart fix_part;
    int32_t extra_length;
    std::vector<uint8_t> extra_bytes;
    std::unordered_map<std::string, std::string> extras;
    std::vector<uint8_t> post_header_lengths;

    DescriptorEventData() = default;
    explicit DescriptorEventData(BinlogReaderPtr reader);

    std::vector<uint8_t>
    Serialize();
};

struct BaseEventData {
    Timestamp start_timestamp;
    Timestamp end_timestamp;
    FieldDataPtr field_data;

    BaseEventData() = default;
    explicit BaseEventData(BinlogReaderPtr reader,
                           int event_length,
                           DataType data_type);

    std::vector<uint8_t>
    Serialize();
};

struct DescriptorEvent {
    EventHeader event_header;
    DescriptorEventData event_data;

    DescriptorEvent() = default;
    explicit DescriptorEvent(BinlogReaderPtr reader);

    std::vector<uint8_t>
    Serialize();
};

struct BaseEvent {
    EventHeader event_header;
    BaseEventData event_data;
    int64_t event_offset;

    BaseEvent() = default;
    explicit BaseEvent(BinlogReaderPtr reader, DataType data_type);

    std::vector<uint8_t>
    Serialize();
};

using InsertEvent = BaseEvent;
using InsertEventData = BaseEventData;
using IndexEvent = BaseEvent;
using IndexEventData = BaseEventData;
using DeleteEvent = BaseEvent;
using DeleteEventData = BaseEventData;
using CreateCollectionEvent = BaseEvent;
using CreateCollectionEventData = BaseEventData;
using CreatePartitionEvent = BaseEvent;
using CreatePartitionEventData = BaseEventData;
using DropCollectionEvent = BaseEvent;
using DropCollectionEventData = BaseEventData;
using DropPartitionEvent = BaseEvent;
using DropPartitionEventData = BaseEventData;

int
GetFixPartSize(DescriptorEventData& data);
int
GetFixPartSize(BaseEventData& data);
int
GetEventHeaderSize(EventHeader& header);
int
GetEventFixPartSize(EventType EventTypeCode);

struct LocalInsertEvent {
    FieldDataPtr field_data;

    std::vector<uint8_t>
    Serialize();
};

struct LocalIndexEvent {
    uint64_t index_size;
    uint32_t degree;
    FieldDataPtr field_data;

    LocalIndexEvent() = default;
    explicit LocalIndexEvent(BinlogReaderPtr reader);

    std::vector<uint8_t>
    Serialize();
};

}  // namespace milvus::storage
