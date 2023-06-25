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

#include "storage/Event.h"
#include "storage/PayloadReader.h"
#include "storage/PayloadWriter.h"
#include "exceptions/EasyAssert.h"
#include "utils/Json.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"

namespace milvus::storage {

int
GetFixPartSize(DescriptorEventData& data) {
    return sizeof(data.fix_part.collection_id) +
           sizeof(data.fix_part.partition_id) +
           sizeof(data.fix_part.segment_id) + sizeof(data.fix_part.field_id) +
           sizeof(data.fix_part.start_timestamp) +
           sizeof(data.fix_part.end_timestamp) +
           sizeof(data.fix_part.data_type);
}
int
GetFixPartSize(BaseEventData& data) {
    return sizeof(data.start_timestamp) + sizeof(data.end_timestamp);
}

int
GetEventHeaderSize(EventHeader& header) {
    return sizeof(header.event_type_) + sizeof(header.timestamp_) +
           sizeof(header.event_length_) + sizeof(header.next_position_);
}

int
GetEventFixPartSize(EventType EventTypeCode) {
    switch (EventTypeCode) {
        case EventType::DescriptorEvent: {
            DescriptorEventData data;
            return GetFixPartSize(data);
        }
        case EventType::InsertEvent:
        case EventType::DeleteEvent:
        case EventType::CreateCollectionEvent:
        case EventType::DropCollectionEvent:
        case EventType::CreatePartitionEvent:
        case EventType::DropPartitionEvent:
        case EventType::IndexFileEvent: {
            BaseEventData data;
            return GetFixPartSize(data);
        }
        default:
            PanicInfo("unsupported event type");
    }
}

EventHeader::EventHeader(BinlogReaderPtr reader) {
    auto ast = reader->Read(sizeof(timestamp_), &timestamp_);
    assert(ast.ok());
    ast = reader->Read(sizeof(event_type_), &event_type_);
    assert(ast.ok());
    ast = reader->Read(sizeof(event_length_), &event_length_);
    assert(ast.ok());
    ast = reader->Read(sizeof(next_position_), &next_position_);
    assert(ast.ok());
}

std::vector<uint8_t>
EventHeader::Serialize() {
    auto header_size = sizeof(timestamp_) + sizeof(event_type_) +
                       sizeof(event_length_) + sizeof(next_position_);
    std::vector<uint8_t> res(header_size);
    int offset = 0;
    memcpy(res.data() + offset, &timestamp_, sizeof(timestamp_));
    offset += sizeof(timestamp_);
    memcpy(res.data() + offset, &event_type_, sizeof(event_type_));
    offset += sizeof(event_type_);
    memcpy(res.data() + offset, &event_length_, sizeof(event_length_));
    offset += sizeof(event_length_);
    memcpy(res.data() + offset, &next_position_, sizeof(next_position_));

    return res;
}

DescriptorEventDataFixPart::DescriptorEventDataFixPart(BinlogReaderPtr reader) {
    auto ast = reader->Read(sizeof(collection_id), &collection_id);
    assert(ast.ok());
    ast = reader->Read(sizeof(partition_id), &partition_id);
    assert(ast.ok());
    ast = reader->Read(sizeof(segment_id), &segment_id);
    assert(ast.ok());
    ast = reader->Read(sizeof(field_id), &field_id);
    assert(ast.ok());
    ast = reader->Read(sizeof(start_timestamp), &start_timestamp);
    assert(ast.ok());
    ast = reader->Read(sizeof(end_timestamp), &end_timestamp);
    assert(ast.ok());
    ast = reader->Read(sizeof(data_type), &data_type);
    assert(ast.ok());
}

std::vector<uint8_t>
DescriptorEventDataFixPart::Serialize() {
    auto fix_part_size = sizeof(collection_id) + sizeof(partition_id) +
                         sizeof(segment_id) + sizeof(field_id) +
                         sizeof(start_timestamp) + sizeof(end_timestamp) +
                         sizeof(data_type);
    std::vector<uint8_t> res(fix_part_size);
    int offset = 0;
    memcpy(res.data() + offset, &collection_id, sizeof(collection_id));
    offset += sizeof(collection_id);
    memcpy(res.data() + offset, &partition_id, sizeof(partition_id));
    offset += sizeof(partition_id);
    memcpy(res.data() + offset, &segment_id, sizeof(segment_id));
    offset += sizeof(segment_id);
    memcpy(res.data() + offset, &field_id, sizeof(field_id));
    offset += sizeof(field_id);
    memcpy(res.data() + offset, &start_timestamp, sizeof(start_timestamp));
    offset += sizeof(start_timestamp);
    memcpy(res.data() + offset, &end_timestamp, sizeof(end_timestamp));
    offset += sizeof(end_timestamp);
    memcpy(res.data() + offset, &data_type, sizeof(data_type));

    return res;
}

DescriptorEventData::DescriptorEventData(BinlogReaderPtr reader) {
    fix_part = DescriptorEventDataFixPart(reader);
    for (auto i = int8_t(EventType::DescriptorEvent);
         i < int8_t(EventType::EventTypeEnd);
         i++) {
        post_header_lengths.push_back(GetEventFixPartSize(EventType(i)));
    }
    auto ast =
        reader->Read(post_header_lengths.size(), post_header_lengths.data());
    assert(ast.ok());
    ast = reader->Read(sizeof(extra_length), &extra_length);
    assert(ast.ok());
    extra_bytes = std::vector<uint8_t>(extra_length);
    ast = reader->Read(extra_length, extra_bytes.data());
    assert(ast.ok());

    milvus::json json =
        milvus::json::parse(extra_bytes.begin(), extra_bytes.end());
    if (json.contains(ORIGIN_SIZE_KEY)) {
        extras[ORIGIN_SIZE_KEY] = json[ORIGIN_SIZE_KEY];
    }
    if (json.contains(INDEX_BUILD_ID_KEY)) {
        extras[INDEX_BUILD_ID_KEY] = json[INDEX_BUILD_ID_KEY];
    }
}

std::vector<uint8_t>
DescriptorEventData::Serialize() {
    auto fix_part_data = fix_part.Serialize();
    milvus::json extras_json;
    for (auto v : extras) {
        extras_json.emplace(v.first, v.second);
    }
    std::string extras_string = extras_json.dump();
    extra_length = extras_string.size();
    extra_bytes =
        std::vector<uint8_t>(extras_string.begin(), extras_string.end());
    auto len = fix_part_data.size() + post_header_lengths.size() +
               sizeof(extra_length) + extra_length;
    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, fix_part_data.data(), fix_part_data.size());
    offset += fix_part_data.size();
    memcpy(res.data() + offset,
           post_header_lengths.data(),
           post_header_lengths.size());
    offset += post_header_lengths.size();
    memcpy(res.data() + offset, &extra_length, sizeof(extra_length));
    offset += sizeof(extra_length);
    memcpy(res.data() + offset, extra_bytes.data(), extra_bytes.size());

    return res;
}

BaseEventData::BaseEventData(BinlogReaderPtr reader,
                             int event_length,
                             DataType data_type) {
    auto ast = reader->Read(sizeof(start_timestamp), &start_timestamp);
    AssertInfo(ast.ok(), "read start timestamp failed");
    ast = reader->Read(sizeof(end_timestamp), &end_timestamp);
    AssertInfo(ast.ok(), "read end timestamp failed");

    int payload_length =
        event_length - sizeof(start_timestamp) - sizeof(end_timestamp);
    auto res = reader->Read(payload_length);
    AssertInfo(res.first.ok(), "read payload failed");
    auto payload_reader = std::make_shared<PayloadReader>(
        res.second.get(), payload_length, data_type);
    field_data = payload_reader->get_field_data();
}

std::vector<uint8_t>
BaseEventData::Serialize() {
    auto data_type = field_data->get_data_type();
    std::shared_ptr<PayloadWriter> payload_writer;
    if (milvus::datatype_is_vector(data_type)) {
        payload_writer =
            std::make_unique<PayloadWriter>(data_type, field_data->get_dim());
    } else {
        payload_writer = std::make_unique<PayloadWriter>(data_type);
    }
    switch (data_type) {
        case DataType::VARCHAR:
        case DataType::STRING: {
            for (size_t offset = 0; offset < field_data->get_num_rows();
                 ++offset) {
                auto str = static_cast<const std::string*>(
                    field_data->RawValue(offset));
                payload_writer->add_one_string_payload(str->c_str(),
                                                       str->size());
            }
            break;
        }
        case DataType::ARRAY:
        case DataType::JSON: {
            for (size_t offset = 0; offset < field_data->get_num_rows();
                 ++offset) {
                auto string_view =
                    static_cast<const Json*>(field_data->RawValue(offset))
                        ->data();
                payload_writer->add_one_binary_payload(
                    reinterpret_cast<const uint8_t*>(
                        std::string(string_view).c_str()),
                    string_view.size());
            }
            break;
        }
        default: {
            auto payload =
                Payload{data_type,
                        static_cast<const uint8_t*>(field_data->Data()),
                        field_data->get_num_rows(),
                        field_data->get_dim()};
            payload_writer->add_payload(payload);
        }
    }

    payload_writer->finish();
    auto payload_buffer = payload_writer->get_payload_buffer();
    auto len =
        sizeof(start_timestamp) + sizeof(end_timestamp) + payload_buffer.size();
    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, &start_timestamp, sizeof(start_timestamp));
    offset += sizeof(start_timestamp);
    memcpy(res.data() + offset, &end_timestamp, sizeof(end_timestamp));
    offset += sizeof(end_timestamp);
    memcpy(res.data() + offset, payload_buffer.data(), payload_buffer.size());

    return res;
}

BaseEvent::BaseEvent(BinlogReaderPtr reader, DataType data_type) {
    event_header = EventHeader(reader);
    auto event_data_length =
        event_header.event_length_ - GetEventHeaderSize(event_header);
    event_data = BaseEventData(reader, event_data_length, data_type);
}

std::vector<uint8_t>
BaseEvent::Serialize() {
    auto data = event_data.Serialize();
    int data_size = data.size();

    event_header.event_length_ = GetEventHeaderSize(event_header) + data_size;
    event_header.next_position_ = event_header.event_length_ + event_offset;
    auto header = event_header.Serialize();
    int header_size = header.size();

    int len = header_size + data_size;
    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, header.data(), header_size);
    offset += header_size;
    memcpy(res.data() + offset, data.data(), data_size);

    return res;
}

DescriptorEvent::DescriptorEvent(BinlogReaderPtr reader) {
    event_header = EventHeader(reader);
    event_data = DescriptorEventData(reader);
}

std::vector<uint8_t>
DescriptorEvent::Serialize() {
    event_header.event_type_ = EventType::DescriptorEvent;
    auto data = event_data.Serialize();
    int data_size = data.size();

    event_header.event_length_ = GetEventHeaderSize(event_header) + data_size;
    auto header = event_header.Serialize();
    int header_size = header.size();

    int len = header_size + data_size + sizeof(MAGIC_NUM);
    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data(), &MAGIC_NUM, sizeof(MAGIC_NUM));
    offset += sizeof(MAGIC_NUM);
    memcpy(res.data() + offset, header.data(), header_size);
    offset += header_size;
    memcpy(res.data() + offset, data.data(), data_size);
    offset += data_size;
    event_header.next_position_ = offset;

    return res;
}

std::vector<uint8_t>
LocalInsertEvent::Serialize() {
    int row_num = field_data->get_num_rows();
    int dimension = field_data->get_dim();
    int payload_size = field_data->Size();
    int len = sizeof(row_num) + sizeof(dimension) + payload_size;

    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, &row_num, sizeof(row_num));
    offset += sizeof(row_num);
    memcpy(res.data() + offset, &dimension, sizeof(dimension));
    offset += sizeof(dimension);
    memcpy(res.data() + offset, field_data->Data(), payload_size);

    return res;
}

LocalIndexEvent::LocalIndexEvent(BinlogReaderPtr reader) {
    auto ret = reader->Read(sizeof(index_size), &index_size);
    AssertInfo(ret.ok(), "read binlog failed");
    ret = reader->Read(sizeof(degree), &degree);
    AssertInfo(ret.ok(), "read binlog failed");

    auto res = reader->Read(index_size);
    AssertInfo(res.first.ok(), "read payload failed");
    auto payload_reader = std::make_shared<PayloadReader>(
        res.second.get(), index_size, DataType::INT8);
    field_data = payload_reader->get_field_data();
}

std::vector<uint8_t>
LocalIndexEvent::Serialize() {
    index_size = field_data->Size();
    int len = sizeof(index_size) + sizeof(degree) + index_size;

    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, &index_size, sizeof(index_size));
    offset += sizeof(index_size);
    memcpy(res.data() + offset, &degree, sizeof(degree));
    offset += sizeof(degree);
    memcpy(res.data() + offset, field_data->Data(), index_size);

    return res;
}

}  // namespace milvus::storage
