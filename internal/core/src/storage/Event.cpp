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
#include "storage/Util.h"
#include "storage/PayloadReader.h"
#include "storage/PayloadWriter.h"
#include "exceptions/EasyAssert.h"
#include "utils/Json.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"

namespace milvus::storage {

int
GetFixPartSize(DescriptorEventData& data) {
    return sizeof(data.fix_part.collection_id) + sizeof(data.fix_part.partition_id) + sizeof(data.fix_part.segment_id) +
           sizeof(data.fix_part.field_id) + sizeof(data.fix_part.start_timestamp) +
           sizeof(data.fix_part.end_timestamp) + sizeof(data.fix_part.data_type);
}
int
GetFixPartSize(BaseEventData& data) {
    return sizeof(data.start_timestamp) + sizeof(data.end_timestamp);
}

int
GetEventHeaderSize(EventHeader& header) {
    return sizeof(header.event_type_) + sizeof(header.timestamp_) + sizeof(header.event_length_) +
           sizeof(header.next_position_);
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

EventHeader::EventHeader(PayloadInputStream* input) {
    auto ast = input->Read(sizeof(timestamp_), &timestamp_);
    assert(ast.ok());
    ast = input->Read(sizeof(event_type_), &event_type_);
    assert(ast.ok());
    ast = input->Read(sizeof(event_length_), &event_length_);
    assert(ast.ok());
    ast = input->Read(sizeof(next_position_), &next_position_);
    assert(ast.ok());
}

std::vector<uint8_t>
EventHeader::Serialize() {
    auto header_size = sizeof(timestamp_) + sizeof(event_type_) + sizeof(event_length_) + sizeof(next_position_);
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

DescriptorEventDataFixPart::DescriptorEventDataFixPart(PayloadInputStream* input) {
    auto ast = input->Read(sizeof(collection_id), &collection_id);
    assert(ast.ok());
    ast = input->Read(sizeof(partition_id), &partition_id);
    assert(ast.ok());
    ast = input->Read(sizeof(segment_id), &segment_id);
    assert(ast.ok());
    ast = input->Read(sizeof(field_id), &field_id);
    assert(ast.ok());
    ast = input->Read(sizeof(start_timestamp), &start_timestamp);
    assert(ast.ok());
    ast = input->Read(sizeof(end_timestamp), &end_timestamp);
    assert(ast.ok());
    ast = input->Read(sizeof(data_type), &data_type);
    assert(ast.ok());
}

std::vector<uint8_t>
DescriptorEventDataFixPart::Serialize() {
    auto fix_part_size = sizeof(collection_id) + sizeof(partition_id) + sizeof(segment_id) + sizeof(field_id) +
                         sizeof(start_timestamp) + sizeof(end_timestamp) + sizeof(data_type);
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

DescriptorEventData::DescriptorEventData(PayloadInputStream* input) {
    fix_part = DescriptorEventDataFixPart(input);
    for (auto i = int8_t(EventType::DescriptorEvent); i < int8_t(EventType::EventTypeEnd); i++) {
        post_header_lengths.push_back(GetEventFixPartSize(EventType(i)));
    }
    auto ast = input->Read(post_header_lengths.size(), post_header_lengths.data());
    assert(ast.ok());
    ast = input->Read(sizeof(extra_length), &extra_length);
    assert(ast.ok());
    extra_bytes = std::vector<uint8_t>(extra_length);
    ast = input->Read(extra_length, extra_bytes.data());
    assert(ast.ok());

    milvus::json json = milvus::json::parse(extra_bytes.begin(), extra_bytes.end());
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
    extra_bytes = std::vector<uint8_t>(extras_string.begin(), extras_string.end());
    auto len = fix_part_data.size() + post_header_lengths.size() + sizeof(extra_length) + extra_length;
    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, fix_part_data.data(), fix_part_data.size());
    offset += fix_part_data.size();
    memcpy(res.data() + offset, post_header_lengths.data(), post_header_lengths.size());
    offset += post_header_lengths.size();
    memcpy(res.data() + offset, &extra_length, sizeof(extra_length));
    offset += sizeof(extra_length);
    memcpy(res.data() + offset, extra_bytes.data(), extra_bytes.size());

    return res;
}

BaseEventData::BaseEventData(PayloadInputStream* input, int event_length, DataType data_type) {
    auto ast = input->Read(sizeof(start_timestamp), &start_timestamp);
    AssertInfo(ast.ok(), "read start timestamp failed");
    ast = input->Read(sizeof(end_timestamp), &end_timestamp);
    AssertInfo(ast.ok(), "read end timestamp failed");

    int payload_length = event_length - sizeof(start_timestamp) - sizeof(end_timestamp);
    auto res = input->Read(payload_length);
    auto payload_reader = std::make_shared<PayloadReader>(res.ValueOrDie()->data(), payload_length, data_type);
    field_data = payload_reader->get_field_data();
}

// TODO :: handle string and bool type
std::vector<uint8_t>
BaseEventData::Serialize() {
    auto payload = field_data->get_payload();
    std::shared_ptr<PayloadWriter> payload_writer;
    if (milvus::datatype_is_vector(payload->data_type)) {
        AssertInfo(payload->dimension.has_value(), "empty dimension");
        payload_writer = std::make_unique<PayloadWriter>(payload->data_type, payload->dimension.value());
    } else {
        payload_writer = std::make_unique<PayloadWriter>(payload->data_type);
    }
    payload_writer->add_payload(*payload.get());
    payload_writer->finish();
    auto payload_buffer = payload_writer->get_payload_buffer();
    auto len = sizeof(start_timestamp) + sizeof(end_timestamp) + payload_buffer.size();
    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, &start_timestamp, sizeof(start_timestamp));
    offset += sizeof(start_timestamp);
    memcpy(res.data() + offset, &end_timestamp, sizeof(end_timestamp));
    offset += sizeof(end_timestamp);
    memcpy(res.data() + offset, payload_buffer.data(), payload_buffer.size());

    return res;
}

BaseEvent::BaseEvent(PayloadInputStream* input, DataType data_type) {
    event_header = EventHeader(input);
    auto event_data_length = event_header.event_length_ - event_header.next_position_;
    event_data = BaseEventData(input, event_data_length, data_type);
}

std::vector<uint8_t>
BaseEvent::Serialize() {
    auto data = event_data.Serialize();
    int data_size = data.size();

    event_header.next_position_ = GetEventHeaderSize(event_header);
    event_header.event_length_ = event_header.next_position_ + data_size;
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

DescriptorEvent::DescriptorEvent(PayloadInputStream* input) {
    event_header = EventHeader(input);
    event_data = DescriptorEventData(input);
}

std::vector<uint8_t>
DescriptorEvent::Serialize() {
    auto data = event_data.Serialize();
    int data_size = data.size();

    event_header.event_type_ = EventType::DescriptorEvent;
    event_header.next_position_ = GetEventHeaderSize(event_header);
    event_header.event_length_ = event_header.next_position_ + data_size;
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

    return res;
}

LocalInsertEvent::LocalInsertEvent(PayloadInputStream* input, DataType data_type) {
    auto ret = input->Read(sizeof(row_num), &row_num);
    AssertInfo(ret.ok(), "read input stream failed");
    ret = input->Read(sizeof(dimension), &dimension);
    AssertInfo(ret.ok(), "read input stream failed");
    int data_size = milvus::datatype_sizeof(data_type) * row_num;
    auto insert_data_bytes = input->Read(data_size);
    auto insert_data = reinterpret_cast<const uint8_t*>(insert_data_bytes.ValueOrDie()->data());
    std::shared_ptr<arrow::ArrayBuilder> builder = nullptr;
    if (milvus::datatype_is_vector(data_type)) {
        builder = CreateArrowBuilder(data_type, dimension);
    } else {
        builder = CreateArrowBuilder(data_type);
    }
    // TODO :: handle string type
    Payload payload{data_type, insert_data, row_num, dimension};
    AddPayloadToArrowBuilder(builder, payload);

    std::shared_ptr<arrow::Array> array;
    auto finish_ret = builder->Finish(&array);
    AssertInfo(finish_ret.ok(), "arrow builder finish failed");
    field_data = std::make_shared<FieldData>(array, data_type);
}

std::vector<uint8_t>
LocalInsertEvent::Serialize() {
    auto payload = field_data->get_payload();
    row_num = payload->rows;
    dimension = 1;
    if (milvus::datatype_is_vector(payload->data_type)) {
        assert(payload->dimension.has_value());
        dimension = payload->dimension.value();
    }
    int payload_size = GetPayloadSize(payload.get());
    int len = sizeof(row_num) + sizeof(dimension) + payload_size;

    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, &row_num, sizeof(row_num));
    offset += sizeof(row_num);
    memcpy(res.data() + offset, &dimension, sizeof(dimension));
    offset += sizeof(dimension);
    memcpy(res.data() + offset, payload->raw_data, payload_size);

    return res;
}

LocalIndexEvent::LocalIndexEvent(PayloadInputStream* input) {
    auto ret = input->Read(sizeof(index_size), &index_size);
    AssertInfo(ret.ok(), "read input stream failed");
    ret = input->Read(sizeof(degree), &degree);
    AssertInfo(ret.ok(), "read input stream failed");
    auto binary_index = input->Read(index_size);

    auto binary_index_data = reinterpret_cast<const int8_t*>(binary_index.ValueOrDie()->data());
    auto builder = std::make_shared<arrow::Int8Builder>();
    auto append_ret = builder->AppendValues(binary_index_data, binary_index_data + index_size);
    AssertInfo(append_ret.ok(), "append data to arrow builder failed");

    std::shared_ptr<arrow::Array> array;
    auto finish_ret = builder->Finish(&array);

    AssertInfo(finish_ret.ok(), "arrow builder finish failed");
    field_data = std::make_shared<FieldData>(array, DataType::INT8);
}

std::vector<uint8_t>
LocalIndexEvent::Serialize() {
    auto payload = field_data->get_payload();
    index_size = payload->rows;
    int len = sizeof(index_size) + sizeof(degree) + index_size;

    std::vector<uint8_t> res(len);
    int offset = 0;
    memcpy(res.data() + offset, &index_size, sizeof(index_size));
    offset += sizeof(index_size);
    memcpy(res.data() + offset, &degree, sizeof(degree));
    offset += sizeof(degree);
    memcpy(res.data() + offset, payload->raw_data, index_size);

    return res;
}

}  // namespace milvus::storage
