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

#include "storage/DataCodec.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "storage/IndexData.h"
#include "exceptions/EasyAssert.h"
#include "common/Consts.h"

namespace milvus::storage {

// deserialize remote insert and index file
std::unique_ptr<DataCodec>
DeserializeRemoteFileData(PayloadInputStream* input_stream) {
    DescriptorEvent descriptor_event(input_stream);
    DataType data_type = DataType(descriptor_event.event_data.fix_part.data_type);
    auto descriptor_fix_part = descriptor_event.event_data.fix_part;
    FieldDataMeta data_meta{descriptor_fix_part.collection_id, descriptor_fix_part.partition_id,
                            descriptor_fix_part.segment_id, descriptor_fix_part.field_id};
    EventHeader header(input_stream);
    switch (header.event_type_) {
        case EventType::InsertEvent: {
            auto event_data_length = header.event_length_ - header.next_position_;
            auto insert_event_data = InsertEventData(input_stream, event_data_length, data_type);
            auto insert_data = std::make_unique<InsertData>(insert_event_data.field_data);
            insert_data->SetFieldDataMeta(data_meta);
            insert_data->SetTimestamps(insert_event_data.start_timestamp, insert_event_data.end_timestamp);
            return insert_data;
        }
        case EventType::IndexFileEvent: {
            auto event_data_length = header.event_length_ - header.next_position_;
            auto index_event_data = IndexEventData(input_stream, event_data_length, data_type);
            auto index_data = std::make_unique<IndexData>(index_event_data.field_data);
            index_data->SetFieldDataMeta(data_meta);
            IndexMeta index_meta;
            index_meta.segment_id = data_meta.segment_id;
            index_meta.field_id = data_meta.field_id;
            auto& extras = descriptor_event.event_data.extras;
            AssertInfo(extras.find(INDEX_BUILD_ID_KEY) != extras.end(), "index build id not exist");
            index_meta.build_id = std::stol(extras[INDEX_BUILD_ID_KEY]);
            index_data->set_index_meta(index_meta);
            index_data->SetTimestamps(index_event_data.start_timestamp, index_event_data.end_timestamp);
            return index_data;
        }
        default:
            PanicInfo("unsupported event type");
    }
}

// For now, no file header in file data
std::unique_ptr<DataCodec>
DeserializeLocalFileData(PayloadInputStream* input_stream) {
    PanicInfo("not supported");
}

std::unique_ptr<DataCodec>
DeserializeFileData(const uint8_t* input_data, int64_t length) {
    auto input_stream = std::make_shared<PayloadInputStream>(input_data, length);
    auto medium_type = ReadMediumType(input_stream.get());
    switch (medium_type) {
        case StorageType::Remote: {
            return DeserializeRemoteFileData(input_stream.get());
        }
        case StorageType::LocalDisk: {
            auto ret = input_stream->Seek(0);
            AssertInfo(ret.ok(), "seek input stream failed");
            return DeserializeLocalFileData(input_stream.get());
        }
        default:
            PanicInfo("unsupported medium type");
    }
}

// local insert file format
// -------------------------------------
// | Rows(int) | Dim(int) | InsertData |
// -------------------------------------
std::unique_ptr<DataCodec>
DeserializeLocalInsertFileData(const uint8_t* input_data, int64_t length, DataType data_type) {
    auto input_stream = std::make_shared<PayloadInputStream>(input_data, length);
    LocalInsertEvent event(input_stream.get(), data_type);
    return std::make_unique<InsertData>(event.field_data);
}

// local index file format: which indexSize = sizeOf(IndexData)
// --------------------------------------------------
// | IndexSize(uint64) | degree(uint32) | IndexData |
// --------------------------------------------------
std::unique_ptr<DataCodec>
DeserializeLocalIndexFileData(const uint8_t* input_data, int64_t length) {
    auto input_stream = std::make_shared<PayloadInputStream>(input_data, length);
    LocalIndexEvent event(input_stream.get());
    return std::make_unique<IndexData>(event.field_data);
}

}  // namespace milvus::storage
